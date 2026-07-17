//! `web-sys` WebSocket shim binding the browser socket to the shared driver
//! (R3.2, F4/F5).
//!
//! This module is mechanical, exactly like the blocking std adapter: it
//! constructs the socket the driver commanded, executes emitted
//! [`SocketCommand`]s, and reports observed browser socket facts as closed
//! [`SocketEvent`]s through the pure [`mirror`](super::mirror) layer. It makes
//! no protocol, framing, correlation, reconnect, or fate decision (F5).
//!
//! F4: `binaryType = "arraybuffer"` is pinned at construction, before the
//! `open`, `message`, `close`, and `error` subscriptions are attached, so no
//! message can ever arrive under the asynchronous `Blob` default.
//!
//! F1: the landed acceptor declines every WebSocket extension offer, so the
//! socket comes up extension-free by the SERVER's posture — the browser
//! offers `permessage-deflate` on its own and cannot be told not to. The
//! adapter still pins the outcome where it becomes observable: at `open`, the
//! socket's negotiated `extensions` string is passed through the mirror, and
//! a non-empty value is a typed transport failure, never a renegotiation.
//!
//! Backpressure: outbound flow control is structural — the client unit's
//! at-most-one-outstanding-correlated-exchange rule (Q-B) bounds
//! request-class traffic before any byte reaches the socket. The adapter
//! never reads `bufferedAmount`; that attribute is a polling surface and
//! polling is banned (LAW-1).
//!
//! Single-threaded seam (tear Q4): the JavaScript socket stays outside the
//! transport-neutral handle. Closed events and commands cross the seam as
//! plain values; driver state sits behind one `RefCell` that never holds a
//! borrow across foreign code, and owner-visible signals are queued and
//! drained re-entrantly-safely — no timer, interval, or executor is involved.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::format;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

use super::super::core::{
    CommandRefusal, DriverOutput, DriverPhase, ResponseExpectation, SocketCommand, SocketEvent,
    SocketFailure, WebSocketFrameDriver,
};
use super::mirror::{BrowserMessageData, BrowserSocketAction, action_for_command};

/// Owner-installed sink receiving every adapter signal, in order.
///
/// The wasm leg is single-threaded and event-driven: the sink runs on the
/// browser event loop, directly from the socket callback that produced the
/// signal. It deliberately does not require `Send + Sync` (F5, following the
/// beamr-wasm precedent).
pub type AdapterSignalSink = Box<dyn FnMut(AdapterSignal)>;

/// One signal delivered to the owner's sink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdapterSignal {
    /// A decision output of the shared driver.
    Output(DriverOutput),
    /// A typed adapter fault outside the driver's closed output set.
    Fault(AdapterFault),
}

/// Typed faults the shim itself can observe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdapterFault {
    /// The single-threaded driver seam was re-entered while borrowed. This is
    /// structurally unreachable under browser event-loop semantics and is
    /// reported instead of being silently dropped.
    SeamReentered,
    /// Executing a driver-emitted command on the browser socket failed.
    CommandExecutionFailed {
        /// Human-readable detail from the JavaScript boundary.
        description: String,
    },
}

/// Typed errors of the adapter's caller-facing surface.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BrowserSocketError {
    /// The shared driver refused the command in its current state.
    #[error("driver refused the command: {refusal:?}")]
    CommandRefused {
        /// The driver's typed refusal.
        refusal: CommandRefusal,
    },
    /// The JavaScript socket boundary reported a failure.
    #[error("browser socket failure: {description}")]
    Socket {
        /// Human-readable detail from the JavaScript boundary.
        description: String,
    },
    /// The single-threaded driver seam was re-entered while borrowed.
    #[error("single-threaded driver seam re-entered")]
    SeamReentered,
    /// The driver emitted a command outside its documented contract (a core
    /// defect surfaced typed instead of being absorbed).
    #[error("driver contract violation: {description}")]
    DriverContract {
        /// What the driver emitted and where.
        description: String,
    },
}

/// Mutable single-threaded state behind the seam: the shared driver plus
/// diagnostics. Borrows of this cell are always scoped to pure driver calls —
/// no foreign (JavaScript or sink) code ever runs while it is held.
struct Seam {
    driver: WebSocketFrameDriver,
    /// Human-readable detail for the most recent observed socket fact, kept
    /// as diagnostics only (close code/reason, error opacity note, extension
    /// string). Decision weight never attaches to it.
    last_event_detail: Option<String>,
}

/// Signal queue plus sink. Signals are queued under a short borrow and
/// drained to the sink outside every seam borrow, so a sink handler may call
/// back into the adapter (send/close) without deadlocking the seam; a signal
/// produced during such a nested call is delivered by the outer drain loop.
struct SignalPort {
    queue: RefCell<VecDeque<AdapterSignal>>,
    sink: RefCell<AdapterSignalSink>,
}

impl SignalPort {
    /// Queues one signal and drains the queue to the sink if it is free.
    ///
    /// When the sink is busy (this emit happened inside a sink handler), the
    /// signal stays queued and the outer drain loop delivers it after the
    /// handler returns — nothing is dropped and nothing polls.
    fn emit(&self, signal: AdapterSignal) {
        match self.queue.try_borrow_mut() {
            Ok(mut queue) => queue.push_back(signal),
            // Queue borrows are scoped to push/pop with no foreign code
            // inside, so a conflict cannot arise on the single browser
            // thread; this arm exists so the closed handling stays total.
            Err(_) => return,
        }
        self.drain();
    }

    fn drain(&self) {
        loop {
            let Ok(mut sink) = self.sink.try_borrow_mut() else {
                // A sink handler is on the stack; it drains on return.
                return;
            };
            let next = match self.queue.try_borrow_mut() {
                Ok(mut queue) => queue.pop_front(),
                Err(_) => return,
            };
            let Some(signal) = next else {
                return;
            };
            sink(signal);
        }
    }
}

/// The browser (`web-sys`) WebSocket adapter for the transport-neutral
/// driver: `WebSysWebSocketSocket` in the goal document.
///
/// One adapter owns one socket lifetime, exactly like [`WsSocket`] on std:
/// it opens at construction (the driver's single `Open` command) and the
/// driver mints at most one typed fate. Reconnecting means a fresh adapter
/// under a fresh aggregate-issued authorization.
///
/// Dropping the adapter detaches the four callbacks before the closures are
/// freed and best-effort closes a still-live socket, so the browser can never
/// invoke a destroyed closure.
///
/// [`WsSocket`]: super::super::std_socket
pub struct WebSysWebSocketSocket {
    socket: WebSocket,
    seam: Rc<RefCell<Seam>>,
    port: Rc<SignalPort>,
    _on_open: Closure<dyn FnMut(Event)>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_close: Closure<dyn FnMut(CloseEvent)>,
    _on_error: Closure<dyn FnMut(Event)>,
}

impl WebSysWebSocketSocket {
    /// Opens the browser socket toward `url` and binds it to a fresh driver.
    ///
    /// Order is load-bearing (F4): the driver authorizes its single open,
    /// the `WebSocket` is constructed (which IS the `Open` execution),
    /// `binaryType` is pinned to `"arraybuffer"`, and only then are the
    /// `open`/`message`/`close`/`error` subscriptions attached.
    ///
    /// # Errors
    ///
    /// Returns [`BrowserSocketError::CommandRefused`] when the fresh driver
    /// refuses the open (impossible by construction, surfaced typed),
    /// [`BrowserSocketError::Socket`] when the browser rejects the URL, and
    /// [`BrowserSocketError::DriverContract`] when the driver emits anything
    /// but `Open` for its open command.
    pub fn open(url: &str, sink: AdapterSignalSink) -> Result<Self, BrowserSocketError> {
        let mut driver = WebSocketFrameDriver::new();
        let command = driver
            .command_open()
            .map_err(|refusal| BrowserSocketError::CommandRefused { refusal })?;
        match command {
            SocketCommand::Open => {}
            SocketCommand::SendBinary(_) | SocketCommand::Close => {
                return Err(BrowserSocketError::DriverContract {
                    description: format!("command_open emitted {command:?} instead of Open"),
                });
            }
        }

        let socket = WebSocket::new(url).map_err(|value| BrowserSocketError::Socket {
            description: format!("browser refused websocket construction for {url}: {value:?}"),
        })?;
        // F4: pinned before any subscription exists, so no message can ever
        // be observed under the asynchronous Blob default.
        socket.set_binary_type(BinaryType::Arraybuffer);

        let seam = Rc::new(RefCell::new(Seam {
            driver,
            last_event_detail: None,
        }));
        let port = Rc::new(SignalPort {
            queue: RefCell::new(VecDeque::new()),
            sink: RefCell::new(sink),
        });

        let on_open = {
            let seam = Rc::clone(&seam);
            let port = Rc::clone(&port);
            let socket = socket.clone();
            Closure::wrap(Box::new(move |_event: Event| {
                let negotiated = socket.extensions();
                let detail = if negotiated.is_empty() {
                    None
                } else {
                    Some(format!(
                        "browser negotiated websocket extensions in violation of the \
                         extension-free contract: {negotiated}"
                    ))
                };
                let event = super::mirror::open_event(&negotiated);
                dispatch(&seam, &socket, &port, event, detail);
            }) as Box<dyn FnMut(Event)>)
        };

        let on_message = {
            let seam = Rc::clone(&seam);
            let port = Rc::clone(&port);
            let socket = socket.clone();
            Closure::wrap(Box::new(move |event: MessageEvent| {
                let (data, detail) = classify_message_data(&event.data());
                let event = super::mirror::message_event(data);
                dispatch(&seam, &socket, &port, event, detail);
            }) as Box<dyn FnMut(MessageEvent)>)
        };

        let on_close = {
            let seam = Rc::clone(&seam);
            let port = Rc::clone(&port);
            let socket = socket.clone();
            Closure::wrap(Box::new(move |event: CloseEvent| {
                // Close code/reason/wasClean are retained verbatim as
                // diagnostics; only wasClean carries conversion weight.
                let detail = Some(format!(
                    "browser close event: code {}, wasClean {}, reason {:?}",
                    event.code(),
                    event.was_clean(),
                    event.reason(),
                ));
                let event = super::mirror::close_event(event.was_clean());
                dispatch(&seam, &socket, &port, event, detail);
            }) as Box<dyn FnMut(CloseEvent)>)
        };

        let on_error = {
            let seam = Rc::clone(&seam);
            let port = Rc::clone(&port);
            let socket = socket.clone();
            Closure::wrap(Box::new(move |_event: Event| {
                // Browser error events are opaque by specification; the
                // typed class states that fact and invents no detail.
                let detail = Some(String::from(
                    "browser websocket error event (opaque by specification)",
                ));
                dispatch(&seam, &socket, &port, super::mirror::error_event(), detail);
            }) as Box<dyn FnMut(Event)>)
        };

        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        Ok(Self {
            socket,
            seam,
            port,
            _on_open: on_open,
            _on_message: on_message,
            _on_close: on_close,
            _on_error: on_error,
        })
    }

    /// Sends one complete canonical binary message through the driver.
    ///
    /// # Errors
    ///
    /// Returns [`BrowserSocketError::CommandRefused`] when the driver refuses
    /// the send (wrong phase, or a correlated exchange already outstanding —
    /// the Q-B rule that also bounds outbound backpressure structurally),
    /// [`BrowserSocketError::SeamReentered`] when the seam is busy, and
    /// [`BrowserSocketError::Socket`] when the browser send fails — in which
    /// case the failure is also fed to the driver as a typed socket fact so
    /// the fate is minted, not absorbed.
    pub fn send_binary(
        &self,
        bytes: Vec<u8>,
        expectation: ResponseExpectation,
    ) -> Result<(), BrowserSocketError> {
        let command = {
            let mut seam = self
                .seam
                .try_borrow_mut()
                .map_err(|_| BrowserSocketError::SeamReentered)?;
            seam.driver
                .command_send(bytes, expectation)
                .map_err(|refusal| BrowserSocketError::CommandRefused { refusal })?
        };
        match action_for_command(command) {
            Ok(BrowserSocketAction::SendBinary(bytes)) => {
                if let Err(value) = self.socket.send_with_u8_array(&bytes) {
                    let description = format!("browser websocket send failed: {value:?}");
                    dispatch(
                        &self.seam,
                        &self.socket,
                        &self.port,
                        SocketEvent::Failed(SocketFailure::Transport),
                        Some(description.clone()),
                    );
                    return Err(BrowserSocketError::Socket { description });
                }
                Ok(())
            }
            Ok(action @ BrowserSocketAction::Close) => Err(BrowserSocketError::DriverContract {
                description: format!("command_send mapped to a non-send action: {action:?}"),
            }),
            Err(refusal) => Err(BrowserSocketError::DriverContract {
                description: format!("command_send emitted a non-send command: {refusal}"),
            }),
        }
    }

    /// Commands the close of this socket lifetime; the echoed browser close
    /// event later mints the driver's `CloseCompleted` fate (F3).
    ///
    /// # Errors
    ///
    /// Returns [`BrowserSocketError::CommandRefused`] when the driver refuses
    /// the close in its current phase, [`BrowserSocketError::SeamReentered`]
    /// when the seam is busy, and [`BrowserSocketError::Socket`] when the
    /// browser close call itself fails.
    pub fn close(&self) -> Result<(), BrowserSocketError> {
        let command = {
            let mut seam = self
                .seam
                .try_borrow_mut()
                .map_err(|_| BrowserSocketError::SeamReentered)?;
            seam.driver
                .command_close()
                .map_err(|refusal| BrowserSocketError::CommandRefused { refusal })?
        };
        match action_for_command(command) {
            Ok(BrowserSocketAction::Close) => {
                self.socket
                    .close()
                    .map_err(|value| BrowserSocketError::Socket {
                        description: format!("browser websocket close failed: {value:?}"),
                    })
            }
            Ok(action @ BrowserSocketAction::SendBinary(_)) => {
                Err(BrowserSocketError::DriverContract {
                    description: format!("command_close mapped to a non-close action: {action:?}"),
                })
            }
            Err(refusal) => Err(BrowserSocketError::DriverContract {
                description: format!("command_close emitted a non-close command: {refusal}"),
            }),
        }
    }

    /// Current driver lifecycle phase.
    ///
    /// # Errors
    ///
    /// Returns [`BrowserSocketError::SeamReentered`] when the seam is busy.
    pub fn phase(&self) -> Result<DriverPhase, BrowserSocketError> {
        self.seam
            .try_borrow()
            .map(|seam| seam.driver.phase())
            .map_err(|_| BrowserSocketError::SeamReentered)
    }

    /// Detail recorded with the most recent observed socket fact, if any
    /// (diagnostics only — close code/reason, extension string, opacity note).
    ///
    /// # Errors
    ///
    /// Returns [`BrowserSocketError::SeamReentered`] when the seam is busy.
    pub fn last_event_detail(&self) -> Result<Option<String>, BrowserSocketError> {
        self.seam
            .try_borrow()
            .map(|seam| seam.last_event_detail.clone())
            .map_err(|_| BrowserSocketError::SeamReentered)
    }
}

impl Drop for WebSysWebSocketSocket {
    fn drop(&mut self) {
        // Detach before the closures are freed so the browser can never call
        // into a destroyed closure.
        self.socket.set_onopen(None);
        self.socket.set_onmessage(None);
        self.socket.set_onclose(None);
        self.socket.set_onerror(None);
        let live = !matches!(
            self.seam.try_borrow().map(|seam| seam.driver.phase()),
            Ok(DriverPhase::Terminated)
        );
        if live {
            // Best-effort: the no-argument close cannot be refused for a code
            // range per the WebSocket specification, but the closed handling
            // stays total — a refusal is reported, never swallowed.
            if let Err(value) = self.socket.close() {
                self.port
                    .emit(AdapterSignal::Fault(AdapterFault::CommandExecutionFailed {
                        description: format!("browser websocket close on drop failed: {value:?}"),
                    }));
            }
        }
    }
}

impl core::fmt::Debug for WebSysWebSocketSocket {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("WebSysWebSocketSocket")
            .finish_non_exhaustive()
    }
}

/// Classifies one `message` event's data for the mirror, extracting bytes
/// only from the `ArrayBuffer` shape (F4).
fn classify_message_data(data: &wasm_bindgen::JsValue) -> (BrowserMessageData, Option<String>) {
    if let Some(buffer) = data.dyn_ref::<js_sys::ArrayBuffer>() {
        let bytes = js_sys::Uint8Array::new(buffer).to_vec();
        return (BrowserMessageData::ArrayBuffer(bytes), None);
    }
    if data.as_string().is_some() {
        let detail = String::from("peer sent a text message on the binary-only liminal route");
        return (BrowserMessageData::Text, Some(detail));
    }
    if data.dyn_ref::<web_sys::Blob>().is_some() {
        let detail =
            String::from("browser delivered a Blob despite the pinned arraybuffer binaryType");
        return (BrowserMessageData::Blob, Some(detail));
    }
    (
        BrowserMessageData::Unrecognized,
        Some(String::from("browser delivered unrecognized message data")),
    )
}

/// Feeds one mirrored socket fact into the shared driver, executes at most
/// one emitted command on the browser socket, and forwards the decision to
/// the owner's sink — with every borrow released before foreign code runs.
fn dispatch(
    seam: &Rc<RefCell<Seam>>,
    socket: &WebSocket,
    port: &Rc<SignalPort>,
    event: SocketEvent,
    detail: Option<String>,
) {
    let step = if let Ok(mut seam) = seam.try_borrow_mut() {
        if let Some(detail) = detail {
            seam.last_event_detail = Some(detail);
        }
        seam.driver.handle_event(event)
    } else {
        // Structurally unreachable on the single browser thread (no foreign
        // code runs under a seam borrow); reported typed.
        port.emit(AdapterSignal::Fault(AdapterFault::SeamReentered));
        return;
    };
    if let Some(command) = step.command {
        // `handle_event` only ever emits Close (terminal minting); the
        // mapping stays total and a contract break is reported typed.
        match action_for_command(command) {
            Ok(BrowserSocketAction::Close) => {
                if let Err(value) = socket.close() {
                    port.emit(AdapterSignal::Fault(AdapterFault::CommandExecutionFailed {
                        description: format!("browser websocket close failed: {value:?}"),
                    }));
                }
            }
            Ok(BrowserSocketAction::SendBinary(_)) => {
                port.emit(AdapterSignal::Fault(AdapterFault::CommandExecutionFailed {
                    description: String::from(
                        "driver emitted SendBinary from handle_event, outside its contract",
                    ),
                }));
            }
            Err(refusal) => {
                port.emit(AdapterSignal::Fault(AdapterFault::CommandExecutionFailed {
                    description: format!(
                        "driver emitted Open from handle_event, outside its contract: \
                             {refusal}"
                    ),
                }));
            }
        }
    }
    port.emit(AdapterSignal::Output(step.output));
}
