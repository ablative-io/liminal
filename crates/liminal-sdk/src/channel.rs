#![allow(clippy::module_name_repetitions)]

use core::future::Future;

use futures_core::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{SchemaValidate, SdkError};

/// Application-facing typed channel API.
///
/// `ChannelHandle` intentionally exposes typed messages only. It does not expose
/// envelopes, byte buffers, protocol frames, publisher identifiers, or transport
/// handles; embedded and remote implementations perform any necessary
/// serialization and schema validation behind this trait.
///
/// # Object safety
///
/// This trait is not object-safe because its methods are generic over message
/// types and because subscriptions/replies are represented with generic
/// associated return types. Use concrete handle types behind generic bounds such
/// as `H: ChannelHandle`, or define an application enum over the concrete handle
/// implementations that need to be selected at runtime. Future SDK layers may
/// add an explicitly erased adapter without weakening the typed API here.
///
/// ```compile_fail
/// use liminal_sdk::ChannelHandle;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct OnlySerializable {
///     id: String,
/// }
///
/// fn publish_without_schema<H>(handle: &H, message: OnlySerializable)
/// where
///     H: ChannelHandle,
/// {
///     let _ = handle.publish(message);
/// }
/// ```
pub trait ChannelHandle: core::fmt::Debug + Send + Sync {
    /// Stream returned by [`subscribe`](Self::subscribe) for message type `M`.
    type Subscription<M>: Stream<Item = Result<M, SdkError>>
    where
        M: DeserializeOwned;

    /// Future returned by [`request_reply`](Self::request_reply) for reply type `Resp`.
    type ReplyFuture<'a, Resp>: Future<Output = Result<Resp, SdkError>> + 'a
    where
        Self: 'a,
        Resp: DeserializeOwned + 'a;

    /// Publishes a typed message to the channel.
    ///
    /// The message type must be serializable and must declare schema metadata;
    /// a merely serializable value is rejected at compile time.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the concrete channel implementation cannot
    /// accept, serialize, validate, or deliver the message.
    fn publish<M>(&self, message: M) -> Result<(), SdkError>
    where
        M: Serialize + SchemaValidate;

    /// Subscribes to typed channel messages.
    ///
    /// Subscription and delivery failures are surfaced to the application as
    /// [`SdkError`] items in the returned stream.
    fn subscribe<M>(&self) -> Self::Subscription<M>
    where
        M: DeserializeOwned;

    /// Sends a typed request and resolves with a typed reply.
    ///
    /// The request type must be serializable and schema-declared. The reply type
    /// must be owned after deserialization so callers never borrow transport
    /// buffers managed by an SDK implementation.
    ///
    /// # Errors
    ///
    /// The returned future resolves to [`SdkError`] when the concrete channel
    /// implementation cannot send the request, observe the reply, or deserialize
    /// the reply payload.
    fn request_reply<Req, Resp>(&self, request: Req) -> Self::ReplyFuture<'_, Resp>
    where
        Req: Serialize + SchemaValidate,
        Resp: DeserializeOwned;
}
