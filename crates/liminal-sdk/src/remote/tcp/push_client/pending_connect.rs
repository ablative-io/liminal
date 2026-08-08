use alloc::vec::Vec;
use core::time::Duration;

use liminal::protocol::WorkerRegistration;

use super::PushClient;
use crate::SdkError;

/// A pending push-client connection with a caller-selected setup deadline.
///
/// The duration has two setup-only roles: it is the maximum wait for any one
/// socket read, and it supplies the wall-clock deadline for each synchronous
/// control-frame reply. A registration connect performs two control exchanges
/// (`Connect`/`ConnectAck` and `WorkerRegister`/`WorkerRegisterAck`), each with
/// its own deadline. The duration does not bound the TCP open or socket writes,
/// and it is removed before the background reader starts.
#[non_exhaustive]
pub struct PendingPushConnect<'address> {
    address: &'address str,
    setup_deadline: Duration,
    auth_token: Vec<u8>,
    registration: Option<WorkerRegistration>,
}

impl<'address> PendingPushConnect<'address> {
    pub(super) const fn new(address: &'address str, setup_deadline: Duration) -> Self {
        Self {
            address,
            setup_deadline,
            auth_token: Vec::new(),
            registration: None,
        }
    }

    /// Adds the authentication token carried by the connect handshake.
    #[must_use]
    pub fn with_auth_token(mut self, auth_token: &[u8]) -> Self {
        self.auth_token = auth_token.to_vec();
        self
    }

    /// Adds the worker registration exchange that runs after the connect
    /// handshake and before the background reader starts.
    #[must_use]
    pub fn with_registration(mut self, registration: WorkerRegistration) -> Self {
        self.registration = Some(registration);
        self
    }

    /// Connects, performs the configured synchronous control exchanges, and
    /// starts the background reader.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection or socket
    /// configuration fails or authentication is rejected, and
    /// [`SdkError::Protocol`] when another control exchange is rejected or the
    /// socket cannot be cloned for the reader thread.
    pub fn connect(self) -> Result<PushClient, SdkError> {
        let Self {
            address,
            setup_deadline,
            auth_token,
            registration,
        } = self;
        PushClient::connect_configured(address, &auth_token, registration, setup_deadline)
    }
}
