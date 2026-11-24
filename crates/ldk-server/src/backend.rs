use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bitcoin::hashes::Hash;
use cdk_common::amount::{Amount, MSAT_IN_SAT};
use cdk_common::common::FeeReserve;
use cdk_common::nuts::{CurrencyUnit, MeltOptions, MeltQuoteState};
use cdk_common::payment::{
    self, CreateIncomingPaymentResponse, Event, IncomingPaymentOptions, MakePaymentResponse,
    MintPayment, OutgoingPaymentOptions, PaymentIdentifier, PaymentQuoteResponse, SettingsResponse,
    WaitPaymentResponse,
};
use cdk_common::util::{hex, unix_time};
use cdk_common::QuoteId;
use futures::Stream;
use ldk_server_client::client::{EventStream, LdkServerClient};
use ldk_server_client::error::{LdkServerError, LdkServerErrorCode};
use ldk_server_client::ldk_server_grpc::api::{
    Bolt11ReceiveRequest, Bolt11ReceiveResponse, Bolt11SendRequest, Bolt11SendResponse,
    Bolt12ReceiveRequest, Bolt12ReceiveResponse, Bolt12SendRequest, Bolt12SendResponse,
    GetNodeInfoRequest, GetNodeInfoResponse, GetPaymentDetailsRequest, GetPaymentDetailsResponse,
    ListPaymentsRequest, ListPaymentsResponse,
};
use ldk_server_client::ldk_server_grpc::events::{event_envelope, EventEnvelope};
use ldk_server_client::ldk_server_grpc::node_error_messages;
use ldk_server_client::ldk_server_grpc::types::{
    bolt11_invoice_description, payment_kind, Bolt11InvoiceDescription, Payment, PaymentDirection,
    PaymentStatus, RouteParametersConfig,
};
use lightning::offers::offer::Amount as OfferAmount;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::settings::AdvertisedMethod;
#[cfg(test)]
use crate::uncertain_sends::IN_FLIGHT_BOUND_SECS;
use crate::uncertain_sends::{SendVerdict, UncertainSends};

const DEFAULT_INVOICE_EXPIRY_SECS: u32 = 36_000;
const DEFAULT_PAYMENT_WAIT_SECS: u64 = 10;
const OUTGOING_CONTEXT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const UNMATCHED_OUTGOING_EVENT_TTL: Duration = Duration::from_secs(60);
/// 200 pages of 50 payments: a scan reads at most 10,000 payments.
pub(crate) const DEFAULT_MAX_PAYMENT_SCAN_PAGES: u16 = 200;
const ROUTE_DEFAULT_MAX_TOTAL_CLTV_EXPIRY_DELTA: u32 = 1008;
/// Exit code when the state file of uncertain sends cannot be written.
const STATE_WRITE_FAILURE_EXIT_CODE: i32 = 70;

/// Stops the process. A send without a stored mark could later be reported
/// with a stale Failed record, so the processor does not continue.
fn exit_on_state_write_failure(error: String) -> ! {
    tracing::error!("Stopping: cannot store an uncertain BOLT11 send: {error}");
    std::process::exit(STATE_WRITE_FAILURE_EXIT_CODE)
}
const ROUTE_DEFAULT_MAX_PATH_COUNT: u32 = 10;
const ROUTE_DEFAULT_MAX_CHANNEL_SATURATION_POWER_OF_HALF: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OutgoingPaymentContext {
    quote_id: QuoteId,
    unit: CurrencyUnit,
    /// `latest_update_timestamp` of the stored record before this attempt. A
    /// failure event that is not newer belongs to an earlier attempt.
    baseline: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutgoingPaymentOutcome {
    Succeeded,
    Failed,
}

#[derive(Clone)]
struct PendingOutgoingContext {
    context: OutgoingPaymentContext,
    inserted_at: Instant,
}

struct UnmatchedOutgoingPayment {
    keys: Vec<String>,
    payment: Payment,
    outcome: OutgoingPaymentOutcome,
    received_at: Instant,
}

struct OutgoingPaymentCorrelatorState {
    by_key: HashMap<String, PendingOutgoingContext>,
    unmatched: VecDeque<UnmatchedOutgoingPayment>,
    ready: VecDeque<Event>,
    finalized: HashMap<QuoteId, Instant>,
}

impl OutgoingPaymentCorrelatorState {
    fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            unmatched: VecDeque::new(),
            ready: VecDeque::new(),
            finalized: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct OutgoingPaymentCorrelator {
    state: Arc<Mutex<OutgoingPaymentCorrelatorState>>,
    notify: Arc<Notify>,
}

impl OutgoingPaymentCorrelator {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(OutgoingPaymentCorrelatorState::new())),
            notify: Arc::new(Notify::new()),
        }
    }
}

type OutgoingPaymentCorrelations = OutgoingPaymentCorrelator;

/// The BOLT11 sends that run in `make_payment` now, by lowercase hash.
#[derive(Clone, Default)]
struct SendsInProgress(Arc<std::sync::Mutex<HashMap<String, usize>>>);

impl SendsInProgress {
    fn start(&self, hash: &str) -> SendInProgress {
        let hash = hash.to_ascii_lowercase();
        *self
            .0
            .lock()
            .expect("send set lock")
            .entry(hash.clone())
            .or_default() += 1;
        SendInProgress {
            sends: self.clone(),
            hash,
        }
    }

    fn is_running(&self, hash: &str) -> bool {
        self.0
            .lock()
            .expect("send set lock")
            .contains_key(&hash.to_ascii_lowercase())
    }
}

/// Removes its send from the set when `make_payment` ends or is cancelled.
struct SendInProgress {
    sends: SendsInProgress,
    hash: String,
}

impl Drop for SendInProgress {
    fn drop(&mut self) {
        let mut sends = self.sends.0.lock().expect("send set lock");
        if let Some(count) = sends.get_mut(&self.hash) {
            *count -= 1;
            if *count == 0 {
                sends.remove(&self.hash);
            }
        }
    }
}

/// True when a failed `Bolt11Send` does not prove that nothing was dispatched.
///
/// `prior` is the stored record before the send.
fn send_error_is_uncertain(error: &LdkServerError, prior: Option<&Payment>) -> bool {
    match error.error_code {
        // The server checks these before it runs the send.
        LdkServerErrorCode::InvalidRequestError
        | LdkServerErrorCode::AuthError
        | LdkServerErrorCode::AuthorizationError => false,
        // A Lightning error comes before a dispatch. A duplicate behind a
        // Failed or missing record means that the channel manager still
        // tracks an earlier send of the hash.
        LdkServerErrorCode::LightningError => {
            error
                .message
                .contains(node_error_messages::DUPLICATE_PAYMENT)
                && prior.is_none_or(|payment| {
                    matches!(payment_status(payment), Ok(PaymentStatus::Failed))
                })
        }
        // A failed store write can come after the dispatch. A node that is
        // not running dispatches nothing.
        LdkServerErrorCode::InternalServerError => {
            !error.message.contains(node_error_messages::NOT_RUNNING)
        }
        // A request that failed while it connected did not reach the server.
        // Any other transport failure can come after the server received it.
        LdkServerErrorCode::InternalError => !error.is_connection_failure(),
    }
}

fn unknown_payment_response(payment_identifier: &PaymentIdentifier) -> MakePaymentResponse {
    MakePaymentResponse {
        payment_lookup_id: payment_identifier.clone(),
        payment_proof: None,
        status: MeltQuoteState::Unknown,
        total_spent: Amount::new(0, CurrencyUnit::Msat),
    }
}

/// The LDK Server calls that the backend makes.
///
/// [`LdkServerClient`] implements it. A test can supply a fake server.
#[async_trait]
pub trait LdkServerApi: Send + Sync {
    /// See [`LdkServerClient::get_node_info`].
    async fn get_node_info(
        &self,
        request: GetNodeInfoRequest,
    ) -> Result<GetNodeInfoResponse, LdkServerError>;
    /// See [`LdkServerClient::bolt11_receive`].
    async fn bolt11_receive(
        &self,
        request: Bolt11ReceiveRequest,
    ) -> Result<Bolt11ReceiveResponse, LdkServerError>;
    /// See [`LdkServerClient::bolt12_receive`].
    async fn bolt12_receive(
        &self,
        request: Bolt12ReceiveRequest,
    ) -> Result<Bolt12ReceiveResponse, LdkServerError>;
    /// See [`LdkServerClient::bolt11_send`].
    async fn bolt11_send(
        &self,
        request: Bolt11SendRequest,
    ) -> Result<Bolt11SendResponse, LdkServerError>;
    /// See [`LdkServerClient::bolt12_send`].
    async fn bolt12_send(
        &self,
        request: Bolt12SendRequest,
    ) -> Result<Bolt12SendResponse, LdkServerError>;
    /// See [`LdkServerClient::get_payment_details`].
    async fn get_payment_details(
        &self,
        request: GetPaymentDetailsRequest,
    ) -> Result<GetPaymentDetailsResponse, LdkServerError>;
    /// See [`LdkServerClient::list_payments`].
    async fn list_payments(
        &self,
        request: ListPaymentsRequest,
    ) -> Result<ListPaymentsResponse, LdkServerError>;
    /// See [`LdkServerClient::subscribe_events`].
    async fn subscribe_events(&self) -> Result<EventStream, LdkServerError>;
}

#[async_trait]
impl LdkServerApi for LdkServerClient {
    async fn get_node_info(
        &self,
        request: GetNodeInfoRequest,
    ) -> Result<GetNodeInfoResponse, LdkServerError> {
        LdkServerClient::get_node_info(self, request).await
    }

    async fn bolt11_receive(
        &self,
        request: Bolt11ReceiveRequest,
    ) -> Result<Bolt11ReceiveResponse, LdkServerError> {
        LdkServerClient::bolt11_receive(self, request).await
    }

    async fn bolt12_receive(
        &self,
        request: Bolt12ReceiveRequest,
    ) -> Result<Bolt12ReceiveResponse, LdkServerError> {
        LdkServerClient::bolt12_receive(self, request).await
    }

    async fn bolt11_send(
        &self,
        request: Bolt11SendRequest,
    ) -> Result<Bolt11SendResponse, LdkServerError> {
        LdkServerClient::bolt11_send(self, request).await
    }

    async fn bolt12_send(
        &self,
        request: Bolt12SendRequest,
    ) -> Result<Bolt12SendResponse, LdkServerError> {
        LdkServerClient::bolt12_send(self, request).await
    }

    async fn get_payment_details(
        &self,
        request: GetPaymentDetailsRequest,
    ) -> Result<GetPaymentDetailsResponse, LdkServerError> {
        LdkServerClient::get_payment_details(self, request).await
    }

    async fn list_payments(
        &self,
        request: ListPaymentsRequest,
    ) -> Result<ListPaymentsResponse, LdkServerError> {
        LdkServerClient::list_payments(self, request).await
    }

    async fn subscribe_events(&self) -> Result<EventStream, LdkServerError> {
        LdkServerClient::subscribe_events(self).await
    }
}

/// LDK Server backend configuration.
#[derive(Clone)]
pub struct Config {
    /// Server address without scheme, for example `127.0.0.1:3536`.
    pub address: String,
    /// Private hex-encoded v2 macaroon issued by LDK Server. The client binds a
    /// copy of it to each request.
    pub macaroon: String,
    /// PEM encoded TLS certificate to pin.
    pub cert_pem: Vec<u8>,
    /// Fee reserve used for melt quotes.
    pub fee_reserve: FeeReserve,
    /// Maximum `ListPayments` pages to scan for incoming status lookups.
    pub max_payment_scan_pages: u16,
    /// Rails this backend reports in `get_settings`, and therefore the only ones
    /// a mint will register it for.
    pub advertised_methods: Vec<AdvertisedMethod>,
    /// File that keeps the uncertain BOLT11 sends across restarts. `None` keeps
    /// them in memory only.
    pub state_path: Option<PathBuf>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.address)
            .field("macaroon", &"[REDACTED]")
            .field("cert_pem", &format!("{} bytes", self.cert_pem.len()))
            .field("fee_reserve", &self.fee_reserve)
            .field("max_payment_scan_pages", &self.max_payment_scan_pages)
            .field("state_path", &self.state_path)
            .finish()
    }
}

#[allow(dead_code)]
impl Config {
    /// Create a new LDK Server backend configuration.
    pub fn new(
        address: String,
        macaroon: String,
        cert_pem: Vec<u8>,
        fee_reserve: FeeReserve,
    ) -> Self {
        Self {
            address,
            macaroon,
            cert_pem,
            fee_reserve,
            max_payment_scan_pages: DEFAULT_MAX_PAYMENT_SCAN_PAGES,
            advertised_methods: AdvertisedMethod::ALL.to_vec(),
            state_path: None,
        }
    }

    /// Restrict the methods this backend advertises to the mint.
    pub fn with_advertised_methods(mut self, methods: Vec<AdvertisedMethod>) -> Self {
        self.advertised_methods = methods;
        self
    }

    /// Set the maximum number of payment-history pages scanned for incoming payment status.
    pub fn with_max_payment_scan_pages(mut self, max_payment_scan_pages: u16) -> Self {
        self.max_payment_scan_pages = max_payment_scan_pages;
        self
    }
}

/// LDK Server payment backend.
#[derive(Clone)]
pub struct LdkServerBackend {
    client: Arc<dyn LdkServerApi>,
    fee_reserve: FeeReserve,
    wait_invoice_cancel_token: CancellationToken,
    wait_invoice_is_active: Arc<AtomicBool>,
    outgoing_payment_correlations: OutgoingPaymentCorrelations,
    uncertain_sends: Arc<Mutex<UncertainSends>>,
    on_state_write_failure: fn(String) -> !,
    sends_in_progress: SendsInProgress,
    max_payment_scan_pages: u16,
    advertised_methods: Vec<AdvertisedMethod>,
}

impl std::fmt::Debug for LdkServerBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LdkServerBackend")
            .field("fee_reserve", &self.fee_reserve)
            .field("max_payment_scan_pages", &self.max_payment_scan_pages)
            .finish_non_exhaustive()
    }
}

/// Build the settings a mint reads to decide which rails to register this
/// backend for.
///
/// A mint registers a backend per `(unit, method)` pair and claims every method
/// the backend advertises here, so restricting `methods` is what allows this
/// processor to sit alongside another one instead of competing with it.
fn settings_response(methods: &[AdvertisedMethod]) -> SettingsResponse {
    let advertises = |method: AdvertisedMethod| methods.contains(&method);

    SettingsResponse {
        unit: CurrencyUnit::Msat.to_string(),
        bolt11: advertises(AdvertisedMethod::Bolt11).then_some(payment::Bolt11Settings {
            mpp: false,
            amountless: true,
            invoice_description: true,
        }),
        bolt12: advertises(AdvertisedMethod::Bolt12).then_some(payment::Bolt12Settings {
            amountless: true,
            invoice_description: true,
        }),
        onchain: None,
        custom: std::collections::HashMap::new(),
    }
}

impl LdkServerBackend {
    /// Create a new LDK Server backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS certificate cannot be parsed or the client cannot be built.
    pub fn new(config: Config) -> Result<Self, Error> {
        let address = normalize_address(&config.address);
        let client = LdkServerClient::new(address, config.macaroon, &config.cert_pem)
            .map_err(Error::Client)?;

        let uncertain_sends = match config.state_path {
            Some(path) => UncertainSends::open(path).map_err(Error::StateFile)?,
            None => {
                tracing::warn!(
                    "No state path: a restart loses the marks of uncertain BOLT11 sends"
                );
                UncertainSends::in_memory()
            }
        };

        Ok(Self::with_api(
            Arc::new(client),
            config.fee_reserve,
            config.max_payment_scan_pages,
            config.advertised_methods,
            uncertain_sends,
        ))
    }

    fn with_api(
        client: Arc<dyn LdkServerApi>,
        fee_reserve: FeeReserve,
        max_payment_scan_pages: u16,
        advertised_methods: Vec<AdvertisedMethod>,
        uncertain_sends: UncertainSends,
    ) -> Self {
        Self {
            client,
            fee_reserve,
            wait_invoice_cancel_token: CancellationToken::new(),
            wait_invoice_is_active: Arc::new(AtomicBool::new(false)),
            outgoing_payment_correlations: OutgoingPaymentCorrelator::new(),
            uncertain_sends: Arc::new(Mutex::new(uncertain_sends)),
            on_state_write_failure: exit_on_state_write_failure,
            sends_in_progress: SendsInProgress::default(),
            max_payment_scan_pages,
            advertised_methods,
        }
    }

    /// Reports a stored BOLT11 send, or `Unknown` while it can be stale.
    ///
    /// With `missing_is_failed`, a hash without a record, without a mark and
    /// without a running send was never dispatched: every send of this
    /// processor has a mark before its dispatch. That definite result lets the
    /// mint release the proofs of a melt that the node never sent.
    async fn reported_bolt11_send(
        &self,
        hash: &str,
        payment: Option<Payment>,
        payment_identifier: &PaymentIdentifier,
        missing_is_failed: bool,
    ) -> Result<MakePaymentResponse, payment::Error> {
        let verdict =
            self.uncertain_sends
                .lock()
                .await
                .verdict(hash, payment.as_ref(), unix_time());
        match (verdict, payment) {
            (SendVerdict::Unknown, _) => {
                tracing::warn!(
                    "Reporting BOLT11 send {} as unknown: its stored record can be stale",
                    hash
                );
                Ok(unknown_payment_response(payment_identifier))
            }
            (SendVerdict::Record, Some(payment)) => Self::make_payment_response_from_payment(
                &CurrencyUnit::Msat,
                payment_identifier.clone(),
                &payment,
            ),
            (SendVerdict::Record, None) if missing_is_failed => {
                if self.sends_in_progress.is_running(hash) {
                    return Ok(unknown_payment_response(payment_identifier));
                }
                if self.uncertain_sends.lock().await.is_young(unix_time()) {
                    // A lost earlier state file can have held a mark for it.
                    return Ok(unknown_payment_response(payment_identifier));
                }
                // A send can start and end during the history scan. Read the
                // record again after the mark and running checks.
                if let Some(payment) = self.get_payment_details(hash).await? {
                    let verdict = self.uncertain_sends.lock().await.verdict(
                        hash,
                        Some(&payment),
                        unix_time(),
                    );
                    return match verdict {
                        SendVerdict::Unknown => Ok(unknown_payment_response(payment_identifier)),
                        SendVerdict::Record => Self::make_payment_response_from_payment(
                            &CurrencyUnit::Msat,
                            payment_identifier.clone(),
                            &payment,
                        ),
                    };
                }
                tracing::info!(
                    "BOLT11 send {} has no record and no mark: it was never sent",
                    hash
                );
                Ok(MakePaymentResponse {
                    payment_lookup_id: payment_identifier.clone(),
                    payment_proof: None,
                    status: MeltQuoteState::Failed,
                    total_spent: Amount::new(0, CurrencyUnit::Msat),
                })
            }
            (SendVerdict::Record, None) => Err(Error::PaymentNotFound.into()),
        }
    }

    async fn get_payment_details(&self, payment_id: &str) -> Result<Option<Payment>, Error> {
        let response = self
            .client
            .get_payment_details(GetPaymentDetailsRequest {
                payment_id: payment_id.to_string(),
            })
            .await?;

        Ok(response.payment)
    }

    /// Scans the payment history, newest first, for matching payments.
    ///
    /// With `first_only`, the scan ends at the first match. A scan that reaches
    /// `max_payment_scan_pages` returns the matches that it found. It fails
    /// only when it found none, because then an older page can hold a match.
    async fn list_matching_payments<F>(
        &self,
        mut matches_payment: F,
        first_only: bool,
    ) -> Result<Vec<Payment>, Error>
    where
        F: FnMut(&Payment) -> bool,
    {
        let mut page_token: Option<String> = None;
        let mut payments = Vec::new();

        for _ in 0..self.max_payment_scan_pages {
            let response = self
                .client
                .list_payments(ListPaymentsRequest { page_token })
                .await?;

            for payment in response.payments {
                if matches_payment(&payment) {
                    payments.push(payment);
                    if first_only {
                        return Ok(payments);
                    }
                }
            }

            match response.next_page_token {
                Some(next_page_token) => page_token = Some(next_page_token),
                None => return Ok(payments),
            }
        }

        if payments.is_empty() {
            return Err(Error::PaymentScanLimitExceeded {
                max_pages: self.max_payment_scan_pages,
            });
        }
        tracing::warn!(
            "Payment scan stopped after {} pages with {} matches; older pages were not read",
            self.max_payment_scan_pages,
            payments.len()
        );
        Ok(payments)
    }

    async fn wait_for_payment_details(
        &self,
        payment_id: &str,
        timeout_secs: Option<u64>,
    ) -> Result<Payment, Error> {
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_PAYMENT_WAIT_SECS));
        let start = std::time::Instant::now();

        loop {
            match self.get_payment_details(payment_id).await? {
                Some(payment) => return Ok(payment),
                None if start.elapsed() >= timeout => return Err(Error::PaymentNotFound),
                None => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }

    fn make_payment_response_from_payment(
        unit: &CurrencyUnit,
        payment_lookup_id: PaymentIdentifier,
        payment: &Payment,
    ) -> Result<MakePaymentResponse, payment::Error> {
        let status = payment_status(payment)?;
        let status = match status {
            PaymentStatus::Pending => MeltQuoteState::Pending,
            PaymentStatus::Succeeded => MeltQuoteState::Paid,
            PaymentStatus::Failed => MeltQuoteState::Failed,
        };

        let payment_proof = match payment_kind(payment)? {
            payment_kind::Kind::Bolt11(bolt11) => bolt11.preimage.clone(),
            payment_kind::Kind::Bolt12Offer(bolt12) => bolt12.preimage.clone(),
            _ => return Err(Error::UnexpectedPaymentKind.into()),
        };

        let total_spent = if status == MeltQuoteState::Paid {
            let total_spent = payment
                .amount_msat
                .ok_or(Error::CouldNotGetAmountSpent)?
                .checked_add(payment.fee_paid_msat.unwrap_or_default())
                .ok_or(Error::AmountOverflow)?;
            msat_total_spent_for_unit(total_spent, unit)?
        } else {
            Amount::new(0, unit.clone())
        };

        Ok(MakePaymentResponse {
            payment_lookup_id,
            payment_proof,
            status,
            total_spent,
        })
    }

    fn wait_payment_response_from_payment(
        payment: &Payment,
    ) -> Result<Option<WaitPaymentResponse>, Error> {
        if payment_direction(payment)? != PaymentDirection::Inbound {
            return Ok(None);
        }

        if payment_status(payment)? != PaymentStatus::Succeeded {
            return Ok(None);
        }

        let payment_amount = Amount::new(
            payment.amount_msat.ok_or(Error::CouldNotGetPaymentAmount)?,
            CurrencyUnit::Msat,
        );

        let (payment_identifier, payment_id) = match payment_kind(payment)? {
            payment_kind::Kind::Bolt11(bolt11) => (
                PaymentIdentifier::PaymentHash(hex_to_array(&bolt11.hash)?),
                bolt11.hash.clone(),
            ),
            payment_kind::Kind::Bolt12Offer(bolt12) => (
                PaymentIdentifier::OfferId(bolt12.offer_id.clone()),
                bolt12
                    .hash
                    .clone()
                    .unwrap_or_else(|| payment.payment_id.clone()),
            ),
            _ => return Ok(None),
        };

        Ok(Some(WaitPaymentResponse {
            payment_identifier,
            payment_amount,
            payment_id,
        }))
    }

    async fn event_from_envelope(
        envelope: EventEnvelope,
        correlations: &OutgoingPaymentCorrelations,
    ) -> Result<Option<Event>, payment::Error> {
        let event = match envelope.event {
            Some(event) => event,
            None => return Ok(None),
        };

        match event {
            event_envelope::Event::PaymentReceived(payment_received) => {
                let payment = match payment_received.payment {
                    Some(payment) => payment,
                    None => return Ok(None),
                };

                Ok(Self::wait_payment_response_from_payment(&payment)?.map(Event::PaymentReceived))
            }
            event_envelope::Event::PaymentSuccessful(payment_successful) => {
                let Some(payment) = payment_successful.payment else {
                    return Ok(None);
                };
                take_correlated_outgoing_event(
                    correlations,
                    payment,
                    OutgoingPaymentOutcome::Succeeded,
                )
                .await
            }
            event_envelope::Event::PaymentFailed(payment_failed) => {
                let Some(payment) = payment_failed.payment else {
                    return Ok(None);
                };
                take_correlated_outgoing_event(
                    correlations,
                    payment,
                    OutgoingPaymentOutcome::Failed,
                )
                .await
            }
            event_envelope::Event::PaymentForwarded(_)
            | event_envelope::Event::PaymentClaimable(_)
            | event_envelope::Event::ChannelStateChanged(_)
            | event_envelope::Event::SpliceNegotiated(_)
            | event_envelope::Event::SpliceNegotiationFailed(_) => Ok(None),
        }
    }
}

#[async_trait]
impl MintPayment for LdkServerBackend {
    type Err = payment::Error;

    async fn start(&self) -> Result<(), Self::Err> {
        self.client
            .get_node_info(GetNodeInfoRequest {})
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        Ok(settings_response(&self.advertised_methods))
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        match options {
            IncomingPaymentOptions::Bolt11(bolt11_options) => {
                let amount_msat = bolt11_options
                    .amount
                    .convert_to(&CurrencyUnit::Msat)?
                    .value();
                let expiry_secs = expiry_secs_or_default(bolt11_options.unix_expiry)?;
                let response = self
                    .client
                    .bolt11_receive(Bolt11ReceiveRequest {
                        amount_msat: Some(amount_msat),
                        description: Some(Bolt11InvoiceDescription {
                            kind: Some(bolt11_invoice_description::Kind::Direct(
                                bolt11_options.description.unwrap_or_default(),
                            )),
                        }),
                        expiry_secs,
                    })
                    .await
                    .map_err(Error::from)?;

                Ok(CreateIncomingPaymentResponse {
                    request_lookup_id: PaymentIdentifier::PaymentHash(hex_to_array(
                        &response.payment_hash,
                    )?),
                    request: response.invoice,
                    expiry: Some(unix_time() + u64::from(expiry_secs)),
                    extra_json: None,
                })
            }
            IncomingPaymentOptions::Bolt12(bolt12_options) => {
                let amount_msat = bolt12_options
                    .amount
                    .map(|amount| amount.convert_to(&CurrencyUnit::Msat).map(|a| a.value()))
                    .transpose()?;
                let expiry_secs = bolt12_options.unix_expiry.map(expiry_secs).transpose()?;
                let response = self
                    .client
                    .bolt12_receive(Bolt12ReceiveRequest {
                        description: bolt12_options.description.unwrap_or_default(),
                        amount_msat,
                        expiry_secs,
                        quantity: None,
                    })
                    .await
                    .map_err(Error::from)?;

                Ok(CreateIncomingPaymentResponse {
                    request_lookup_id: PaymentIdentifier::OfferId(response.offer_id),
                    request: response.offer,
                    expiry: bolt12_options.unix_expiry,
                    extra_json: None,
                })
            }
            IncomingPaymentOptions::Custom(_) | IncomingPaymentOptions::Onchain(_) => {
                Err(payment::Error::UnsupportedPaymentOption)
            }
        }
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        match options {
            OutgoingPaymentOptions::Bolt11(bolt11_options) => {
                let amount_msat = bolt11_amount_msat_for_quote(&bolt11_options)?;
                let amount = Amount::new(amount_msat, CurrencyUnit::Msat).convert_to(unit)?;
                let fee = self.fee_reserve_for_amount(unit, amount.value());

                Ok(PaymentQuoteResponse {
                    request_lookup_id: Some(PaymentIdentifier::PaymentHash(
                        *bolt11_options.bolt11.payment_hash().as_byte_array(),
                    )),
                    amount,
                    fee,
                    state: MeltQuoteState::Unpaid,
                    extra_json: None,
                    estimated_blocks: None,
                    fee_options: None,
                })
            }
            OutgoingPaymentOptions::Bolt12(bolt12_options) => {
                let amount_msat = bolt12_amount_msat_for_quote(&bolt12_options)?;
                let amount = Amount::new(amount_msat, CurrencyUnit::Msat).convert_to(unit)?;
                let fee = self.fee_reserve_for_amount(unit, amount.value());

                Ok(PaymentQuoteResponse {
                    request_lookup_id: None,
                    amount,
                    fee,
                    state: MeltQuoteState::Unpaid,
                    extra_json: None,
                    estimated_blocks: None,
                    fee_options: None,
                })
            }
            OutgoingPaymentOptions::Custom(_) | OutgoingPaymentOptions::Onchain(_) => {
                Err(payment::Error::UnsupportedPaymentOption)
            }
        }
    }

    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        match options {
            OutgoingPaymentOptions::Bolt11(bolt11_options) => {
                let amount_msat = bolt11_amount_msat_for_send(&bolt11_options)?;
                let route_parameters =
                    route_parameters_from_max_fee(bolt11_options.max_fee_amount.clone())?;
                let payment_hash =
                    hex::encode(bolt11_options.bolt11.payment_hash().as_byte_array());
                // Until a mark exists, a status check must not take the missing
                // record as proof that nothing was sent.
                let _in_progress = self.sends_in_progress.start(&payment_hash);
                // The record before the send: a later Failed record is current
                // only when it is newer. LDK Node uses the hash as payment ID.
                let prior = self.get_payment_details(&payment_hash).await?;
                let baseline = prior
                    .as_ref()
                    .map(|payment| payment.latest_update_timestamp);
                let context = OutgoingPaymentContext {
                    quote_id: bolt11_options.quote_id.clone(),
                    unit: unit.clone(),
                    baseline,
                };
                begin_outgoing_attempt(
                    &self.outgoing_payment_correlations,
                    vec![normalize_payment_correlation_key(&payment_hash)],
                    context.clone(),
                )
                .await;

                // Mark the send before the dispatch, so that a call that the
                // mint cancels during the send keeps its mark.
                let begun = self.uncertain_sends.lock().await.begin_send(
                    &payment_hash,
                    baseline,
                    unix_time(),
                );
                let new_mark = match begun {
                    Ok(new_mark) => new_mark,
                    Err(error) => (self.on_state_write_failure)(error),
                };

                let sent = self
                    .client
                    .bolt11_send(Bolt11SendRequest {
                        invoice: bolt11_options.bolt11.to_string(),
                        amount_msat,
                        route_parameters,
                    })
                    .await;
                let uncertain = sent
                    .as_ref()
                    .err()
                    .is_some_and(|error| send_error_is_uncertain(error, prior.as_ref()));
                if uncertain {
                    tracing::warn!(
                        "BOLT11 send {} failed after a possible dispatch; it stays marked",
                        payment_hash
                    );
                } else if new_mark {
                    if let Err(error) = self.uncertain_sends.lock().await.clear(&payment_hash) {
                        // The file keeps the mark, so a check stays cautious.
                        tracing::error!(
                            "Could not clear the mark of BOLT11 send {}: {}",
                            payment_hash,
                            error
                        );
                    }
                }
                let response = sent.map_err(Error::from)?;
                remember_outgoing_payment_context(
                    &self.outgoing_payment_correlations,
                    &response.payment_id,
                    context.clone(),
                )
                .await;
                let payment = self
                    .wait_for_payment_details(&response.payment_id, bolt11_options.timeout_secs)
                    .await?;
                remember_outgoing_payment_keys(
                    &self.outgoing_payment_correlations,
                    &payment,
                    context.clone(),
                )
                .await;

                let payment_response = Self::make_payment_response_from_payment(
                    unit,
                    outgoing_payment_lookup_id(&payment)?,
                    &payment,
                )?;
                if matches!(
                    payment_response.status,
                    MeltQuoteState::Paid | MeltQuoteState::Failed
                ) {
                    remove_outgoing_payment_context(
                        &self.outgoing_payment_correlations,
                        &context.quote_id,
                    )
                    .await;
                }
                Ok(payment_response)
            }
            OutgoingPaymentOptions::Bolt12(bolt12_options) => {
                let amount_msat = bolt12_amount_msat_for_send(&bolt12_options)?;
                let route_parameters =
                    route_parameters_from_max_fee(bolt12_options.max_fee_amount.clone())?;
                // A BOLT12 send gets a new payment ID, so no stored record
                // of an earlier attempt shares its key.
                let context = OutgoingPaymentContext {
                    quote_id: bolt12_options.quote_id.clone(),
                    unit: unit.clone(),
                    baseline: None,
                };
                begin_outgoing_attempt(
                    &self.outgoing_payment_correlations,
                    Vec::new(),
                    context.clone(),
                )
                .await;
                let response = self
                    .client
                    .bolt12_send(Bolt12SendRequest {
                        offer: bolt12_options.offer.to_string(),
                        amount_msat,
                        quantity: None,
                        payer_note: None,
                        route_parameters,
                    })
                    .await
                    .map_err(Error::from)?;
                remember_outgoing_payment_context(
                    &self.outgoing_payment_correlations,
                    &response.payment_id,
                    context.clone(),
                )
                .await;
                let payment = self
                    .wait_for_payment_details(&response.payment_id, bolt12_options.timeout_secs)
                    .await?;
                remember_outgoing_payment_keys(
                    &self.outgoing_payment_correlations,
                    &payment,
                    context.clone(),
                )
                .await;

                let payment_response = Self::make_payment_response_from_payment(
                    unit,
                    outgoing_payment_lookup_id(&payment)?,
                    &payment,
                )?;
                if matches!(
                    payment_response.status,
                    MeltQuoteState::Paid | MeltQuoteState::Failed
                ) {
                    remove_outgoing_payment_context(
                        &self.outgoing_payment_correlations,
                        &context.quote_id,
                    )
                    .await;
                }
                Ok(payment_response)
            }
            OutgoingPaymentOptions::Custom(_) | OutgoingPaymentOptions::Onchain(_) => {
                Err(payment::Error::UnsupportedPaymentOption)
            }
        }
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        let stream = self.client.subscribe_events().await.map_err(Error::from)?;
        self.wait_invoice_is_active.store(true, Ordering::SeqCst);

        let state = EventStreamState {
            client: self.client.clone(),
            cancel_token: self.wait_invoice_cancel_token.clone(),
            is_active: Arc::clone(&self.wait_invoice_is_active),
            outgoing_payment_correlations: self.outgoing_payment_correlations.clone(),
            stream: Some(stream),
            retry_count: 0,
        };

        Ok(Box::pin(futures::stream::unfold(
            state,
            |mut state| async move {
                state.is_active.store(true, Ordering::SeqCst);

                loop {
                    if let Some(event) =
                        take_ready_outgoing_event(&state.outgoing_payment_correlations).await
                    {
                        return Some((event, state));
                    }

                    if state.stream.is_none() {
                        match state.client.subscribe_events().await {
                            Ok(stream) => {
                                state.stream = Some(stream);
                                state.retry_count = 0;
                            }
                            Err(err) => {
                                tracing::warn!("Failed to subscribe to LDK Server events: {}", err);
                                if !sleep_or_cancel(&state.cancel_token, state.retry_delay()).await
                                {
                                    state.is_active.store(false, Ordering::SeqCst);
                                    return None;
                                }
                                state.retry_count = state.retry_count.saturating_add(1);
                                continue;
                            }
                        }
                    }

                    let mut stream = match state.stream.take() {
                        Some(stream) => stream,
                        None => continue,
                    };

                    tokio::select! {
                        _ = state.cancel_token.cancelled() => {
                            state.is_active.store(false, Ordering::SeqCst);
                            return None;
                        }
                        _ = state.outgoing_payment_correlations.notify.notified() => {
                            state.stream = Some(stream);
                            continue;
                        }
                        message = stream.next_message() => {
                            state.stream = Some(stream);

                            match message {
                                Some(Ok(envelope)) => match Self::event_from_envelope(
                                    envelope,
                                    &state.outgoing_payment_correlations,
                                ).await {
                                    Ok(Some(event)) => return Some((event, state)),
                                    Ok(None) => continue,
                                    Err(err) => {
                                        tracing::warn!("Could not map LDK Server event: {}", err);
                                        continue;
                                    }
                                },
                                Some(Err(err)) => {
                                    tracing::warn!("LDK Server event stream error: {}", err);
                                    state.stream = None;
                                    if !sleep_or_cancel(&state.cancel_token, state.retry_delay()).await {
                                        state.is_active.store(false, Ordering::SeqCst);
                                        return None;
                                    }
                                    state.retry_count = state.retry_count.saturating_add(1);
                                    continue;
                                }
                                None => {
                                    tracing::warn!("LDK Server event stream ended");
                                    state.stream = None;
                                    if !sleep_or_cancel(&state.cancel_token, state.retry_delay()).await {
                                        state.is_active.store(false, Ordering::SeqCst);
                                        return None;
                                    }
                                    state.retry_count = state.retry_count.saturating_add(1);
                                    continue;
                                }
                            }
                        }
                    }
                }
            },
        )))
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.wait_invoice_is_active.load(Ordering::SeqCst)
    }

    fn cancel_payment_event_stream(&self) {
        self.wait_invoice_cancel_token.cancel();
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        match payment_identifier {
            PaymentIdentifier::OfferId(offer_id) => {
                let payments = self
                    .list_matching_payments(
                        |payment| {
                            matches!(payment_direction(payment), Ok(PaymentDirection::Inbound))
                                && matches!(payment_status(payment), Ok(PaymentStatus::Succeeded))
                                && matches!(
                                    payment_kind(payment),
                                    Ok(payment_kind::Kind::Bolt12Offer(bolt12))
                                        if bolt12.offer_id == *offer_id
                                )
                        },
                        false,
                    )
                    .await?;

                Ok(payments
                    .iter()
                    .filter_map(
                        |payment| match Self::wait_payment_response_from_payment(payment) {
                            Ok(response) => response,
                            Err(err) => {
                                tracing::warn!(
                                    "Could not map LDK Server incoming payment: {}",
                                    err
                                );
                                None
                            }
                        },
                    )
                    .collect())
            }
            PaymentIdentifier::PaymentHash(hash) => {
                let hash = hex::encode(hash);
                let payments = self
                    .list_matching_payments(
                        |payment| {
                            matches!(payment_direction(payment), Ok(PaymentDirection::Inbound))
                                && matches!(payment_status(payment), Ok(PaymentStatus::Succeeded))
                                && matches!(
                                    payment_kind(payment),
                                    Ok(payment_kind::Kind::Bolt11(bolt11))
                                        if bolt11.hash.eq_ignore_ascii_case(&hash)
                                )
                        },
                        true,
                    )
                    .await?;

                Ok(payments
                    .iter()
                    .filter_map(
                        |payment| match Self::wait_payment_response_from_payment(payment) {
                            Ok(response) => response,
                            Err(err) => {
                                tracing::warn!(
                                    "Could not map LDK Server incoming payment: {}",
                                    err
                                );
                                None
                            }
                        },
                    )
                    .collect())
            }
            PaymentIdentifier::PaymentId(payment_id) => {
                let payment_id = hex::encode(payment_id);
                let payment = match self.get_payment_details(&payment_id).await? {
                    Some(payment) => payment,
                    None => return Ok(vec![]),
                };

                Ok(Self::wait_payment_response_from_payment(&payment)?
                    .into_iter()
                    .collect())
            }
            _ => Err(Error::UnsupportedPaymentIdentifierType.into()),
        }
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        match payment_identifier {
            PaymentIdentifier::PaymentId(payment_id) => {
                let payment_id = hex::encode(payment_id);
                let payment = self.get_payment_details(&payment_id).await?;

                let bolt11_hash = match &payment {
                    Some(payment) => {
                        if payment_direction(payment)? != PaymentDirection::Outbound {
                            return Err(Error::InvalidPaymentDirection.into());
                        }
                        match payment_kind(payment)? {
                            payment_kind::Kind::Bolt11(bolt11) => Some(bolt11.hash.clone()),
                            _ => None,
                        }
                    }
                    // A BOLT11 send uses its payment hash as payment ID.
                    None => Some(payment_id),
                };

                match (bolt11_hash, payment) {
                    (Some(hash), payment) => {
                        self.reported_bolt11_send(&hash, payment, payment_identifier, false)
                            .await
                    }
                    (None, Some(payment)) => Self::make_payment_response_from_payment(
                        &CurrencyUnit::Msat,
                        payment_identifier.clone(),
                        &payment,
                    ),
                    (None, None) => Err(Error::PaymentNotFound.into()),
                }
            }
            PaymentIdentifier::PaymentHash(hash) => {
                let hash = hex::encode(hash);
                let is_bolt11_send = |payment: &Payment| {
                    matches!(payment_direction(payment), Ok(PaymentDirection::Outbound))
                        && matches!(
                            payment_kind(payment),
                            Ok(payment_kind::Kind::Bolt11(bolt11))
                                if bolt11.hash.eq_ignore_ascii_case(&hash)
                        )
                };
                // LDK Node uses the invoice payment hash as the payment ID of a
                // BOLT11 send, so a direct lookup avoids a history scan.
                let payment = match self.get_payment_details(&hash).await? {
                    Some(payment) if is_bolt11_send(&payment) => Some(payment),
                    _ => select_bolt11_payment(
                        self.list_matching_payments(is_bolt11_send, false).await?,
                    ),
                };

                self.reported_bolt11_send(&hash, payment, payment_identifier, true)
                    .await
            }
            _ => Ok(unknown_payment_response(payment_identifier)),
        }
    }
}

impl LdkServerBackend {
    fn fee_reserve_for_amount(&self, unit: &CurrencyUnit, amount: u64) -> Amount<CurrencyUnit> {
        let relative_fee_reserve = (self.fee_reserve.percent_fee_reserve * amount as f32) as u64;
        let absolute_fee_reserve: u64 = self.fee_reserve.min_fee_reserve.into();
        Amount::new(relative_fee_reserve.max(absolute_fee_reserve), unit.clone())
    }
}

impl Drop for LdkServerBackend {
    fn drop(&mut self) {
        self.wait_invoice_cancel_token.cancel();
    }
}

struct EventStreamState {
    client: Arc<dyn LdkServerApi>,
    cancel_token: CancellationToken,
    is_active: Arc<AtomicBool>,
    outgoing_payment_correlations: OutgoingPaymentCorrelations,
    stream: Option<EventStream>,
    retry_count: u32,
}

impl EventStreamState {
    fn retry_delay(&self) -> Duration {
        Duration::from_secs(2_u64.saturating_pow(self.retry_count).min(10))
    }
}

async fn remember_outgoing_payment_keys(
    correlations: &OutgoingPaymentCorrelations,
    payment: &Payment,
    context: OutgoingPaymentContext,
) {
    remember_outgoing_payment_context_keys(
        correlations,
        outgoing_payment_correlation_keys(payment),
        context,
    )
    .await;
}

async fn remember_outgoing_payment_context(
    correlations: &OutgoingPaymentCorrelations,
    payment_id: &str,
    context: OutgoingPaymentContext,
) {
    remember_outgoing_payment_context_keys(
        correlations,
        vec![normalize_payment_correlation_key(payment_id)],
        context,
    )
    .await;
}

async fn remember_outgoing_payment_context_keys(
    correlations: &OutgoingPaymentCorrelations,
    keys: Vec<String>,
    context: OutgoingPaymentContext,
) {
    let mut matched = false;
    {
        let mut state = correlations.state.lock().await;
        expire_correlator_state(&mut state, Instant::now());
        if state.finalized.contains_key(&context.quote_id) {
            drop_unmatched_for_keys(&mut state, &keys);
        } else if let Some(index) = state.unmatched.iter().position(|unmatched| {
            unmatched
                .keys
                .iter()
                .any(|existing| keys.iter().any(|key| key == existing))
        }) {
            let unmatched = state
                .unmatched
                .remove(index)
                .expect("unmatched outgoing payment index exists");
            if is_stale_failure(&context, &unmatched.payment, unmatched.outcome) {
                tracing::debug!(
                    "Dropping a failure of an earlier attempt of {}",
                    unmatched.payment.payment_id
                );
                insert_outgoing_context_keys(&mut state, &keys, context);
                return;
            }
            match map_outgoing_payment_event(&context, &unmatched.payment, unmatched.outcome) {
                Ok(event) => {
                    finalize_quote(&mut state, &context.quote_id);
                    state.ready.push_back(event);
                    matched = true;
                }
                Err(err) => {
                    tracing::warn!(
                        "Could not map buffered LDK Server outgoing payment {}: {}",
                        unmatched.payment.payment_id,
                        err
                    );
                    insert_outgoing_context_keys(&mut state, &keys, context);
                }
            }
        } else {
            insert_outgoing_context_keys(&mut state, &keys, context);
        }
    }
    if matched {
        correlations.notify.notify_one();
    }
}

/// Starts a payment attempt of a melt quote.
///
/// A quote can be melted again after a failed attempt. The new attempt drops
/// the finalized mark, the keys and the buffered events of earlier attempts,
/// so that only events of this attempt reach the quote.
async fn begin_outgoing_attempt(
    correlations: &OutgoingPaymentCorrelations,
    keys: Vec<String>,
    context: OutgoingPaymentContext,
) {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    state.finalized.remove(&context.quote_id);
    remove_quote_keys(&mut state, &context.quote_id);
    drop_unmatched_for_keys(&mut state, &keys);
    insert_outgoing_context_keys(&mut state, &keys, context);
}

/// True for a failure event that belongs to an earlier attempt of the hash.
fn is_stale_failure(
    context: &OutgoingPaymentContext,
    payment: &Payment,
    outcome: OutgoingPaymentOutcome,
) -> bool {
    outcome == OutgoingPaymentOutcome::Failed
        && context
            .baseline
            .is_some_and(|baseline| payment.latest_update_timestamp <= baseline)
}

async fn remove_outgoing_payment_context(
    correlations: &OutgoingPaymentCorrelations,
    quote_id: &QuoteId,
) {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    finalize_quote(&mut state, quote_id);
}

async fn take_correlated_outgoing_event(
    correlations: &OutgoingPaymentCorrelations,
    payment: Payment,
    outcome: OutgoingPaymentOutcome,
) -> Result<Option<Event>, payment::Error> {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    let keys = outgoing_payment_correlation_keys(&payment);
    if let Some(context) = keys
        .iter()
        .find_map(|key| state.by_key.get(key).map(|pending| pending.context.clone()))
    {
        if is_stale_failure(&context, &payment, outcome) {
            tracing::debug!(
                "Dropping a failure of an earlier attempt of {} for quote {}",
                payment.payment_id,
                context.quote_id
            );
            return Ok(None);
        }
        finalize_quote(&mut state, &context.quote_id);
        return map_outgoing_payment_event(&context, &payment, outcome).map(Some);
    }

    tracing::debug!(
        "Buffering uncorrelated LDK Server outgoing payment {}: {:?}",
        payment.payment_id,
        outcome
    );
    state.unmatched.push_back(UnmatchedOutgoingPayment {
        keys,
        payment,
        outcome,
        received_at: Instant::now(),
    });
    Ok(None)
}

async fn take_ready_outgoing_event(correlations: &OutgoingPaymentCorrelations) -> Option<Event> {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    state.ready.pop_front()
}

fn insert_outgoing_context_keys(
    state: &mut OutgoingPaymentCorrelatorState,
    keys: &[String],
    context: OutgoingPaymentContext,
) {
    let pending = PendingOutgoingContext {
        context,
        inserted_at: Instant::now(),
    };
    for key in keys {
        state.by_key.insert(key.clone(), pending.clone());
    }
}

fn remove_quote_keys(state: &mut OutgoingPaymentCorrelatorState, quote_id: &QuoteId) {
    state
        .by_key
        .retain(|_, pending| &pending.context.quote_id != quote_id);
}

fn finalize_quote(state: &mut OutgoingPaymentCorrelatorState, quote_id: &QuoteId) {
    remove_quote_keys(state, quote_id);
    state.finalized.insert(quote_id.clone(), Instant::now());
}

fn drop_unmatched_for_keys(state: &mut OutgoingPaymentCorrelatorState, keys: &[String]) {
    state.unmatched.retain(|unmatched| {
        !unmatched
            .keys
            .iter()
            .any(|existing| keys.iter().any(|key| key == existing))
    });
}

fn expire_correlator_state(state: &mut OutgoingPaymentCorrelatorState, now: Instant) {
    state.by_key.retain(|_, pending| {
        now.saturating_duration_since(pending.inserted_at) < OUTGOING_CONTEXT_TTL
    });
    state.finalized.retain(|_, finalized_at| {
        now.saturating_duration_since(*finalized_at) < OUTGOING_CONTEXT_TTL
    });
    while state.unmatched.front().is_some_and(|unmatched| {
        now.saturating_duration_since(unmatched.received_at) >= UNMATCHED_OUTGOING_EVENT_TTL
    }) {
        if let Some(unmatched) = state.unmatched.pop_front() {
            tracing::warn!(
                "Dropping uncorrelated LDK Server outgoing payment {} after {:?}",
                unmatched.payment.payment_id,
                UNMATCHED_OUTGOING_EVENT_TTL
            );
        }
    }
}

fn map_outgoing_payment_event(
    context: &OutgoingPaymentContext,
    payment: &Payment,
    outcome: OutgoingPaymentOutcome,
) -> Result<Event, payment::Error> {
    match outcome {
        OutgoingPaymentOutcome::Succeeded => {
            let payment_lookup_id = outgoing_payment_lookup_id(payment)?;
            let details = LdkServerBackend::make_payment_response_from_payment(
                &context.unit,
                payment_lookup_id,
                payment,
            )?;
            tracing::debug!(
                "LDK Server outgoing payment succeeded for quote {}: {}",
                context.quote_id,
                payment.payment_id
            );
            Ok(Event::PaymentSuccessful {
                quote_id: context.quote_id.clone(),
                details,
            })
        }
        OutgoingPaymentOutcome::Failed => {
            let reason = format!("LDK Server payment {} failed", payment.payment_id);
            tracing::warn!(
                "LDK Server outgoing payment failed for quote {}: {}",
                context.quote_id,
                payment.payment_id
            );
            Ok(Event::PaymentFailed {
                quote_id: context.quote_id.clone(),
                reason,
            })
        }
    }
}

/// The lookup ID that the mint stores for an outgoing payment.
///
/// A BOLT11 melt quote is keyed by the invoice payment hash
/// (`get_payment_quote`), so every BOLT11 result reports that hash. A BOLT12
/// quote has no lookup ID, and the mint stores the LDK payment ID that
/// `make_payment` returns. A mint that requires exact lookup IDs rejects any
/// other ID.
fn outgoing_payment_lookup_id(payment: &Payment) -> Result<PaymentIdentifier, Error> {
    match payment_kind(payment)? {
        payment_kind::Kind::Bolt11(bolt11) => {
            Ok(PaymentIdentifier::PaymentHash(hex_to_array(&bolt11.hash)?))
        }
        _ => Ok(PaymentIdentifier::PaymentId(hex_to_array(
            &payment.payment_id,
        )?)),
    }
}

fn outgoing_payment_correlation_keys(payment: &Payment) -> Vec<String> {
    let mut keys = vec![normalize_payment_correlation_key(&payment.payment_id)];

    if let Some(kind) = payment.kind.as_ref().and_then(|kind| kind.kind.as_ref()) {
        match kind {
            payment_kind::Kind::Bolt11(bolt11) => {
                keys.push(normalize_payment_correlation_key(&bolt11.hash));
            }
            payment_kind::Kind::Bolt12Offer(bolt12) => {
                if let Some(hash) = &bolt12.hash {
                    keys.push(normalize_payment_correlation_key(hash));
                }
            }
            _ => {}
        }
    }

    keys.sort_unstable();
    keys.dedup();
    keys
}

fn normalize_payment_correlation_key(payment_id: &str) -> String {
    payment_id.trim().to_ascii_lowercase()
}

fn normalize_address(address: &str) -> String {
    address
        .trim()
        .strip_prefix("https://")
        .or_else(|| address.trim().strip_prefix("http://"))
        .unwrap_or_else(|| address.trim())
        .trim_end_matches('/')
        .to_string()
}

fn expiry_secs(unix_expiry: u64) -> Result<u32, payment::Error> {
    let seconds = unix_expiry
        .checked_sub(unix_time())
        .ok_or(payment::Error::InvalidExpiry)?;
    seconds
        .try_into()
        .map_err(|_| payment::Error::InvalidExpiry)
}

fn expiry_secs_or_default(unix_expiry: Option<u64>) -> Result<u32, payment::Error> {
    match unix_expiry {
        Some(unix_expiry) => expiry_secs(unix_expiry),
        None => Ok(DEFAULT_INVOICE_EXPIRY_SECS),
    }
}

fn hex_to_array(hex_value: &str) -> Result<[u8; 32], Error> {
    hex::decode(hex_value)?
        .try_into()
        .map_err(|_| Error::InvalidPaymentId)
}

fn payment_kind(payment: &Payment) -> Result<&payment_kind::Kind, Error> {
    payment
        .kind
        .as_ref()
        .and_then(|kind| kind.kind.as_ref())
        .ok_or(Error::MissingPaymentKind)
}

fn payment_direction(payment: &Payment) -> Result<PaymentDirection, Error> {
    PaymentDirection::from_i32(payment.direction)
        .ok_or(Error::UnknownPaymentDirection(payment.direction))
}

fn payment_status(payment: &Payment) -> Result<PaymentStatus, Error> {
    PaymentStatus::from_i32(payment.status).ok_or(Error::UnknownPaymentStatus(payment.status))
}

fn bolt11_amount_msat_for_quote(
    options: &cdk_common::payment::Bolt11OutgoingPaymentOptions,
) -> Result<u64, payment::Error> {
    match &options.melt_options {
        Some(MeltOptions::Amountless { amountless }) => {
            let amount_msat = u64::from(amountless.amount_msat);

            if let Some(invoice_amount) = options.bolt11.amount_milli_satoshis() {
                if invoice_amount != amount_msat {
                    return Err(payment::Error::AmountMismatch);
                }
            }

            Ok(amount_msat)
        }
        Some(MeltOptions::Mpp { mpp }) => Ok(mpp.amount.into()),
        None => options
            .bolt11
            .amount_milli_satoshis()
            .ok_or(Error::UnknownInvoiceAmount.into()),
    }
}

fn bolt11_amount_msat_for_send(
    options: &cdk_common::payment::Bolt11OutgoingPaymentOptions,
) -> Result<Option<u64>, payment::Error> {
    match &options.melt_options {
        Some(MeltOptions::Amountless { amountless }) => {
            let amount_msat = u64::from(amountless.amount_msat);

            if let Some(invoice_amount) = options.bolt11.amount_milli_satoshis() {
                if invoice_amount != amount_msat {
                    return Err(payment::Error::AmountMismatch);
                }
            }

            Ok(Some(amount_msat))
        }
        Some(MeltOptions::Mpp { mpp: _ }) => Err(payment::Error::UnsupportedPaymentOption),
        None => Ok(None),
    }
}

fn bolt12_amount_msat_for_quote(
    options: &cdk_common::payment::Bolt12OutgoingPaymentOptions,
) -> Result<u64, payment::Error> {
    match bolt12_melt_options_amount_msat(options)? {
        Some(amount_msat) => Ok(amount_msat),
        None => {
            let amount = options
                .offer
                .amount()
                .ok_or(payment::Error::AmountMismatch)?;

            match amount {
                OfferAmount::Bitcoin { amount_msats } => Ok(amount_msats),
                _ => Err(payment::Error::AmountMismatch),
            }
        }
    }
}

fn bolt12_amount_msat_for_send(
    options: &cdk_common::payment::Bolt12OutgoingPaymentOptions,
) -> Result<Option<u64>, payment::Error> {
    bolt12_melt_options_amount_msat(options)
}

fn bolt12_melt_options_amount_msat(
    options: &cdk_common::payment::Bolt12OutgoingPaymentOptions,
) -> Result<Option<u64>, payment::Error> {
    match &options.melt_options {
        Some(MeltOptions::Amountless { amountless }) => {
            let amount_msat = u64::from(amountless.amount_msat);
            validate_bolt12_amount_msat(&options.offer, amount_msat)?;
            Ok(Some(amount_msat))
        }
        Some(MeltOptions::Mpp { mpp: _ }) => Err(payment::Error::UnsupportedPaymentOption),
        None => Ok(None),
    }
}

fn validate_bolt12_amount_msat(
    offer: &lightning::offers::offer::Offer,
    amount_msat: u64,
) -> Result<(), payment::Error> {
    match offer.amount() {
        Some(OfferAmount::Bitcoin { amount_msats }) if amount_msats == amount_msat => Ok(()),
        Some(_) => Err(payment::Error::AmountMismatch),
        None => Ok(()),
    }
}

fn msat_total_spent_for_unit(
    total_msat: u64,
    unit: &CurrencyUnit,
) -> Result<Amount<CurrencyUnit>, payment::Error> {
    match unit {
        CurrencyUnit::Msat => Ok(Amount::new(total_msat, CurrencyUnit::Msat)),
        CurrencyUnit::Sat => Ok(Amount::new(
            total_msat.div_ceil(MSAT_IN_SAT),
            CurrencyUnit::Sat,
        )),
        _ => Amount::new(total_msat, CurrencyUnit::Msat)
            .convert_to(unit)
            .map_err(payment::Error::from),
    }
}

fn route_parameters_from_max_fee(
    max_fee_amount: Option<Amount<CurrencyUnit>>,
) -> Result<Option<RouteParametersConfig>, payment::Error> {
    max_fee_amount
        .map(|amount| {
            amount
                .convert_to(&CurrencyUnit::Msat)
                .map(|amount_msat| RouteParametersConfig {
                    max_total_routing_fee_msat: Some(amount_msat.value()),
                    max_total_cltv_expiry_delta: ROUTE_DEFAULT_MAX_TOTAL_CLTV_EXPIRY_DELTA,
                    max_path_count: ROUTE_DEFAULT_MAX_PATH_COUNT,
                    max_channel_saturation_power_of_half:
                        ROUTE_DEFAULT_MAX_CHANNEL_SATURATION_POWER_OF_HALF,
                })
        })
        .transpose()
        .map_err(payment::Error::from)
}

fn select_bolt11_payment(payments: Vec<Payment>) -> Option<Payment> {
    payments.into_iter().min_by_key(|payment| {
        let status_order = match payment_status(payment) {
            Ok(PaymentStatus::Succeeded) => 0_u8,
            Ok(PaymentStatus::Pending) => 1,
            Ok(PaymentStatus::Failed) | Err(_) => 2,
        };

        (
            status_order,
            std::cmp::Reverse(payment.latest_update_timestamp),
        )
    })
}

async fn sleep_or_cancel(cancel_token: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::settings_response;
    use crate::settings::AdvertisedMethod;

    #[test]
    fn every_method_is_advertised_by_default() {
        let settings = settings_response(&AdvertisedMethod::ALL);
        assert!(settings.bolt11.is_some());
        assert!(settings.bolt12.is_some());
        // This backend has never served on-chain.
        assert!(settings.onchain.is_none());
    }

    #[test]
    fn bolt12_only_leaves_bolt11_free() {
        // The shape that lets another backend take bolt11 without colliding.
        let settings = settings_response(&[AdvertisedMethod::Bolt12]);
        assert!(settings.bolt12.is_some());
        assert!(settings.bolt11.is_none());
    }

    #[test]
    fn bolt11_only_leaves_bolt12_free() {
        let settings = settings_response(&[AdvertisedMethod::Bolt11]);
        assert!(settings.bolt11.is_some());
        assert!(settings.bolt12.is_none());
    }

    #[test]
    fn advertising_nothing_yields_no_registrable_method() {
        let settings = settings_response(&[]);
        assert!(settings.bolt11.is_none());
        assert!(settings.bolt12.is_none());
    }

    #[test]
    fn advertised_settings_keep_their_values() {
        let all = settings_response(&AdvertisedMethod::ALL);
        let bolt11_only = settings_response(&[AdvertisedMethod::Bolt11]);
        assert_eq!(all.unit, bolt11_only.unit);
        assert_eq!(
            all.bolt11
                .map(|s| (s.mpp, s.amountless, s.invoice_description)),
            bolt11_only
                .bolt11
                .map(|s| (s.mpp, s.amountless, s.invoice_description))
        );
    }

    use super::*;
    use bitcoin::secp256k1::{Keypair, PublicKey, Secp256k1, SecretKey};
    use cdk_common::payment::Bolt12OutgoingPaymentOptions;
    use ldk_server_client::ldk_server_grpc::events::{
        PaymentFailed as LdkPaymentFailed, PaymentSuccessful as LdkPaymentSuccessful,
    };
    use ldk_server_client::ldk_server_grpc::types::{Bolt11, Bolt12Offer, PaymentKind};
    use lightning::offers::offer::OfferBuilder;

    fn test_bolt11_payment(status: PaymentStatus, amount_msat: Option<u64>) -> Payment {
        Payment {
            payment_id: "02".repeat(32),
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt11(Bolt11 {
                    hash: "01".repeat(32),
                    preimage: Some("03".repeat(32)),
                    secret: None,
                    counterparty_skimmed_fee_msat: None,
                })),
            }),
            amount_msat,
            fee_paid_msat: None,
            direction: PaymentDirection::Outbound as i32,
            status: status as i32,
            latest_update_timestamp: 0,
        }
    }

    fn successful_event(payment: Payment) -> LdkPaymentSuccessful {
        LdkPaymentSuccessful {
            payment_id: payment.payment_id.clone(),
            payment: Some(payment),
            payment_preimage: None,
            bolt12_invoice: None,
        }
    }

    fn failed_event(payment: Payment) -> LdkPaymentFailed {
        LdkPaymentFailed {
            payment_id: payment.payment_id.clone(),
            payment: Some(payment),
            reason: None,
        }
    }

    fn test_bolt11_payment_with_id(
        id_byte: u8,
        status: PaymentStatus,
        latest_update_timestamp: u64,
    ) -> Payment {
        Payment {
            payment_id: format!("{id_byte:02x}").repeat(32),
            latest_update_timestamp,
            ..test_bolt11_payment(status, None)
        }
    }

    fn test_offer(amount_msats: Option<u64>) -> lightning::offers::offer::Offer {
        let secp_ctx = Secp256k1::new();
        let secret_key = SecretKey::from_slice(&[42; 32]).expect("test secret key should be valid");
        let keys = Keypair::from_secret_key(&secp_ctx, &secret_key);
        let pubkey = PublicKey::from(keys);
        let builder = OfferBuilder::new(pubkey);

        match amount_msats {
            Some(amount_msats) => builder
                .amount_msats(amount_msats)
                .build()
                .expect("fixed test offer should build"),
            None => builder.build().expect("variable test offer should build"),
        }
    }

    fn bolt12_options(
        offer: lightning::offers::offer::Offer,
        melt_options: Option<MeltOptions>,
    ) -> Bolt12OutgoingPaymentOptions {
        Bolt12OutgoingPaymentOptions {
            offer,
            max_fee_amount: None,
            timeout_secs: None,
            melt_options,
            quote_id: cdk_common::QuoteId::new(),
        }
    }

    #[test]
    fn normalize_address_removes_scheme() {
        assert_eq!(
            normalize_address("https://127.0.0.1:3536/"),
            "127.0.0.1:3536"
        );
        assert_eq!(normalize_address("127.0.0.1:3536"), "127.0.0.1:3536");
    }

    async fn correlator_is_idle(correlations: &OutgoingPaymentCorrelations) -> bool {
        let state = correlations.state.lock().await;
        state.by_key.is_empty() && state.unmatched.is_empty() && state.ready.is_empty()
    }

    #[tokio::test]
    async fn successful_outgoing_event_includes_quote_and_payment_details() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        remember_outgoing_payment_context(
            &correlations,
            &"02".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Sat,
                baseline: None,
            },
        )
        .await;
        let payment = Payment {
            fee_paid_msat: Some(1),
            ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000))
        };
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(successful_event(
                payment,
            ))),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("successful event should map")
            .expect("successful event should be emitted");
        let Event::PaymentSuccessful {
            quote_id: event_quote_id,
            details,
        } = event
        else {
            panic!("expected outgoing payment success event");
        };

        assert_eq!(event_quote_id, quote_id);
        // A BOLT11 melt quote is keyed by the invoice payment hash ("01" * 32
        // in the fixture). The event must carry the same lookup ID.
        assert_eq!(
            details.payment_lookup_id,
            PaymentIdentifier::PaymentHash([1; 32])
        );
        assert_eq!(details.status, MeltQuoteState::Paid);
        assert_eq!(details.total_spent, Amount::new(2, CurrencyUnit::Sat));
        assert!(correlator_is_idle(&correlations).await);
    }

    #[tokio::test]
    async fn successful_bolt12_event_keeps_the_payment_id() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        remember_outgoing_payment_context(
            &correlations,
            &"02".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Msat,
                baseline: None,
            },
        )
        .await;
        let payment = Payment {
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt12Offer(Bolt12Offer {
                    hash: Some("01".repeat(32)),
                    preimage: Some("03".repeat(32)),
                    secret: None,
                    offer_id: "04".repeat(32),
                    payer_note: None,
                    quantity: None,
                })),
            }),
            ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000))
        };
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(successful_event(
                payment,
            ))),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("successful event should map")
            .expect("successful event should be emitted");
        let Event::PaymentSuccessful { details, .. } = event else {
            panic!("expected outgoing payment success event");
        };

        // A BOLT12 melt quote has no lookup ID. The mint stores the ID that
        // make_payment returns, which is the LDK payment ID ("02" * 32).
        assert_eq!(
            details.payment_lookup_id,
            PaymentIdentifier::PaymentId([2; 32])
        );
    }

    #[tokio::test]
    async fn failed_outgoing_event_can_correlate_by_bolt11_hash() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        remember_outgoing_payment_context(
            &correlations,
            &"01".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Msat,
                baseline: None,
            },
        )
        .await;
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentFailed(failed_event(
                test_bolt11_payment(PaymentStatus::Failed, None),
            ))),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("failed event should map")
            .expect("failed event should be emitted");
        let Event::PaymentFailed {
            quote_id: event_quote_id,
            reason,
        } = event
        else {
            panic!("expected outgoing payment failure event");
        };

        assert_eq!(event_quote_id, quote_id);
        assert!(reason.contains(&"02".repeat(32)));
        assert!(correlator_is_idle(&correlations).await);
    }

    #[tokio::test]
    async fn uncorrelated_outgoing_event_is_buffered_without_blocking() {
        let correlations = OutgoingPaymentCorrelator::new();
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(successful_event(
                test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000)),
            ))),
        };

        let started = Instant::now();
        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("uncorrelated event should map");
        assert!(event.is_none());
        assert!(started.elapsed() < Duration::from_millis(50));
        assert!(take_ready_outgoing_event(&correlations).await.is_none());
        assert_eq!(correlations.state.lock().await.unmatched.len(), 1);
    }

    #[tokio::test]
    async fn remembering_quote_replays_buffered_outgoing_success() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(successful_event(
                Payment {
                    fee_paid_msat: Some(1),
                    ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000))
                },
            ))),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("buffered event should map");
        assert!(event.is_none());

        remember_outgoing_payment_context(
            &correlations,
            &"02".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Sat,
                baseline: None,
            },
        )
        .await;

        let event = take_ready_outgoing_event(&correlations)
            .await
            .expect("buffered success should become ready after remember");
        let Event::PaymentSuccessful {
            quote_id: event_quote_id,
            details,
        } = event
        else {
            panic!("expected outgoing payment success event");
        };
        assert_eq!(event_quote_id, quote_id);
        assert_eq!(details.status, MeltQuoteState::Paid);
        assert_eq!(details.total_spent, Amount::new(2, CurrencyUnit::Sat));
        assert!(correlator_is_idle(&correlations).await);
    }

    #[test]
    fn failed_payment_response_does_not_require_amount() {
        let payment = test_bolt11_payment(PaymentStatus::Failed, None);

        let response = LdkServerBackend::make_payment_response_from_payment(
            &CurrencyUnit::Msat,
            PaymentIdentifier::PaymentId([2; 32]),
            &payment,
        )
        .expect("failed payment details should map without amount");

        assert_eq!(response.status, MeltQuoteState::Failed);
        assert_eq!(response.total_spent, Amount::new(0, CurrencyUnit::Msat));
    }

    #[test]
    fn paid_payment_response_requires_amount() {
        let payment = test_bolt11_payment(PaymentStatus::Succeeded, None);

        let err = LdkServerBackend::make_payment_response_from_payment(
            &CurrencyUnit::Msat,
            PaymentIdentifier::PaymentId([2; 32]),
            &payment,
        )
        .expect_err("paid payment details without amount should fail");

        assert!(matches!(err, payment::Error::Backend(_)));
    }

    #[test]
    fn paid_payment_response_rounds_msats_up_for_sat_unit() {
        let payment = Payment {
            fee_paid_msat: Some(1),
            ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1000))
        };

        let response = LdkServerBackend::make_payment_response_from_payment(
            &CurrencyUnit::Sat,
            PaymentIdentifier::PaymentId([2; 32]),
            &payment,
        )
        .expect("paid payment details should map");

        assert_eq!(response.status, MeltQuoteState::Paid);
        assert_eq!(response.total_spent, Amount::new(2, CurrencyUnit::Sat));
    }

    #[test]
    fn bolt12_amountless_quote_rejects_mismatched_fixed_offer() {
        let offer = test_offer(Some(10_000));
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(1_000_u64)));

        let err = bolt12_amount_msat_for_quote(&options)
            .expect_err("mismatched fixed offer amount should fail");

        assert!(matches!(err, payment::Error::AmountMismatch));
    }

    #[test]
    fn bolt12_amountless_send_rejects_mismatched_fixed_offer() {
        let offer = test_offer(Some(10_000));
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(1_000_u64)));

        let err = bolt12_amount_msat_for_send(&options)
            .expect_err("mismatched fixed offer amount should fail");

        assert!(matches!(err, payment::Error::AmountMismatch));
    }

    #[test]
    fn bolt12_amountless_accepts_matching_fixed_offer() {
        let offer = test_offer(Some(10_000));
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(10_000_u64)));

        let amount_msat =
            bolt12_amount_msat_for_quote(&options).expect("matching fixed offer amount is valid");

        assert_eq!(amount_msat, 10_000);
    }

    #[test]
    fn bolt12_amountless_accepts_variable_offer() {
        let offer = test_offer(None);
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(10_000_u64)));

        let amount_msat =
            bolt12_amount_msat_for_send(&options).expect("variable offer amount is valid");

        assert_eq!(amount_msat, Some(10_000));
    }

    #[test]
    fn bolt11_payment_selection_prefers_pending_over_failed() {
        let failed = test_bolt11_payment_with_id(1, PaymentStatus::Failed, 2);
        let pending = test_bolt11_payment_with_id(2, PaymentStatus::Pending, 1);

        let selected = select_bolt11_payment(vec![failed, pending])
            .expect("payment details should be selected");

        assert_eq!(selected.payment_id, "02".repeat(32));
        assert_eq!(
            payment_status(&selected).expect("valid status"),
            PaymentStatus::Pending
        );
    }

    #[test]
    fn bolt11_payment_selection_prefers_succeeded_over_pending() {
        let pending = test_bolt11_payment_with_id(1, PaymentStatus::Pending, 2);
        let succeeded = Payment {
            amount_msat: Some(1000),
            ..test_bolt11_payment_with_id(2, PaymentStatus::Succeeded, 1)
        };

        let selected = select_bolt11_payment(vec![pending, succeeded])
            .expect("payment details should be selected");

        assert_eq!(selected.payment_id, "02".repeat(32));
        assert_eq!(
            payment_status(&selected).expect("valid status"),
            PaymentStatus::Succeeded
        );
    }
}

/// Tests that drive the backend against an in-memory LDK Server.
#[cfg(test)]
mod fake_server_tests {
    use std::collections::VecDeque;
    use std::str::FromStr;

    use bitcoin::hashes::sha256;
    use bitcoin::secp256k1::{Secp256k1, SecretKey};
    use cdk_common::lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
    use cdk_common::payment::Bolt11OutgoingPaymentOptions;
    use cdk_common::Bolt11Invoice;
    use ldk_server_client::error::LdkServerErrorCode;
    use ldk_server_client::ldk_server_grpc::events::{
        PaymentFailed as LdkPaymentFailed, PaymentSuccessful as LdkPaymentSuccessful,
    };
    use ldk_server_client::ldk_server_grpc::node_error_messages::{DUPLICATE_PAYMENT, NOT_RUNNING};
    use ldk_server_client::ldk_server_grpc::types::{Bolt11, Bolt12Offer, PaymentKind};

    use super::*;

    /// LDK Node lists 50 payments on each page.
    const PAGE_SIZE: usize = 50;

    /// What the fake node does on the next `Bolt11Send`.
    enum FakeSend {
        /// The node dispatches the payment and stores this record.
        Stores(Box<Payment>),
        /// The call fails with this code and message. The store keeps its old record.
        Fails(LdkServerErrorCode, &'static str),
        /// The call signals `entered` and never returns.
        Hangs(Arc<tokio::sync::Notify>),
    }

    #[derive(Default)]
    struct FakeState {
        /// Newest first, as LDK Node lists them.
        payments: Vec<Payment>,
        list_calls: usize,
        sends: VecDeque<FakeSend>,
        /// A file whose content the fake keeps at each `Bolt11Send`.
        observed_file: Option<std::path::PathBuf>,
        observed_at_send: Vec<String>,
        /// The next `GetPaymentDetails` signals the first and waits for the second.
        hold_next_details: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        /// Stored after the next `ListPayments` builds its page, as a send that
        /// ends during a history scan.
        store_after_next_list: Option<Payment>,
    }

    #[derive(Default)]
    struct FakeLdkServer {
        state: std::sync::Mutex<FakeState>,
    }

    impl FakeLdkServer {
        fn with_payments(payments: Vec<Payment>) -> Arc<Self> {
            let server = Self::default();
            server.state.lock().unwrap().payments = payments;
            Arc::new(server)
        }

        fn list_calls(&self) -> usize {
            self.state.lock().unwrap().list_calls
        }

        /// An update keeps the list position of a record; a new record comes first.
        fn upsert(&self, payment: Payment) {
            let mut state = self.state.lock().unwrap();
            match state
                .payments
                .iter_mut()
                .find(|existing| existing.payment_id == payment.payment_id)
            {
                Some(existing) => *existing = payment,
                None => state.payments.insert(0, payment),
            }
        }
        fn script_send(&self, send: FakeSend) {
            self.state.lock().unwrap().sends.push_back(send);
        }
    }

    fn unsupported<T>() -> Result<T, LdkServerError> {
        Err(LdkServerError::new(
            LdkServerErrorCode::InternalError,
            "not supported by the fake LDK Server",
        ))
    }

    #[async_trait]
    impl LdkServerApi for FakeLdkServer {
        async fn get_node_info(
            &self,
            _request: GetNodeInfoRequest,
        ) -> Result<GetNodeInfoResponse, LdkServerError> {
            unsupported()
        }

        async fn bolt11_receive(
            &self,
            _request: Bolt11ReceiveRequest,
        ) -> Result<Bolt11ReceiveResponse, LdkServerError> {
            unsupported()
        }

        async fn bolt12_receive(
            &self,
            _request: Bolt12ReceiveRequest,
        ) -> Result<Bolt12ReceiveResponse, LdkServerError> {
            unsupported()
        }

        async fn bolt11_send(
            &self,
            _request: Bolt11SendRequest,
        ) -> Result<Bolt11SendResponse, LdkServerError> {
            let send = {
                let mut state = self.state.lock().unwrap();
                if let Some(path) = state.observed_file.clone() {
                    let content = std::fs::read_to_string(path).unwrap_or_default();
                    state.observed_at_send.push(content);
                }
                state.sends.pop_front()
            };
            match send {
                Some(FakeSend::Hangs(entered)) => {
                    entered.notify_one();
                    std::future::pending().await
                }
                Some(FakeSend::Stores(payment)) => {
                    let payment_id = payment.payment_id.clone();
                    self.upsert(*payment);
                    Ok(Bolt11SendResponse { payment_id })
                }
                Some(FakeSend::Fails(code, message)) => Err(LdkServerError::new(code, message)),
                None => unsupported(),
            }
        }

        async fn bolt12_send(
            &self,
            _request: Bolt12SendRequest,
        ) -> Result<Bolt12SendResponse, LdkServerError> {
            unsupported()
        }

        async fn get_payment_details(
            &self,
            request: GetPaymentDetailsRequest,
        ) -> Result<GetPaymentDetailsResponse, LdkServerError> {
            let hold = self.state.lock().unwrap().hold_next_details.take();
            if let Some((entered, release)) = hold {
                entered.notify_one();
                release.notified().await;
            }
            let state = self.state.lock().unwrap();
            let payment = state
                .payments
                .iter()
                .find(|payment| payment.payment_id == request.payment_id)
                .cloned();
            Ok(GetPaymentDetailsResponse { payment })
        }

        async fn list_payments(
            &self,
            request: ListPaymentsRequest,
        ) -> Result<ListPaymentsResponse, LdkServerError> {
            let mut state = self.state.lock().unwrap();
            state.list_calls += 1;
            let start: usize = request
                .page_token
                .map(|token| token.parse().expect("fake page token"))
                .unwrap_or(0);
            let end = (start + PAGE_SIZE).min(state.payments.len());
            let response = ListPaymentsResponse {
                payments: state.payments[start..end].to_vec(),
                next_page_token: (end < state.payments.len()).then(|| end.to_string()),
            };
            if let Some(payment) = state.store_after_next_list.take() {
                state.payments.insert(0, payment);
            }
            Ok(response)
        }

        async fn subscribe_events(&self) -> Result<EventStream, LdkServerError> {
            unsupported()
        }
    }

    /// Opens the state file, and creates it first for a new test.
    fn opened_state(path: &std::path::Path) -> UncertainSends {
        if !path.exists() {
            crate::uncertain_sends::init_state_file(path, unix_time()).unwrap();
        }
        UncertainSends::open(path.to_path_buf()).unwrap()
    }

    fn backend(server: Arc<FakeLdkServer>, max_payment_scan_pages: u16) -> LdkServerBackend {
        backend_with_sends(server, max_payment_scan_pages, UncertainSends::in_memory())
    }

    fn backend_with_sends(
        server: Arc<FakeLdkServer>,
        max_payment_scan_pages: u16,
        uncertain_sends: UncertainSends,
    ) -> LdkServerBackend {
        LdkServerBackend::with_api(
            server,
            FeeReserve {
                min_fee_reserve: Amount::from(2_u64),
                percent_fee_reserve: 0.01,
            },
            max_payment_scan_pages,
            AdvertisedMethod::ALL.to_vec(),
            uncertain_sends,
        )
    }

    fn hash_hex(index: u32) -> String {
        format!("{index:064x}")
    }

    fn bolt11_payment(
        payment_id: String,
        hash: String,
        direction: PaymentDirection,
        status: PaymentStatus,
        latest_update_timestamp: u64,
    ) -> Payment {
        Payment {
            payment_id,
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt11(Bolt11 {
                    hash,
                    preimage: (status == PaymentStatus::Succeeded).then(|| "03".repeat(32)),
                    secret: None,
                    counterparty_skimmed_fee_msat: None,
                })),
            }),
            amount_msat: Some(1_000_000),
            fee_paid_msat: Some(0),
            direction: direction as i32,
            status: status as i32,
            latest_update_timestamp,
        }
    }

    /// Received BOLT11 payments that match no lookup. Their payment IDs are not
    /// their hashes, as for LDK Node receives.
    fn other_receives(count: u32) -> Vec<Payment> {
        (1..=count)
            .map(|index| {
                bolt11_payment(
                    hash_hex(1_000_000 + index),
                    hash_hex(index),
                    PaymentDirection::Inbound,
                    PaymentStatus::Succeeded,
                    10,
                )
            })
            .collect()
    }

    fn bolt12_receive(offer_id: &str, index: u32) -> Payment {
        Payment {
            payment_id: hash_hex(2_000_000 + index),
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt12Offer(Bolt12Offer {
                    hash: Some(hash_hex(3_000_000 + index)),
                    preimage: Some("03".repeat(32)),
                    secret: None,
                    offer_id: offer_id.to_string(),
                    payer_note: None,
                    quantity: None,
                })),
            }),
            amount_msat: Some(1_000_000),
            fee_paid_msat: None,
            direction: PaymentDirection::Inbound as i32,
            status: PaymentStatus::Succeeded as i32,
            latest_update_timestamp: 10,
        }
    }

    fn hash_bytes(hash: &str) -> [u8; 32] {
        hex::decode(hash).unwrap().try_into().unwrap()
    }

    // ------------------------------------------------ incoming status scans

    #[tokio::test]
    async fn an_incoming_hash_check_stops_at_the_newest_match() {
        let target = hash_hex(4_000_000);
        let mut history = vec![bolt11_payment(
            hash_hex(5_000_000),
            target.clone(),
            PaymentDirection::Inbound,
            PaymentStatus::Succeeded,
            20,
        )];
        // More than 32 pages of older payments.
        history.extend(other_receives(1_700));
        let server = FakeLdkServer::with_payments(history);

        let found = backend(server.clone(), 32)
            .check_incoming_payment_status(&PaymentIdentifier::PaymentHash(hash_bytes(&target)))
            .await
            .expect("a match on the first page must not hit the scan cap");

        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].payment_identifier,
            PaymentIdentifier::PaymentHash(hash_bytes(&target))
        );
        assert_eq!(server.list_calls(), 1);
    }

    #[tokio::test]
    async fn an_incoming_offer_check_keeps_the_matches_found_before_the_cap() {
        let mut history = vec![bolt12_receive("offer-a", 1)];
        history.extend(other_receives(1_700));
        let server = FakeLdkServer::with_payments(history);

        let found = backend(server, 32)
            .check_incoming_payment_status(&PaymentIdentifier::OfferId("offer-a".to_string()))
            .await
            .expect("a found offer payment must not be lost at the scan cap");

        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].payment_identifier,
            PaymentIdentifier::OfferId("offer-a".to_string())
        );
    }

    #[tokio::test]
    async fn an_incoming_check_without_a_match_fails_at_the_cap() {
        let server = FakeLdkServer::with_payments(other_receives(1_700));

        let error = backend(server, 32)
            .check_incoming_payment_status(&PaymentIdentifier::PaymentHash([0xee; 32]))
            .await
            .expect_err("a check that saw no match must not report an empty result");

        assert!(error.to_string().contains("scan limit"), "{error}");
    }

    #[tokio::test]
    async fn the_default_cap_reaches_a_receive_behind_three_thousand_payments() {
        let target = hash_hex(4_000_001);
        let mut history = other_receives(3_000);
        history.push(bolt11_payment(
            hash_hex(5_000_001),
            target.clone(),
            PaymentDirection::Inbound,
            PaymentStatus::Succeeded,
            5,
        ));
        let server = FakeLdkServer::with_payments(history);

        let found = backend(server, DEFAULT_MAX_PAYMENT_SCAN_PAGES)
            .check_incoming_payment_status(&PaymentIdentifier::PaymentHash(hash_bytes(&target)))
            .await
            .expect("the default cap must cover a long payment history");

        assert_eq!(found.len(), 1);
    }
    // ------------------------------------------------ outgoing BOLT11 sends

    fn invoice(hash: &str) -> Bolt11Invoice {
        let secp = Secp256k1::new();
        let key = SecretKey::from_slice(&[41; 32]).unwrap();
        InvoiceBuilder::new(Currency::Regtest)
            .description("fake".to_string())
            .payment_hash(sha256::Hash::from_byte_array(hash_bytes(hash)))
            .payment_secret(PaymentSecret([42; 32]))
            .current_timestamp()
            .min_final_cltv_expiry_delta(144)
            .amount_milli_satoshis(1_000_000)
            .build_signed(|message| secp.sign_ecdsa_recoverable(message, &key))
            .unwrap()
    }

    fn send_options(hash: &str, quote_id: &QuoteId) -> OutgoingPaymentOptions {
        OutgoingPaymentOptions::Bolt11(Box::new(Bolt11OutgoingPaymentOptions {
            bolt11: Bolt11Invoice::from_str(&invoice(hash).to_string()).unwrap(),
            max_fee_amount: None,
            timeout_secs: Some(1),
            melt_options: None,
            quote_id: quote_id.clone(),
        }))
    }

    /// The record of a BOLT11 send. LDK Node uses the hash as its payment ID.
    fn send_record(hash: &str, status: PaymentStatus, latest_update_timestamp: u64) -> Payment {
        bolt11_payment(
            hash.to_string(),
            hash.to_string(),
            PaymentDirection::Outbound,
            status,
            latest_update_timestamp,
        )
    }

    async fn statuses(backend: &LdkServerBackend, hash: &str) -> (MeltQuoteState, MeltQuoteState) {
        let by_hash = backend
            .check_outgoing_payment(&PaymentIdentifier::PaymentHash(hash_bytes(hash)))
            .await
            .expect("check by hash");
        let by_id = backend
            .check_outgoing_payment(&PaymentIdentifier::PaymentId(hash_bytes(hash)))
            .await
            .expect("check by payment ID");
        (by_hash.status, by_id.status)
    }

    /// Attempt 1 failed. Attempt 2 dispatches, but the call fails, for example
    /// because the node cannot store its Pending record.
    async fn uncertain_second_attempt(
        code: LdkServerErrorCode,
        message: &'static str,
    ) -> (Arc<FakeLdkServer>, LdkServerBackend, String) {
        let hash = hash_hex(6_000_000);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);
        let backend = backend(server.clone(), 32);
        server.script_send(FakeSend::Fails(code, message));
        backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
            .await
            .expect_err("the fake send fails");
        (server, backend, hash)
    }

    #[tokio::test]
    async fn a_stale_failed_record_is_unknown_after_a_store_error() {
        let (_, backend, hash) = uncertain_second_attempt(
            LdkServerErrorCode::InternalServerError,
            "Failed to persist data.",
        )
        .await;
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    #[tokio::test]
    async fn a_stale_failed_record_is_unknown_after_a_transport_error() {
        let (_, backend, hash) = uncertain_second_attempt(
            LdkServerErrorCode::InternalError,
            "gRPC request failed: error sending request",
        )
        .await;
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    #[tokio::test]
    async fn a_newer_failed_record_ends_the_uncertainty() {
        let (server, backend, hash) = uncertain_second_attempt(
            LdkServerErrorCode::InternalServerError,
            "Failed to persist data.",
        )
        .await;
        server.upsert(send_record(&hash, PaymentStatus::Failed, 101));
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
    }

    #[tokio::test]
    async fn a_succeeded_record_ends_the_uncertainty() {
        let (server, backend, hash) = uncertain_second_attempt(
            LdkServerErrorCode::InternalServerError,
            "Failed to persist data.",
        )
        .await;
        server.upsert(send_record(&hash, PaymentStatus::Succeeded, 100));
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Paid, MeltQuoteState::Paid)
        );
    }

    #[tokio::test]
    async fn a_lightning_error_leaves_the_failed_record_authoritative() {
        let (_, backend, hash) = uncertain_second_attempt(
            LdkServerErrorCode::LightningError,
            "Failed to send the given payment.",
        )
        .await;
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
    }

    #[tokio::test]
    async fn an_uncertain_send_without_a_record_is_unknown() {
        let hash = hash_hex(6_000_004);
        let server = FakeLdkServer::with_payments(vec![]);
        let backend = backend_with_sends(server, 32, UncertainSends::in_memory_since(0));
        // A hash that the node never stored and that has no mark was never sent.
        let never_sent = backend
            .check_outgoing_payment(&PaymentIdentifier::PaymentHash(hash_bytes(&hash)))
            .await
            .expect("a hash that was never sent has a definite result");
        assert_eq!(never_sent.status, MeltQuoteState::Failed);
        assert_eq!(never_sent.total_spent, Amount::new(0, CurrencyUnit::Msat));
        backend
            .uncertain_sends
            .lock()
            .await
            .mark(&hash, None, unix_time())
            .unwrap();
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    #[tokio::test]
    async fn a_dispatched_second_attempt_reports_its_new_record() {
        let hash = hash_hex(6_000_006);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);
        let backend = backend(server.clone(), 32);
        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            101,
        ))));

        let started = backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
            .await
            .expect("attempt 2 starts");

        assert_eq!(started.status, MeltQuoteState::Pending);
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Pending, MeltQuoteState::Pending)
        );
    }
    // ------------------------------------------------ melt events

    fn failed_envelope(payment: Payment) -> EventEnvelope {
        EventEnvelope {
            event: Some(event_envelope::Event::PaymentFailed(LdkPaymentFailed {
                payment_id: payment.payment_id.clone(),
                payment: Some(payment),
                reason: None,
            })),
        }
    }

    fn successful_envelope(payment: Payment) -> EventEnvelope {
        EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(
                LdkPaymentSuccessful {
                    payment_id: payment.payment_id.clone(),
                    payment: Some(payment),
                    payment_preimage: None,
                    bolt12_invoice: None,
                },
            )),
        }
    }

    #[tokio::test]
    async fn a_quote_melted_again_after_a_failure_gets_its_success_event() {
        let hash = hash_hex(7_000_000);
        let quote_id = QuoteId::new();
        let server = FakeLdkServer::with_payments(vec![]);
        let backend = backend(server.clone(), 32);
        let correlations = backend.outgoing_payment_correlations.clone();

        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            100,
        ))));
        let first = backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &quote_id))
            .await
            .expect("attempt 1 starts");
        assert_eq!(first.status, MeltQuoteState::Pending);
        let failed = send_record(&hash, PaymentStatus::Failed, 101);
        server.upsert(failed.clone());
        let event = LdkServerBackend::event_from_envelope(failed_envelope(failed), &correlations)
            .await
            .unwrap();
        assert!(matches!(event, Some(Event::PaymentFailed { .. })));

        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            102,
        ))));
        let second = backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &quote_id))
            .await
            .expect("attempt 2 starts");
        assert_eq!(second.status, MeltQuoteState::Pending);
        let succeeded = send_record(&hash, PaymentStatus::Succeeded, 103);
        server.upsert(succeeded.clone());
        let event =
            LdkServerBackend::event_from_envelope(successful_envelope(succeeded), &correlations)
                .await
                .unwrap();

        match event {
            Some(Event::PaymentSuccessful {
                quote_id: event_quote,
                details,
            }) => {
                assert_eq!(event_quote, quote_id);
                assert_eq!(
                    details.payment_lookup_id,
                    PaymentIdentifier::PaymentHash(hash_bytes(&hash))
                );
            }
            other => panic!("attempt 2 must emit its success event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_late_failure_of_an_earlier_attempt_is_not_reported() {
        let hash = hash_hex(7_000_001);
        let quote_id = QuoteId::new();
        let attempt_one_failed = send_record(&hash, PaymentStatus::Failed, 101);
        let server = FakeLdkServer::with_payments(vec![attempt_one_failed.clone()]);
        let backend = backend(server.clone(), 32);
        let correlations = backend.outgoing_payment_correlations.clone();

        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            102,
        ))));
        backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &quote_id))
            .await
            .expect("attempt 2 starts");

        // The failure event of attempt 1 arrives after attempt 2 starts.
        let late = LdkServerBackend::event_from_envelope(
            failed_envelope(attempt_one_failed),
            &correlations,
        )
        .await
        .unwrap();
        assert!(
            late.is_none(),
            "a failure of attempt 1 must not fail attempt 2"
        );

        let attempt_two_failed = send_record(&hash, PaymentStatus::Failed, 103);
        let current = LdkServerBackend::event_from_envelope(
            failed_envelope(attempt_two_failed),
            &correlations,
        )
        .await
        .unwrap();
        assert!(matches!(
            current,
            Some(Event::PaymentFailed { quote_id: event_quote, .. }) if event_quote == quote_id
        ));
    }

    #[tokio::test]
    async fn a_failed_record_from_before_a_restart_is_reported() {
        // No uncertain send is recorded, so the stored record is authoritative.
        let hash = hash_hex(6_000_001);
        let server = FakeLdkServer::with_payments(vec![send_record(
            &hash,
            PaymentStatus::Failed,
            unix_time() - 3_600,
        )]);
        assert_eq!(
            statuses(&backend(server, 32), &hash).await,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
    }

    #[tokio::test]
    async fn a_node_that_is_not_running_dispatches_nothing() {
        let (_, backend, hash) =
            uncertain_second_attempt(LdkServerErrorCode::InternalServerError, NOT_RUNNING).await;
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
    }

    #[tokio::test]
    async fn a_connection_failure_dispatches_nothing() {
        let (_, backend, hash) = uncertain_second_attempt(
            LdkServerErrorCode::InternalError,
            "gRPC connection failed: error sending request for url (https://127.0.0.1:3536/)",
        )
        .await;
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
    }

    #[tokio::test]
    async fn a_duplicate_payment_behind_a_failed_record_is_uncertain() {
        // The node refuses the send because its channel manager still tracks it.
        let (_, backend, hash) =
            uncertain_second_attempt(LdkServerErrorCode::LightningError, DUPLICATE_PAYMENT).await;
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    #[tokio::test]
    async fn a_duplicate_payment_behind_a_pending_record_reports_the_record() {
        let hash = hash_hex(6_000_007);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Pending, 100)]);
        let backend = backend(server.clone(), 32);
        server.script_send(FakeSend::Fails(
            LdkServerErrorCode::LightningError,
            DUPLICATE_PAYMENT,
        ));
        backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
            .await
            .expect_err("the fake send fails");
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Pending, MeltQuoteState::Pending)
        );
    }

    #[tokio::test]
    async fn a_mark_survives_a_processor_restart() {
        let directory =
            std::env::temp_dir().join(format!("ldk-backend-state-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let state = directory.join("state");
        let hash = hash_hex(6_000_008);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);

        let first = backend_with_sends(server.clone(), 32, opened_state(&state));
        server.script_send(FakeSend::Fails(
            LdkServerErrorCode::InternalServerError,
            "Failed to persist data.",
        ));
        first
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
            .await
            .expect_err("the fake send fails");
        drop(first);

        let restarted = backend_with_sends(server.clone(), 32, opened_state(&state));
        let after_restart = statuses(&restarted, &hash).await;
        server.upsert(send_record(&hash, PaymentStatus::Failed, 101));
        let after_failure = statuses(&restarted, &hash).await;
        let again = backend_with_sends(server, 32, opened_state(&state));
        let after_second_restart = statuses(&again, &hash).await;
        std::fs::remove_dir_all(&directory).unwrap();

        assert_eq!(
            after_restart,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
        assert_eq!(
            after_failure,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
        assert_eq!(
            after_second_restart,
            (MeltQuoteState::Failed, MeltQuoteState::Failed)
        );
    }

    struct StateDirectory(std::path::PathBuf);

    impl StateDirectory {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("ldk-backend-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn file(&self) -> std::path::PathBuf {
            self.0.join("state")
        }

        fn content(&self) -> String {
            std::fs::read_to_string(self.file()).unwrap()
        }
    }

    impl Drop for StateDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn a_send_is_marked_before_dispatch_and_cleared_after_it() {
        let directory = StateDirectory::new("premark");
        let hash = hash_hex(6_000_010);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);
        server.state.lock().unwrap().observed_file = Some(directory.file());
        let backend = backend_with_sends(server.clone(), 32, opened_state(&directory.file()));
        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            101,
        ))));

        backend
            .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
            .await
            .expect("the send starts");

        let at_send = server.state.lock().unwrap().observed_at_send.clone();
        assert_eq!(at_send.len(), 1);
        assert!(
            at_send[0].contains(&format!("{hash} 100 ")),
            "{}",
            at_send[0]
        );
        assert!(
            !directory.content().contains(&hash),
            "{}",
            directory.content()
        );
    }

    #[tokio::test]
    async fn a_send_cancelled_during_dispatch_keeps_its_mark() {
        // The mint can drop the call while the node dispatches.
        let directory = StateDirectory::new("cancel");
        let hash = hash_hex(6_000_011);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);
        let backend = Arc::new(backend_with_sends(
            server.clone(),
            32,
            opened_state(&directory.file()),
        ));
        let entered = Arc::new(tokio::sync::Notify::new());
        server.script_send(FakeSend::Hangs(entered.clone()));

        let sending = {
            let backend = backend.clone();
            let hash = hash.clone();
            tokio::spawn(async move {
                backend
                    .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(10), entered.notified())
            .await
            .expect("the send reaches the node");
        sending.abort();
        assert!(sending.await.unwrap_err().is_cancelled());

        assert!(
            directory.content().contains(&hash),
            "{}",
            directory.content()
        );
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    #[tokio::test]
    async fn an_earlier_mark_survives_a_new_attempt_that_dispatches_nothing() {
        let directory = StateDirectory::new("earlier");
        let hash = hash_hex(6_000_012);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);
        let backend = backend_with_sends(server.clone(), 32, opened_state(&directory.file()));
        server.script_send(FakeSend::Fails(
            LdkServerErrorCode::InternalServerError,
            "Failed to persist data.",
        ));
        server.script_send(FakeSend::Fails(
            LdkServerErrorCode::InternalServerError,
            NOT_RUNNING,
        ));
        for _ in 0..2 {
            backend
                .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
                .await
                .expect_err("the fake send fails");
        }

        assert!(
            directory.content().contains(&hash),
            "{}",
            directory.content()
        );
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    fn panic_on_state_write_failure(error: String) -> ! {
        panic!("state write failure: {error}")
    }

    #[tokio::test]
    async fn a_failed_mark_write_stops_the_processor_before_the_send() {
        let directory = StateDirectory::new("write-failure");
        let hash = hash_hex(6_000_013);
        let server =
            FakeLdkServer::with_payments(vec![send_record(&hash, PaymentStatus::Failed, 100)]);
        let mut backend = backend_with_sends(server.clone(), 32, opened_state(&directory.file()));
        backend.on_state_write_failure = panic_on_state_write_failure;
        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            101,
        ))));
        // The state file cannot be written any more.
        std::fs::remove_dir_all(&directory.0).unwrap();

        let backend = Arc::new(backend);
        let sending = tokio::spawn({
            let backend = backend.clone();
            async move {
                backend
                    .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
                    .await
            }
        });

        assert!(sending.await.unwrap_err().is_panic());
        assert_eq!(
            server.state.lock().unwrap().sends.len(),
            1,
            "the node must not get the send"
        );
    }

    #[tokio::test]
    async fn a_hash_whose_send_has_not_reached_the_node_is_unknown() {
        let hash = hash_hex(6_000_014);
        let server = FakeLdkServer::with_payments(vec![]);
        let backend = Arc::new(backend(server.clone(), 32));
        let (entered, release) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        server.state.lock().unwrap().hold_next_details = Some((entered.clone(), release.clone()));
        server.script_send(FakeSend::Stores(Box::new(send_record(
            &hash,
            PaymentStatus::Pending,
            101,
        ))));

        let sending = tokio::spawn({
            let backend = backend.clone();
            let hash = hash.clone();
            async move {
                backend
                    .make_payment(&CurrencyUnit::Msat, send_options(&hash, &QuoteId::new()))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), entered.notified())
            .await
            .expect("make_payment reads the record before the send");

        // The node has no record and no mark exists yet, but the send runs.
        let during = backend
            .check_outgoing_payment(&PaymentIdentifier::PaymentHash(hash_bytes(&hash)))
            .await
            .expect("the check answers");
        release.notify_one();
        let started = sending.await.unwrap().expect("the send starts");

        assert_eq!(during.status, MeltQuoteState::Unknown);
        assert_eq!(started.status, MeltQuoteState::Pending);
    }

    #[tokio::test]
    async fn a_send_that_ends_during_the_history_scan_reports_its_record() {
        let hash = hash_hex(6_000_015);
        let server = FakeLdkServer::with_payments(other_receives(3));
        server.state.lock().unwrap().store_after_next_list =
            Some(send_record(&hash, PaymentStatus::Succeeded, 200));

        let status = backend_with_sends(server, 32, UncertainSends::in_memory_since(0))
            .check_outgoing_payment(&PaymentIdentifier::PaymentHash(hash_bytes(&hash)))
            .await
            .expect("the check answers");

        assert_eq!(status.status, MeltQuoteState::Paid);
    }

    #[tokio::test]
    async fn an_expired_mark_without_a_record_is_unknown() {
        let hash = hash_hex(6_000_016);
        let server = FakeLdkServer::with_payments(vec![]);
        let backend = backend(server, 32);
        backend
            .uncertain_sends
            .lock()
            .await
            .mark(&hash, None, unix_time() - 100 * IN_FLIGHT_BOUND_SECS)
            .unwrap();
        assert_eq!(
            statuses(&backend, &hash).await,
            (MeltQuoteState::Unknown, MeltQuoteState::Unknown)
        );
    }

    #[tokio::test]
    async fn a_missing_record_is_unknown_while_the_state_file_is_young() {
        let hash = hash_hex(6_000_017);
        let young = backend_with_sends(
            FakeLdkServer::with_payments(vec![]),
            32,
            UncertainSends::in_memory_since(unix_time() - IN_FLIGHT_BOUND_SECS + 60),
        );
        let old = backend_with_sends(
            FakeLdkServer::with_payments(vec![]),
            32,
            UncertainSends::in_memory_since(unix_time() - IN_FLIGHT_BOUND_SECS - 60),
        );
        for (backend, expected) in [
            (young, MeltQuoteState::Unknown),
            (old, MeltQuoteState::Failed),
        ] {
            let status = backend
                .check_outgoing_payment(&PaymentIdentifier::PaymentHash(hash_bytes(&hash)))
                .await
                .unwrap()
                .status;
            assert_eq!(status, expected);
        }
    }
}
