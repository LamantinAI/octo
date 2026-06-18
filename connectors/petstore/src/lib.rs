//! Petstore binary connector — testbed for binary-connector patterns.
//!
//! Wraps a thin slice of `https://petstore3.swagger.io/api/v3` behind the
//! CQRS-style `petstore.cmd.*` / `petstore.event.*` envelopes. Not a research
//! subject — Petstore is a stable public API used to exercise:
//!
//! - request/response correlation through the bus
//!   ([`octo_core::EventBus::publish_and_await_response`]),
//! - per-target subscription (`Filter::by_target("petstore")`),
//! - `petstore.event.error` as the common error-kind, correlated to the failing
//!   command,
//! - typed payload downcast (`envelope.payload_as::<Pet>()`).
//!
//! Future Telegram / SMTP / MQTT connectors should follow the same shape.
//!
//! ## Endpoints covered
//!
//! | cmd_kind                          | HTTP                     | event_kind               |
//! |-----------------------------------|--------------------------|--------------------------|
//! | `petstore.cmd.add_pet`            | `POST /pet`              | `petstore.event.pet_added`     |
//! | `petstore.cmd.fetch_pet`          | `GET /pet/{id}`          | `petstore.event.pet_fetched`   |
//! | `petstore.cmd.find_pets_by_status`| `GET /pet/findByStatus`  | `petstore.event.pets_found`    |
//! | `petstore.cmd.delete_pet`         | `DELETE /pet/{id}`       | `petstore.event.pet_deleted`   |
//! | `petstore.cmd.update_pet`         | `PUT /pet`               | `petstore.event.pet_updated`   |
//!
//! Any HTTP failure or decode error is emitted as `petstore.event.error`
//! carrying [`ApiError`], correlated to the originating command.
//!
//! `POST /pet/{id}/uploadImage` (multipart) is intentionally out of scope —
//! binary artifacts on the bus are a separate design question.

use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, PayloadRegistry, SubscribeOptions, TrailAction, TrailActor, TrailEntry,
};
use serde::{Deserialize, Serialize};

// ─── Models ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PetStatus {
    Available,
    Pending,
    Sold,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Category {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pet {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<Category>,
    #[serde(default, rename = "photoUrls")]
    pub photo_urls: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PetStatus>,
}

/// Payload of `petstore.cmd.fetch_pet` and `petstore.cmd.delete_pet`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PetIdRequest {
    pub id: i64,
}

/// Payload of `petstore.cmd.find_pets_by_status`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FindByStatusRequest {
    pub status: PetStatus,
}

/// Payload of `petstore.event.error`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiError {
    /// HTTP status if the failure happened in transport. `None` for purely
    /// local errors (payload decode mismatch, malformed request, ...).
    pub http_status: Option<u16>,
    pub message: String,
}

// ─── Kinds ────────────────────────────────────────────────────────────────

pub mod kinds {
    //! Stable CQRS kind constants for the petstore connector. Use these
    //! rather than ad-hoc string literals when building envelopes.

    pub const CMD_ADD_PET: &str = "petstore.cmd.add_pet";
    pub const CMD_FETCH_PET: &str = "petstore.cmd.fetch_pet";
    pub const CMD_FIND_BY_STATUS: &str = "petstore.cmd.find_pets_by_status";
    pub const CMD_DELETE_PET: &str = "petstore.cmd.delete_pet";
    pub const CMD_UPDATE_PET: &str = "petstore.cmd.update_pet";

    pub const EVT_PET_ADDED: &str = "petstore.event.pet_added";
    pub const EVT_PET_FETCHED: &str = "petstore.event.pet_fetched";
    pub const EVT_PETS_FOUND: &str = "petstore.event.pets_found";
    pub const EVT_PET_DELETED: &str = "petstore.event.pet_deleted";
    pub const EVT_PET_UPDATED: &str = "petstore.event.pet_updated";
    pub const EVT_ERROR: &str = "petstore.event.error";
}

// ─── PayloadRegistry helper ───────────────────────────────────────────────

/// Register the connector's CQRS kinds against their payload types in the
/// shared [`PayloadRegistry`]. Builder-style: consumes the registry, returns
/// the extended one. Call once during runtime setup, before the bus publishes
/// any envelopes, so that strict mode can catch type mismatches.
pub fn register_payloads(registry: PayloadRegistry) -> PayloadRegistry {
    registry
        // Commands.
        .register_type::<Pet>(EventKind::from_static(kinds::CMD_ADD_PET))
        .register_type::<PetIdRequest>(EventKind::from_static(kinds::CMD_FETCH_PET))
        .register_type::<FindByStatusRequest>(EventKind::from_static(
            kinds::CMD_FIND_BY_STATUS,
        ))
        .register_type::<PetIdRequest>(EventKind::from_static(kinds::CMD_DELETE_PET))
        .register_type::<Pet>(EventKind::from_static(kinds::CMD_UPDATE_PET))
        // Events.
        .register_type::<Pet>(EventKind::from_static(kinds::EVT_PET_ADDED))
        .register_type::<Pet>(EventKind::from_static(kinds::EVT_PET_FETCHED))
        .register_type::<Vec<Pet>>(EventKind::from_static(kinds::EVT_PETS_FOUND))
        .register_type::<PetIdRequest>(EventKind::from_static(kinds::EVT_PET_DELETED))
        .register_type::<Pet>(EventKind::from_static(kinds::EVT_PET_UPDATED))
        .register_type::<ApiError>(EventKind::from_static(kinds::EVT_ERROR))
}

// ─── Connector ────────────────────────────────────────────────────────────

const DEFAULT_BASE_URL: &str = "https://petstore3.swagger.io/api/v3";

/// Petstore connector. Subscribes to envelopes targeted at `self.id` and
/// translates `petstore.cmd.*` commands into HTTP calls against
/// [`base_url`](PetstoreConnector::base_url), emitting `petstore.event.*`
/// responses correlated to the originating command.
pub struct PetstoreConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    base_url: String,
    client: reqwest::Client,
}

impl PetstoreConnector {
    pub fn builder(id: impl Into<String>) -> PetstoreConnectorBuilder {
        PetstoreConnectorBuilder {
            id: ConnectorId::new(id),
            base_url: DEFAULT_BASE_URL.to_string(),
            client: None,
        }
    }
}

pub struct PetstoreConnectorBuilder {
    id: ConnectorId,
    base_url: String,
    client: Option<reqwest::Client>,
}

impl PetstoreConnectorBuilder {
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    pub fn build(self) -> Arc<PetstoreConnector> {
        let capabilities = ConnectorCapabilities::output_only()
            .with_accept_kinds([
                EventKind::from_static(kinds::CMD_ADD_PET),
                EventKind::from_static(kinds::CMD_FETCH_PET),
                EventKind::from_static(kinds::CMD_FIND_BY_STATUS),
                EventKind::from_static(kinds::CMD_DELETE_PET),
                EventKind::from_static(kinds::CMD_UPDATE_PET),
            ])
            .with_emit_kinds([
                EventKind::from_static(kinds::EVT_PET_ADDED),
                EventKind::from_static(kinds::EVT_PET_FETCHED),
                EventKind::from_static(kinds::EVT_PETS_FOUND),
                EventKind::from_static(kinds::EVT_PET_DELETED),
                EventKind::from_static(kinds::EVT_PET_UPDATED),
                EventKind::from_static(kinds::EVT_ERROR),
            ]);
        Arc::new(PetstoreConnector {
            id: self.id,
            capabilities,
            base_url: self.base_url,
            client: self.client.unwrap_or_default(),
        })
    }
}

#[async_trait]
impl Connector for PetstoreConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut sub = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;

        loop {
            tokio::select! {
                next = sub.next() => match next {
                    Some(envelope) => {
                        self.clone().handle(envelope, &ctx).await;
                    }
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

impl PetstoreConnector {
    async fn handle(self: Arc<Self>, envelope: Arc<Envelope>, ctx: &ConnectorContext) {
        let cmd_id = envelope.id;
        let result = match envelope.kind.as_str() {
            kinds::CMD_ADD_PET => self.handle_add_pet(&envelope).await,
            kinds::CMD_FETCH_PET => self.handle_fetch_pet(&envelope).await,
            kinds::CMD_FIND_BY_STATUS => self.handle_find_by_status(&envelope).await,
            kinds::CMD_DELETE_PET => self.handle_delete_pet(&envelope).await,
            kinds::CMD_UPDATE_PET => self.handle_update_pet(&envelope).await,
            other => Err(ApiError {
                http_status: None,
                message: format!("unsupported command kind: {other}"),
            }),
        };

        let emission = match result {
            Ok(reply) => reply.with_correlation(cmd_id),
            Err(err) => Envelope::new(
                self.id.clone(),
                EventKind::from_static(kinds::EVT_ERROR),
                err,
            )
            .with_correlation(cmd_id),
        };
        let emission_kind = emission.kind.clone();
        let emission = emission.with_trail(TrailEntry::new(
            TrailActor::Connector(self.id.clone()),
            TrailAction::Emit {
                kind: emission_kind,
            },
        ));

        if let Err(e) = ctx.publish(emission).await {
            tracing::warn!(connector = %self.id, error = %e, "failed to publish petstore response");
        }
    }

    async fn handle_add_pet(&self, envelope: &Envelope) -> Result<Envelope, ApiError> {
        let pet = expect_payload::<Pet>(envelope, kinds::CMD_ADD_PET)?;
        let url = format!("{}/pet", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(pet)
            .send()
            .await
            .map_err(transport_error)?;
        let added: Pet = parse_response(resp).await?;
        Ok(Envelope::new(
            self.id.clone(),
            EventKind::from_static(kinds::EVT_PET_ADDED),
            added,
        ))
    }

    async fn handle_fetch_pet(&self, envelope: &Envelope) -> Result<Envelope, ApiError> {
        let req = expect_payload::<PetIdRequest>(envelope, kinds::CMD_FETCH_PET)?;
        let url = format!("{}/pet/{}", self.base_url, req.id);
        let resp = self.client.get(&url).send().await.map_err(transport_error)?;
        let pet: Pet = parse_response(resp).await?;
        Ok(Envelope::new(
            self.id.clone(),
            EventKind::from_static(kinds::EVT_PET_FETCHED),
            pet,
        ))
    }

    async fn handle_find_by_status(&self, envelope: &Envelope) -> Result<Envelope, ApiError> {
        let req = expect_payload::<FindByStatusRequest>(envelope, kinds::CMD_FIND_BY_STATUS)?;
        let url = format!("{}/pet/findByStatus", self.base_url);
        let status = match req.status {
            PetStatus::Available => "available",
            PetStatus::Pending => "pending",
            PetStatus::Sold => "sold",
        };
        let resp = self
            .client
            .get(&url)
            .query(&[("status", status)])
            .send()
            .await
            .map_err(transport_error)?;
        let pets: Vec<Pet> = parse_response(resp).await?;
        Ok(Envelope::new(
            self.id.clone(),
            EventKind::from_static(kinds::EVT_PETS_FOUND),
            pets,
        ))
    }

    async fn handle_delete_pet(&self, envelope: &Envelope) -> Result<Envelope, ApiError> {
        let req = expect_payload::<PetIdRequest>(envelope, kinds::CMD_DELETE_PET)?;
        let url = format!("{}/pet/{}", self.base_url, req.id);
        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(transport_error)?;
        if !resp.status().is_success() {
            return Err(http_error(resp).await);
        }
        // DELETE returns no useful body in petstore; echo the id back so callers
        // can correlate by payload as well as correlation_id.
        Ok(Envelope::new(
            self.id.clone(),
            EventKind::from_static(kinds::EVT_PET_DELETED),
            PetIdRequest { id: req.id },
        ))
    }

    async fn handle_update_pet(&self, envelope: &Envelope) -> Result<Envelope, ApiError> {
        let pet = expect_payload::<Pet>(envelope, kinds::CMD_UPDATE_PET)?;
        let url = format!("{}/pet", self.base_url);
        let resp = self
            .client
            .put(&url)
            .json(pet)
            .send()
            .await
            .map_err(transport_error)?;
        let updated: Pet = parse_response(resp).await?;
        Ok(Envelope::new(
            self.id.clone(),
            EventKind::from_static(kinds::EVT_PET_UPDATED),
            updated,
        ))
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────

fn expect_payload<'a, T: 'static>(envelope: &'a Envelope, kind: &str) -> Result<&'a T, ApiError> {
    envelope.payload_as::<T>().ok_or_else(|| ApiError {
        http_status: None,
        message: format!(
            "payload type mismatch for {kind}: got {}",
            envelope.payload.type_name()
        ),
    })
}

fn transport_error(e: reqwest::Error) -> ApiError {
    ApiError {
        http_status: e.status().map(|s| s.as_u16()),
        message: format!("transport error: {e}"),
    }
}

async fn parse_response<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, ApiError> {
    if !resp.status().is_success() {
        return Err(http_error(resp).await);
    }
    resp.json::<T>().await.map_err(|e| ApiError {
        http_status: None,
        message: format!("decode error: {e}"),
    })
}

async fn http_error(resp: reqwest::Response) -> ApiError {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    ApiError {
        http_status: Some(status),
        message: format!("http {status}: {body}"),
    }
}

