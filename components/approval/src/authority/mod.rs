//! Native host proofs. No constructor is exposed through serialization or tools.
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use octo_core::{Envelope, control::CANCEL_SCOPE_TAG};
use rand::{RngCore, rngs::OsRng};
use serde_json::{Value, to_vec};
use sha2::{Digest, Sha256};

use crate::{ScopeBinding, ScopeValidator};

#[derive(Clone, Debug)]
pub struct Origin {
    pub principal: String,
    pub connector: String,
    pub conversation: String,
    pub private_destination: String,
    pub may_approve: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stores {
    Owned,
    Only(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Access {
    pub stores: Stores,
    pub read: bool,
    pub write: bool,
    pub schema: bool,
    pub create: bool,
}
impl Access {
    pub fn owner() -> Self {
        Self {
            stores: Stores::Owned,
            read: true,
            write: true,
            schema: true,
            create: true,
        }
    }
    pub fn permits(&self, store: &str, owner: &str, principal: &str) -> bool {
        owner == principal
            && match &self.stores {
                Stores::Owned => true,
                Stores::Only(ids) => ids.iter().any(|id| id == store),
            }
    }
}

#[derive(Clone)]
pub struct Ingress {
    authority: [u8; 32],
    origin: Origin,
    access: Access,
    expires: Instant,
    delegated: bool,
    scope_binding: Option<ScopeBinding>,
}
impl Debug for Ingress {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Ingress").finish_non_exhaustive()
    }
}
impl Ingress {
    pub fn scope(&self) -> Option<&ScopeBinding> {
        self.scope_binding.as_ref()
    }
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
    pub fn access(&self) -> &Access {
        &self.access
    }
    pub fn delegated(&self) -> bool {
        self.delegated
    }
    pub fn limit_lifetime(mut self, lifetime: Duration) -> Self {
        self.expires = self.expires.min(Instant::now() + lifetime);
        self
    }
    pub fn attenuate(
        &self,
        stores: Vec<String>,
        write: bool,
        schema: bool,
    ) -> Result<Self, String> {
        if (write && !self.access.write) || (schema && !self.access.schema) {
            return Err("Cannot widen delegated database rights".into());
        }
        if let Stores::Only(parent) = &self.access.stores {
            if stores.iter().any(|id| !parent.contains(id)) {
                return Err("Cannot widen delegated stores".into());
            }
        }
        let mut child = self.clone();
        child.access = Access {
            stores: Stores::Only(stores),
            read: self.access.read,
            write,
            schema,
            create: false,
        };
        child.origin.may_approve = false;
        child.delegated = true;
        Ok(child)
    }
}

#[derive(Clone)]
pub struct Invocation {
    authority: [u8; 32],
    nonce: [u8; 32],
    digest: [u8; 32],
    ingress: Ingress,
}
impl Debug for Invocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Invocation").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct VerifiedInvocation {
    pub origin: Origin,
    pub access: Access,
    pub delegated: bool,
    pub remaining: Duration,
    pub scope_binding: Option<ScopeBinding>,
}

#[derive(Clone, Debug)]
pub struct DecisionProof(Invocation);

#[derive(Clone)]
pub struct Authority {
    inner: Arc<Inner>,
}
struct Inner {
    id: [u8; 32],
    ttl: Duration,
    capacity: usize,
    pending: Mutex<HashMap<[u8; 32], Instant>>,
    scope_validator: Mutex<Option<Arc<dyn ScopeValidator>>>,
}
impl Authority {
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        let mut id = [0; 32];
        OsRng.fill_bytes(&mut id);
        Self {
            inner: Arc::new(Inner {
                id,
                ttl,
                capacity,
                pending: Mutex::new(HashMap::new()),
                scope_validator: Mutex::new(None),
            }),
        }
    }
    pub fn with_scope_validator(self, validator: Arc<dyn ScopeValidator>) -> Self {
        *self
            .inner
            .scope_validator
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(validator);
        self
    }

    pub fn check_scope(&self, binding: Option<&ScopeBinding>) -> Result<(), String> {
        let Some(binding) = binding else {
            return Ok(());
        };
        binding.validate()?;
        let validator = self
            .inner
            .scope_validator
            .lock()
            .map_err(|_| "Scope validator unavailable")?
            .clone();
        if validator.is_some_and(|validator| validator.allows(binding)) {
            Ok(())
        } else {
            Err("Authority scope is unknown, closed or expired".into())
        }
    }

    /// Host attachment only. Existing immutable scope cannot be exchanged for a
    /// fresh permanent identity by an adapter or delegated tool.
    pub fn bind_scope(&self, ingress: &Ingress, binding: ScopeBinding) -> Result<Ingress, String> {
        if ingress.authority != self.inner.id || Instant::now() >= ingress.expires {
            return Err("Invalid or expired database ingress".into());
        }
        if ingress
            .scope_binding
            .as_ref()
            .is_some_and(|existing| existing != &binding)
        {
            return Err("Cannot rebind database ingress scope".into());
        }
        self.check_scope(Some(&binding))?;
        let mut scoped = ingress.clone();
        scoped.scope_binding = Some(binding);
        Ok(scoped)
    }
    /// Only transport ingress/assembly code receives this authority handle.
    pub fn issue(&self, origin: Origin, access: Access) -> Ingress {
        Ingress {
            authority: self.inner.id,
            origin,
            access,
            expires: Instant::now() + self.inner.ttl,
            delegated: false,
            scope_binding: None,
        }
    }
    pub fn bind(&self, ingress: &Ingress, envelope: &Envelope) -> Result<Invocation, String> {
        if ingress.authority != self.inner.id || Instant::now() >= ingress.expires {
            return Err("Invalid or expired database ingress".into());
        }
        self.check_scope(ingress.scope_binding.as_ref())?;
        let digest = call_digest(envelope)?;
        let mut pending = self.inner.pending.lock().unwrap();
        pending.retain(|_, expires| *expires > Instant::now());
        if pending.len() >= self.inner.capacity {
            return Err("Invocation capacity reached".into());
        }
        let mut nonce = [0; 32];
        OsRng.fill_bytes(&mut nonce);
        pending.insert(nonce, ingress.expires);
        Ok(Invocation {
            authority: self.inner.id,
            nonce,
            digest,
            ingress: ingress.clone(),
        })
    }
    pub fn consume(&self, envelope: &Envelope) -> Result<VerifiedInvocation, String> {
        let proof = envelope
            .context
            .get::<Invocation>()
            .ok_or("Missing authenticated invocation")?;
        self.verify(proof, envelope)
    }
    pub fn bind_decision(
        &self,
        ingress: &Ingress,
        envelope: &Envelope,
    ) -> Result<DecisionProof, String> {
        if ingress.delegated
            || !ingress.origin.may_approve
            || ingress.origin.conversation != ingress.origin.private_destination
        {
            return Err("Approval requires an authorized human in their private channel".into());
        }
        Ok(DecisionProof(self.bind(ingress, envelope)?))
    }
    pub fn consume_decision(&self, envelope: &Envelope) -> Result<VerifiedInvocation, String> {
        let proof = envelope
            .context
            .get::<DecisionProof>()
            .ok_or("Missing human decision proof")?;
        self.verify(&proof.0, envelope)
    }
    fn verify(
        &self,
        proof: &Invocation,
        envelope: &Envelope,
    ) -> Result<VerifiedInvocation, String> {
        if proof.authority != self.inner.id
            || Instant::now() >= proof.ingress.expires
            || proof.digest != call_digest(envelope)?
        {
            return Err("Invocation identity or payload mismatch".into());
        }
        self.check_scope(proof.ingress.scope_binding.as_ref())?;
        if self
            .inner
            .pending
            .lock()
            .unwrap()
            .remove(&proof.nonce)
            .is_none()
        {
            return Err("Invocation already consumed or unknown".into());
        }
        Ok(VerifiedInvocation {
            origin: proof.ingress.origin.clone(),
            access: proof.ingress.access.clone(),
            delegated: proof.ingress.delegated,
            remaining: proof
                .ingress
                .expires
                .saturating_duration_since(Instant::now()),
            scope_binding: proof.ingress.scope_binding.clone(),
        })
    }
    pub fn recognizes(&self, ingress: &Ingress) -> bool {
        ingress.authority == self.inner.id
            && Instant::now() < ingress.expires
            && self.check_scope(ingress.scope_binding.as_ref()).is_ok()
    }
}

fn call_digest(envelope: &Envelope) -> Result<[u8; 32], String> {
    let payload = envelope
        .payload_as::<Value>()
        .ok_or("Invocation requires a JSON payload")?;
    let fields = (
        &envelope.id,
        &envelope.source,
        &envelope.target,
        &envelope.kind,
        &envelope.channel,
        envelope.tags.get(CANCEL_SCOPE_TAG),
        payload,
    );
    let bytes = to_vec(&fields).map_err(|_| "Cannot encode invocation")?;
    Ok(Sha256::digest(bytes).into())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use octo_core::{ConnectorId, Envelope, EventKind, control::CANCEL_SCOPE_TAG};
    use serde_json::json;

    use super::{Access, Authority, Origin, Stores};

    fn origin() -> Origin {
        Origin {
            principal: "person:1".into(),
            connector: "chat".into(),
            conversation: "private-1".into(),
            private_destination: "1".into(),
            may_approve: true,
        }
    }
    fn call() -> Envelope {
        Envelope::new(
            ConnectorId::new("agent"),
            EventKind::new("sql.execute"),
            json!({"sql":"SELECT 1"}),
        )
        .with_target(ConnectorId::new("sql"))
    }
    #[test]
    fn native_proof_binds_identity_body_target_and_is_single_use() {
        let authority = Authority::new(Duration::from_secs(60), 8);
        let ingress = authority.issue(origin(), Access::owner());
        let env = call();
        let proof = authority.bind(&ingress, &env).unwrap();
        let signed = env.with_context(proof);
        let mut changed = signed.clone();
        changed.target = Some(ConnectorId::new("other"));
        assert!(authority.consume(&changed).is_err());
        let changed = signed.clone().with_tag(CANCEL_SCOPE_TAG, "other-scope");
        assert!(authority.consume(&changed).is_err());
        let verified = authority.consume(&signed).unwrap();
        assert_eq!(verified.origin.principal, "person:1");
        assert!(authority.consume(&signed).is_err());
        assert!(
            authority
                .consume(&call().with_tag("approved", "true"))
                .is_err()
        );
        let foreign = Authority::new(Duration::from_secs(60), 8);
        assert!(!foreign.recognizes(&ingress));
    }
    #[test]
    fn delegation_cannot_expand_stores_or_approval_authority() {
        let authority = Authority::new(Duration::from_secs(60), 8);
        let ingress = authority.issue(origin(), Access::owner());
        let child = ingress.attenuate(vec!["s1".into()], true, false).unwrap();
        assert!(!child.origin().may_approve);
        assert!(!child.access().create);
        assert!(child.attenuate(vec!["s2".into()], true, false).is_err());
        assert!(child.attenuate(vec!["s1".into()], false, true).is_err());
        assert_eq!(child.access().stores, Stores::Only(vec!["s1".into()]));
    }
    #[test]
    fn scoped_proofs_are_immutable_inherited_and_closed_at_consumption() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        use crate::{ScopeBinding, ScopeValidator};

        struct Live(AtomicBool);
        impl ScopeValidator for Live {
            fn allows(&self, _: &ScopeBinding) -> bool {
                self.0.load(Ordering::SeqCst)
            }
        }
        let live = Arc::new(Live(AtomicBool::new(true)));
        let authority = Authority::new(Duration::from_secs(60), 8);
        let ingress = authority.issue(origin(), Access::owner());
        let binding = ScopeBinding::new("opaque", "epoch").unwrap();
        assert!(authority.bind_scope(&ingress, binding.clone()).is_err());
        let authority = authority.with_scope_validator(live.clone());
        let scoped = authority.bind_scope(&ingress, binding.clone()).unwrap();
        assert!(
            authority
                .bind_scope(&scoped, ScopeBinding::new("other", "epoch").unwrap())
                .is_err()
        );
        let child = scoped.attenuate(vec!["s1".into()], true, false).unwrap();
        assert_eq!(child.scope(), Some(&binding));
        let admitted = call();
        let proof = authority.bind(&child, &admitted).unwrap();
        let verified = authority.consume(&admitted.with_context(proof)).unwrap();
        assert_eq!(verified.scope_binding, Some(binding));
        let queued = call();
        let proof = authority.bind(&scoped, &queued).unwrap();
        live.0.store(false, Ordering::SeqCst);
        assert!(authority.consume(&queued.with_context(proof)).is_err());
        assert!(authority.bind(&scoped, &call()).is_err());
        assert!(!authority.recognizes(&scoped));
        // None remains an independent legacy principal, never a scoped revival.
        assert!(authority.bind(&ingress, &call()).is_ok());
    }
}
