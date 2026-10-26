//! Host-authenticated invocation grants and operation-specific approval contracts.
mod authority;
mod operation;
mod scope;
pub use scope::{ScopeBinding, ScopeValidator};
mod presentation;
mod settings;

pub use authority::{
    Access, Authority, DecisionProof, Ingress, Invocation, Origin, Stores, VerifiedInvocation,
};
pub use operation::{OperationState, PreparedOperation, operation_hash};
pub use presentation::{ApprovalAction, ApprovalPresentation};
pub use settings::AuthoritySettings;
