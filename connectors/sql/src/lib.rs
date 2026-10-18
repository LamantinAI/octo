//! SQLite stores with native SQL policy, authenticated host calls and durable approvals.
mod config;
mod connector;
mod database;
pub mod engine;
mod error;

pub use config::Limits;
pub use connector::{SqlConnectorFactory, factory};
pub use database::Database;
pub use error::SqlError;
