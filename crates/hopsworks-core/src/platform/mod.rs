//! Platform SDK to manage Hopsworks [`Project`](project::Project)s, Jobs, [`User`](user::User)s, etc.
//!
//! This module contains entities such as Project or Job, which are not part
//! of the Feature Store API, but are used by the SDK to interact with the Hopsworks platform.
pub mod file_system;
pub mod job;
pub mod job_execution;
pub mod kafka;
pub mod project;
pub mod user;
