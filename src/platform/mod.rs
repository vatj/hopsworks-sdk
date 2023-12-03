//! Platform SDK to manage Hopsworks [`User`](user::User)s, [`Project`](project::Project)s, Jobs, etc.
//!
//! This module contains entities such as User or Job, which are not part
//! of the Feature Store API, but are used by the SDK to interact with the Hopsworks platform.
pub mod project;
pub mod user;
