use thiserror::Error;

pub type Result<T> = std::result::Result<T, PolarsArrowDestinationError>;

#[derive(Error, Debug)]
pub enum PolarsArrowDestinationError {
    #[error(transparent)]
    PolarsError(#[from] polars::error::PolarsError),

    #[error(transparent)]
    ConnectorXError(#[from] connectorx::errors::ConnectorXError),

    /// Any other errors that are too trivial to be put here explicitly.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
