use thiserror::Error;

pub type Result<T> = std::result::Result<T, GuardError>;

#[derive(Debug, Error)]
pub enum GuardError {
    #[error("source error: {0}")]
    Source(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("handler error: {0}")]
    Handler(String),

    #[error("sink error: {0}")]
    Sink(String),
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for GuardError {
    fn from(e: sqlx::Error) -> Self {
        GuardError::Store(e.to_string())
    }
}
