use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxideError {
    #[error("Protocol error: expected '{expected}', found '{found}'")]
    ProtocolError { expected: char, found: char},

    #[error("Protocol error: invalid array length format")]
    InvalidArrayLength,

    #[error("Protocol desynchronization: arguments count mismatch")]
    ProtocolDesync,

    #[error("Command error: wrong number of arguments for '{cmd}' command")]
    WrongArgsCount { cmd: String },

    #[error("Command error: unknown command '{cmd}'")]
    UnknownCommand { cmd: String },

    #[error("Internal error: data storage lock is poisoned")]
    PoisonedLock,

    #[error("Storage error: failed to write to WAL log")]
    WalWriteError(#[from] std::io::Error),
}

impl OxideError {
    pub fn to_resp(&self) -> String {
                match self {
            OxideError::ProtocolError { .. } => "-ERR unknown protocol or formatting\r\n".to_string(),
            OxideError::InvalidArrayLength => "-ERR invalid array length\r\n".to_string(),
            OxideError::ProtocolDesync => "-ERR protocol desynchronization\r\n".to_string(),
            OxideError::WrongArgsCount { cmd } => format!("-ERR wrong number of arguments for '{}' command\r\n", cmd.to_lowercase()),
            OxideError::UnknownCommand { cmd } => format!("-ERR unknown command '{}'\r\n", cmd),
            OxideError::PoisonedLock => "-ERR internal server error (poisoned lock)\r\n".to_string(),
            OxideError::WalWriteError(e) => format!("-ERR WAL write failed: {}\r\n", e),
        }
    }
}