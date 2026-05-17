use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxideError {
    #[error("Protocol error: expected '{expected}', found '{found}'")]
    ProtocolError { expected: char, found: char },
    #[error("Incomplete RESP request")]
    IncompleteRequest,
    #[error("Invalid integer in RESP")]
    InvalidInteger,
    #[error("Invalid bulk string")]
    InvalidBulkString,
    #[error("Invalid array length")]
    InvalidArrayLength,
    #[error("Wrong number of arguments for '{cmd}'")]
    WrongArgsCount { cmd: String },
    #[error("Unknown command '{cmd}'")]
    UnknownCommand { cmd: String },
    #[error("WAL write error: {0}")]
    WalWriteError(#[from] std::io::Error),
    #[error("Empty command")]
    EmptyCommand,
}

impl OxideError {
    pub fn to_resp(&self) -> String {
        match self {
            OxideError::ProtocolError { .. } => "-ERR protocol error\r\n".to_string(),
            OxideError::IncompleteRequest => "-ERR incomplete request\r\n".to_string(),
            OxideError::InvalidInteger => "-ERR invalid integer\r\n".to_string(),
            OxideError::InvalidBulkString => "-ERR invalid bulk string\r\n".to_string(),
            OxideError::InvalidArrayLength => "-ERR invalid array length\r\n".to_string(),
            OxideError::WrongArgsCount { cmd } => format!("-ERR wrong number of arguments for '{}'\r\n", cmd),
            OxideError::UnknownCommand { cmd } => format!("-ERR unknown command '{}'\r\n", cmd),
            OxideError::WalWriteError(e) => format!("-ERR WAL error: {}\r\n", e),
            OxideError::EmptyCommand => "-ERR empty command\r\n".to_string(),
        }
    }
}
