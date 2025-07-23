use std::fmt::{self, Debug, Display};
use std::str::FromStr;

use clap::error::ErrorFormatter;
use thiserror::Error;
use winnow::ascii::space1;
use winnow::combinator::{alt, eof};
use winnow::error::ContextError;
use winnow::token::literal;
use winnow::Result;
use winnow::{ascii::hex_digit0, Parser};

#[derive(Debug, Error)]
pub enum DecodeTokenError {
    #[error("Hex decode error")]
    HexDecode(#[from] hex::FromHexError),
    #[error("Unexpected length")]
    UnexpectedLength,
}

#[derive(Debug, Error)]
pub enum DecodeSideError {
    #[error("Hex decode error")]
    HexDecode(#[from] hex::FromHexError),
    #[error("Unexpected length")]
    UnexpectedLength,
}

#[derive(PartialEq)]
pub struct Side(Box<[u8; 8]>);

impl FromStr for Side {
    type Err = DecodeSideError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Side(
            hex::decode(s)?
                .try_into()
                .map_err(|_| DecodeSideError::UnexpectedLength)?,
        ))
    }
}

impl Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(*self.0))
    }
}

impl Debug for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Token(Box<[u8; 32]>);

impl FromStr for Token {
    type Err = DecodeTokenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Token(
            hex::decode(s)?
                .try_into()
                .map_err(|_| DecodeTokenError::UnexpectedLength)?,
        ))
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(*self.0))
    }
}

impl Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

// Represents the type of handshake received
#[derive(PartialEq)]
pub enum HandshakeType {
    Legacy { token: Token },
    Modern { token: Token, side: Side },
}

impl HandshakeType {
    pub fn get_token(&self) -> &Token {
        match self {
            HandshakeType::Legacy { token } => token,
            HandshakeType::Modern { token, .. } => token,
        }
    }

    pub fn get_side(&self) -> Option<&Side> {
        match self {
            HandshakeType::Legacy { .. } => None,
            HandshakeType::Modern { token: _, side } => Some(side),
        }
    }
}

impl fmt::Display for HandshakeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HandshakeType::Legacy { token } => {
                write!(f, "Legacy(token={token})")
            }
            HandshakeType::Modern { token, side } => {
                write!(f, "Modern(token={token}, side={side})",)
            }
        }
    }
}

impl Debug for HandshakeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

fn parse<T: FromStr>(input: &mut &str) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    hex_digit0.try_map(str::parse).parse_next(input)
}

pub fn parse_handshake(input: &mut &str) -> Result<HandshakeType> {
    // Parser for Legacy handshake: "please relay <64 hex chars>"
    let legacy = (literal("please relay"), space1, parse::<Token>, eof)
        .map(|(_, _, token, _)| HandshakeType::Legacy { token });

    // Parser for Modern handshake: "please relay <64 hex chars> for side <16 hex chars>"
    let modern = (
        literal("please relay"),
        space1,
        parse::<Token>,
        space1,
        literal("for side"),
        space1,
        parse::<Side>,
        eof,
    )
        .map(|(_, _, token, _, _, _, side, _)| HandshakeType::Modern { token, side });

    // Choose between Legacy or Modern
    alt((modern, legacy)).parse_next(input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_modern_handshake() {
        let hs = format!(
            "please relay {} for side {}",
            "f".repeat(64),
            "f".repeat(16)
        );

        assert!(matches!(
            parse_handshake(&mut hs.as_ref()).unwrap(),
            HandshakeType::Modern { .. }
        ));
    }

    #[test]
    fn test_parse_lagacy_handshake() {
        let hs = format!("please relay {}", "f".repeat(64));

        assert!(matches!(
            parse_handshake(&mut hs.as_ref()).unwrap(),
            HandshakeType::Legacy { .. }
        ));
    }

    #[test]
    fn test_parse_invalid_lagacy_handshake() {
        let hs = format!("please relay {} ", "f".repeat(64));

        assert!(parse_handshake(&mut hs.as_ref()).is_err());
    }
}
