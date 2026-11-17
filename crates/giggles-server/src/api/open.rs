//! Shared validation for HTTP JSON and gRPC session-open requests.
use super::{error::ApiError, proto};

#[derive(PartialEq, Eq)]
pub(crate) enum Credential {
    None,
    Password(String),
    ScramStart { client_first: String },
}

#[derive(PartialEq, Eq)]
pub(crate) enum OpenKind {
    Binding {
        task: Option<String>,
    },
    RoleSignin {
        role: String,
        database: String,
        credential: Credential,
    },
    ScramContinue {
        exchange: String,
        client_final: String,
    },
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "None",
            Self::Password(_) => "Password([redacted])",
            Self::ScramStart { .. } => "ScramStart([redacted])",
        })
    }
}
impl std::fmt::Debug for OpenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpenKind([redacted])")
    }
}

fn bounded(value: &str, max: usize, ascii: bool) -> Result<(), ApiError> {
    if value.is_empty()
        || value.len() > max
        || (ascii && !value.is_ascii())
        || (!ascii && value.contains('\0'))
    {
        return Err(ApiError::InvalidArgument);
    }
    Ok(())
}

pub(crate) fn classify_open(request: &proto::OpenSessionRequest) -> Result<OpenKind, ApiError> {
    use proto::open_session_request::Credential as C;
    let role_fields =
        request.role.is_some() || request.database.is_some() || request.credential.is_some();
    if request.task.is_some() && role_fields {
        return Err(ApiError::InvalidArgument);
    }
    if let Some(C::ScramContinue(s)) = &request.credential {
        if request.role.is_some()
            || request.database.is_some()
            || request.task.is_some()
            || s.exchange.len() != 64
            || !s
                .exchange
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ApiError::InvalidArgument);
        }
        bounded(&s.client_final, 2048, true)?;
        return Ok(OpenKind::ScramContinue {
            exchange: s.exchange.clone(),
            client_final: s.client_final.clone(),
        });
    }
    if !role_fields {
        return Ok(OpenKind::Binding {
            task: request.task.clone(),
        });
    }
    let role = request.role.as_deref().ok_or(ApiError::InvalidArgument)?;
    let database = request
        .database
        .as_deref()
        .ok_or(ApiError::InvalidArgument)?;
    bounded(role, 63, false)?;
    bounded(database, 63, false)?;
    let credential = match &request.credential {
        None => Credential::None,
        Some(C::Password(password)) => {
            bounded(password, 1024, false)?;
            Credential::Password(password.clone())
        }
        Some(C::ScramStart(s)) => {
            bounded(&s.client_first, 2048, true)?;
            Credential::ScramStart {
                client_first: s.client_first.clone(),
            }
        }
        Some(C::ScramContinue(_)) => unreachable!(),
    };
    Ok(OpenKind::RoleSignin {
        role: role.into(),
        database: database.into(),
        credential,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use proto::open_session_request::Credential as C;

    fn role() -> proto::OpenSessionRequest {
        proto::OpenSessionRequest {
            role: Some("role".into()),
            database: Some("db".into()),
            credential: Some(C::Password("secret".into())),
            ..Default::default()
        }
    }
    fn rejects(request: proto::OpenSessionRequest) {
        let wire = request.encode_to_vec();
        let decoded = proto::OpenSessionRequest::decode(wire.as_slice()).unwrap();
        assert_eq!(classify_open(&decoded), Err(ApiError::InvalidArgument));
    }

    #[test]
    fn credential_debug_redacts_secrets() {
        for credential in [
            Credential::Password("secret".into()),
            Credential::ScramStart {
                client_first: "secret".into(),
            },
        ] {
            assert!(!format!("{credential:?}").contains("secret"));
        }
        assert!(
            !format!(
                "{:?}",
                OpenKind::ScramContinue {
                    exchange: "secret".into(),
                    client_final: "secret".into()
                }
            )
            .contains("secret")
        );
    }

    #[test]
    fn binding_and_credential_shapes() {
        for task in [None, Some("task".into())] {
            let request = proto::OpenSessionRequest {
                task: task.clone(),
                ..Default::default()
            };
            assert_eq!(classify_open(&request), Ok(OpenKind::Binding { task }));
        }
        for credential in [
            None,
            Some(C::Password("p".into())),
            Some(C::ScramStart(proto::ScramStart {
                client_first: "n,,n=r,r=x".into(),
            })),
        ] {
            let request = proto::OpenSessionRequest {
                credential: credential.clone(),
                ..role()
            };
            let expected = match credential {
                None => Credential::None,
                Some(C::Password(p)) => Credential::Password(p),
                Some(C::ScramStart(s)) => Credential::ScramStart {
                    client_first: s.client_first,
                },
                _ => unreachable!(),
            };
            assert_eq!(
                classify_open(&request),
                Ok(OpenKind::RoleSignin {
                    role: "role".into(),
                    database: "db".into(),
                    credential: expected,
                })
            );
        }
        let request = proto::OpenSessionRequest {
            credential: Some(C::ScramContinue(proto::ScramContinue {
                exchange: "a0".repeat(32),
                client_final: "c=biws,r=x,p=y".into(),
            })),
            ..Default::default()
        };
        assert_eq!(
            classify_open(&request),
            Ok(OpenKind::ScramContinue {
                exchange: "a0".repeat(32),
                client_final: "c=biws,r=x,p=y".into(),
            })
        );
    }

    #[test]
    fn rejects_mixed_and_incomplete_shapes() {
        for request in [
            proto::OpenSessionRequest {
                role: Some("r".into()),
                ..Default::default()
            },
            proto::OpenSessionRequest {
                database: Some("d".into()),
                ..Default::default()
            },
            proto::OpenSessionRequest {
                credential: Some(C::Password("p".into())),
                ..Default::default()
            },
            proto::OpenSessionRequest {
                credential: Some(C::ScramStart(proto::ScramStart {
                    client_first: "x".into(),
                })),
                ..Default::default()
            },
            proto::OpenSessionRequest {
                database: None,
                ..role()
            },
            proto::OpenSessionRequest {
                role: None,
                ..role()
            },
        ] {
            rejects(request);
        }
        for request in [
            proto::OpenSessionRequest {
                role: Some("r".into()),
                ..Default::default()
            },
            proto::OpenSessionRequest {
                database: Some("d".into()),
                ..Default::default()
            },
            proto::OpenSessionRequest {
                credential: Some(C::Password("p".into())),
                ..Default::default()
            },
            role(),
        ] {
            rejects(proto::OpenSessionRequest {
                task: Some("task".into()),
                ..request
            });
        }
        for (role, database, task) in [
            (Some("r"), None, None),
            (None, Some("d"), None),
            (None, None, Some("t")),
        ] {
            rejects(proto::OpenSessionRequest {
                role: role.map(str::to_owned),
                database: database.map(str::to_owned),
                task: task.map(str::to_owned),
                credential: Some(C::ScramContinue(proto::ScramContinue {
                    exchange: "a".repeat(64),
                    client_final: "f".into(),
                })),
            });
        }
    }

    #[test]
    fn name_and_password_byte_bounds() {
        for field in ["role", "database", "password"] {
            let max = if field == "password" { 1024 } else { 63 };
            for value in [
                String::new(),
                "x".repeat(max + 1),
                "x\0y".into(),
                "\u{e9}".repeat(max / 2 + 1),
            ] {
                let mut request = role();
                match field {
                    "role" => request.role = Some(value),
                    "database" => request.database = Some(value),
                    _ => request.credential = Some(C::Password(value)),
                }
                rejects(request);
            }
            for value in ["x".into(), "x".repeat(max)] {
                let mut request = role();
                match field {
                    "role" => request.role = Some(value),
                    "database" => request.database = Some(value),
                    _ => request.credential = Some(C::Password(value)),
                }
                assert!(classify_open(&request).is_ok());
            }
        }
    }

    #[test]
    fn scram_ascii_bounds_and_exchange_format() {
        for first in [true, false] {
            for (value, valid) in [
                (String::new(), false),
                ("x".repeat(2049), false),
                ("\u{e9}".into(), false),
                ("x".into(), true),
                ("x".repeat(2048), true),
            ] {
                let request = if first {
                    proto::OpenSessionRequest {
                        credential: Some(C::ScramStart(proto::ScramStart {
                            client_first: value,
                        })),
                        ..role()
                    }
                } else {
                    proto::OpenSessionRequest {
                        credential: Some(C::ScramContinue(proto::ScramContinue {
                            exchange: "f".repeat(64),
                            client_final: value,
                        })),
                        ..Default::default()
                    }
                };
                assert_eq!(classify_open(&request).is_ok(), valid);
            }
        }
        for exchange in [
            String::new(),
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
            format!("{}\u{e9}", "a".repeat(62)),
        ] {
            rejects(proto::OpenSessionRequest {
                credential: Some(C::ScramContinue(proto::ScramContinue {
                    exchange,
                    client_final: "f".into(),
                })),
                ..Default::default()
            });
        }
    }
}
