//! Conservative detection of token-shaped text.
use crate::policy::Policy;

/// Detect a declared domain prefix followed by at least one base32 character.
pub fn looks_like_token(text: &str, policy: &Policy) -> bool {
    let Some(rest) = text.strip_prefix("tok_") else {
        return false;
    };
    policy.domains.keys().any(|domain| {
        rest.strip_prefix(&domain.0)
            .and_then(|suffix| suffix.strip_prefix('_'))
            .and_then(|suffix| suffix.as_bytes().first())
            .is_some_and(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{DomainName, DomainSettings, ReadLogSink, TokenizeSetting};
    #[test]
    fn recognizes_only_declared_domains_and_base32_prefixes() {
        let policy = Policy {
            json_filters: Default::default(),
            columns: Default::default(),
            domains: [(
                DomainName("email".into()),
                DomainSettings {
                    tokenize: TokenizeSetting::Off,
                    range: None,
                },
            )]
            .into(),
            snapshot_max_age_ms: 0,
            readlog: ReadLogSink::Stdout,
        };
        for text in [
            "tok_email_a",
            "tok_email_2",
            "tok_email_z...",
            "tok_email_7",
        ] {
            assert!(looks_like_token(text, &policy));
        }
        for text in [
            "tok_email_",
            "tok_email_1",
            "tok_email_A",
            "tok_other_a",
            " tok_email_a",
            "tok_emailx_a",
        ] {
            assert!(!looks_like_token(text, &policy));
        }
    }
}
