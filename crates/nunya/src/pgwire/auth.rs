//! Pure SCRAM-SHA-256 server authentication.
use crate::error::{NyCode, Refusal};
use crate::token::SessionId;
use data_encoding::BASE64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::fmt;

fn denied() -> Refusal {
    Refusal::new(NyCode::TokenNotValid, "authentication failed")
}
fn unsupported() -> Refusal {
    Refusal::new(NyCode::ProtocolNotAllowed, "channel binding unsupported")
}
fn mac(key: &[u8], message: &[u8]) -> [u8; 32] {
    // HMAC accepts keys of every length.
    let mut h = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts every key length");
    h.update(message);
    h.finalize().into_bytes().into()
}

/// PostgreSQL SCRAM credential with redacted debug output.
pub struct ScramVerifier {
    /// Public password salt.
    pub salt: Vec<u8>,
    /// PBKDF2 iteration count.
    pub iterations: u32,
    /// Hashed client key.
    pub stored_key: [u8; 32],
    /// Server signing key.
    pub server_key: [u8; 32],
}
impl fmt::Debug for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramVerifier")
            .field("iterations", &self.iterations)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}
impl ScramVerifier {
    /// Derive a verifier using PBKDF2-HMAC-SHA-256 (positive iterations required).
    pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> Self {
        assert!(iterations > 0, "SCRAM iterations must be positive");
        let normalized =
            stringprep::saslprep(password).unwrap_or(std::borrow::Cow::Borrowed(password));
        let password = normalized.as_ref();
        let mut input = salt.to_vec();
        input.extend_from_slice(&1u32.to_be_bytes());
        let mut u = mac(password.as_bytes(), &input);
        let mut salted = u;
        for _ in 1..iterations {
            u = mac(password.as_bytes(), &u);
            for (a, b) in salted.iter_mut().zip(u) {
                *a ^= b;
            }
        }
        let client_key = mac(&salted, b"Client Key");
        Self {
            salt: salt.to_vec(),
            iterations,
            stored_key: Sha256::digest(client_key).into(),
            server_key: mac(&salted, b"Server Key"),
        }
    }
    /// Parse the PostgreSQL SCRAM verifier format strictly.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = s.split_once('$')?;
        let (iterations, salt) = params.split_once(':')?;
        if iterations.is_empty() || !iterations.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let iterations = iterations.parse().ok()?;
        if iterations == 0 {
            return None;
        }
        let (stored, server) = keys.split_once(':')?;
        Some(Self {
            salt: BASE64.decode(salt.as_bytes()).ok()?,
            iterations,
            stored_key: BASE64.decode(stored.as_bytes()).ok()?.try_into().ok()?,
            server_key: BASE64.decode(server.as_bytes()).ok()?.try_into().ok()?,
        })
    }
    /// Serialize a verifier in PostgreSQL format; the result is secret.
    pub fn to_pg_string(&self) -> String {
        format!(
            "SCRAM-SHA-256${}:{}${}:{}",
            self.iterations,
            BASE64.encode(&self.salt),
            BASE64.encode(&self.stored_key),
            BASE64.encode(&self.server_key)
        )
    }
}
/// The next server message in a SCRAM exchange.
#[derive(PartialEq, Eq)]
pub enum ScramStep {
    /// Continue with the server-first message.
    Continue(Vec<u8>),
    /// Authentication succeeded with the server signature.
    Done {
        /// Server signature message sent after successful authentication.
        server_final: Vec<u8>,
    },
}
impl fmt::Debug for ScramStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Continue(_) => f.write_str("Continue([REDACTED])"),
            Self::Done { .. } => f.write_str("Done { server_final: [REDACTED] }"),
        }
    }
}
enum State {
    Initial,
    Final {
        bare: String,
        first: String,
        nonce: String,
    },
    Closed,
}
/// Single-use SCRAM server; every failed exchange becomes permanently closed.
pub struct ScramServer {
    verifiers: Vec<ScramVerifier>,
    suffix: String,
    state: State,
}
impl fmt::Debug for ScramServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScramServer { [REDACTED] }")
    }
}
impl ScramServer {
    /// Create an exchange using 144 bits of caller-supplied fresh nonce entropy.
    pub fn new(verifiers: Vec<ScramVerifier>, nonce_source: impl FnOnce() -> [u8; 18]) -> Self {
        Self::with_suffix(verifiers, BASE64.encode(&nonce_source()))
    }
    fn with_suffix(verifiers: Vec<ScramVerifier>, suffix: String) -> Self {
        Self {
            verifiers,
            suffix,
            state: State::Initial,
        }
    }
    /// Advertise the supported SASL mechanism.
    pub fn mechanisms() -> Vec<String> {
        vec!["SCRAM-SHA-256".into()]
    }
    /// Validate client-first and produce server-first.
    pub fn handle_initial(&mut self, mechanism: &str, data: &[u8]) -> Result<ScramStep, Refusal> {
        let state = std::mem::replace(&mut self.state, State::Closed);
        if !matches!(state, State::Initial) || mechanism != "SCRAM-SHA-256" {
            return Err(denied());
        }
        let verifier = self.verifiers.first().ok_or_else(denied)?;
        if verifier.iterations == 0
            || !self.verifiers.iter().all(|candidate| {
                candidate.salt == verifier.salt && candidate.iterations == verifier.iterations
            })
        {
            return Err(denied());
        }
        if data.starts_with(b"y,,") || data.starts_with(b"p=") {
            return Err(unsupported());
        }
        if data.len() > 16_384 {
            return Err(denied());
        }
        let text = std::str::from_utf8(data).map_err(|_| denied())?;
        let bare = text.strip_prefix("n,,").ok_or_else(denied)?;
        let mut fields = bare.split(',');
        let user = fields
            .next()
            .and_then(|s| s.strip_prefix("n="))
            .ok_or_else(denied)?;
        if !valid_user(user) {
            return Err(denied());
        }
        let nonce = fields
            .next()
            .and_then(|s| s.strip_prefix("r="))
            .ok_or_else(denied)?;
        if !valid_nonce(nonce) || fields.next().is_some() {
            return Err(denied());
        }
        let nonce = format!("{}{}", nonce, self.suffix);
        let first = format!(
            "r={},s={},i={}",
            nonce,
            BASE64.encode(&verifier.salt),
            verifier.iterations
        );
        self.state = State::Final {
            bare: bare.into(),
            first: first.clone(),
            nonce,
        };
        Ok(ScramStep::Continue(first.into_bytes()))
    }
    /// Validate client-final and produce the server signature once only.
    pub fn handle_final(&mut self, data: &[u8]) -> Result<ScramStep, Refusal> {
        let State::Final { bare, first, nonce } = std::mem::replace(&mut self.state, State::Closed)
        else {
            return Err(denied());
        };
        if data.len() > 16_384 {
            return Err(denied());
        }
        let text = std::str::from_utf8(data).map_err(|_| denied())?;
        let mut fields = text.split(',');
        if fields.next() != Some("c=biws") || fields.next() != Some(format!("r={nonce}").as_str()) {
            return Err(denied());
        }
        let proof = fields
            .next()
            .and_then(|s| s.strip_prefix("p="))
            .ok_or_else(denied)?;
        if fields.next().is_some() {
            return Err(denied());
        }
        let proof: [u8; 32] = BASE64
            .decode(proof.as_bytes())
            .map_err(|_| denied())?
            .try_into()
            .map_err(|_| denied())?;
        let (without_proof, _) = text.rsplit_once(",p=").ok_or_else(denied)?;
        let auth = format!("{bare},{first},{without_proof}");
        let mut matched = false;
        let mut server_signature = [0u8; 32];
        for verifier in &self.verifiers {
            let signature = mac(&verifier.stored_key, auth.as_bytes());
            let mut key = [0; 32];
            for ((out, a), b) in key.iter_mut().zip(proof).zip(signature) {
                *out = a ^ b;
            }
            // Vetted constant-time equality for every candidate, without early exit.
            let candidate: [u8; 32] = Sha256::digest(key).into();
            let check = mac(&verifier.stored_key, &candidate);
            let mut comparison =
                Hmac::<Sha256>::new_from_slice(&verifier.stored_key).map_err(|_| denied())?;
            comparison.update(&verifier.stored_key);
            let valid = comparison.verify_slice(&check).is_ok();
            let mask = 0u8.wrapping_sub(u8::from(valid));
            let signature = mac(&verifier.server_key, auth.as_bytes());
            for (out, byte) in server_signature.iter_mut().zip(signature) {
                *out = (*out & !mask) | (byte & mask);
            }
            matched |= valid;
        }
        if !matched {
            return Err(denied());
        }
        Ok(ScramStep::Done {
            server_final: format!("v={}", BASE64.encode(&server_signature)).into_bytes(),
        })
    }
}
fn valid_nonce(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| (0x21..=0x7e).contains(&b) && b != b',')
}
fn valid_user(s: &str) -> bool {
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '=' => match (chars.next(), chars.next()) {
                (Some('2'), Some('C')) | (Some('3'), Some('D')) => {}
                _ => return false,
            },
            '\0' | ',' => return false,
            _ => {}
        }
    }
    true
}
/// Decode the PostgreSQL SASLInitialResponse body with exact length checking.
pub fn parse_sasl_initial(payload: &[u8]) -> Result<(String, Vec<u8>), Refusal> {
    let nul = payload.iter().position(|&b| b == 0).ok_or_else(denied)?;
    let mechanism = std::str::from_utf8(&payload[..nul]).map_err(|_| denied())?;
    if mechanism.is_empty() {
        return Err(denied());
    }
    let rest = &payload[nul + 1..];
    let length = i32::from_be_bytes(
        rest.get(..4)
            .ok_or_else(denied)?
            .try_into()
            .map_err(|_| denied())?,
    );
    let data = &rest[4..];
    if length < 0 || length as usize != data.len() {
        return Err(denied());
    }
    Ok((mechanism.into(), data.into()))
}
/// Recognize session credentials with exactly 32 lowercase hexadecimal digits.
pub fn parse_session_user(user: &str) -> Option<SessionId> {
    SessionId::from_hex(user.strip_prefix("session:")?)
}
#[cfg(test)]
mod tests {
    use super::*;
    const SUFFIX: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const INITIAL: &[u8] = b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    const FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const FINAL: &[u8] = b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    fn verifier(password: &str) -> ScramVerifier {
        ScramVerifier::from_password(
            password,
            &BASE64.decode(b"W22ZaJ0SNY7soEsUEjb6gQ==").unwrap(),
            4096,
        )
    }
    fn server(password: &str) -> ScramServer {
        ScramServer::with_suffix(vec![verifier(password)], SUFFIX.into())
    }
    fn client_final(password: &str) -> (Vec<u8>, Vec<u8>) {
        let salt = BASE64.decode(b"W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let mut input = salt;
        input.extend_from_slice(&1u32.to_be_bytes());
        let mut u = mac(password.as_bytes(), &input);
        let mut salted = u;
        for _ in 1..4096 {
            u = mac(password.as_bytes(), &u);
            for (a, b) in salted.iter_mut().zip(u) {
                *a ^= b;
            }
        }
        let without = format!("c=biws,r=rOprNGfwEbeRWgbNEkqO{SUFFIX}");
        let auth = format!("n=user,r=rOprNGfwEbeRWgbNEkqO,{FIRST},{without}");
        let v = verifier(password);
        let mut proof = mac(&salted, b"Client Key");
        for (out, byte) in proof.iter_mut().zip(mac(&v.stored_key, auth.as_bytes())) {
            *out ^= byte;
        }
        (
            format!("{without},p={}", BASE64.encode(&proof)).into_bytes(),
            format!("v={}", BASE64.encode(&mac(&v.server_key, auth.as_bytes()))).into_bytes(),
        )
    }
    #[test]
    fn overlap_accepts_each_password_with_its_server_signature() {
        for password in ["password-a", "password-b"] {
            let mut s = ScramServer::with_suffix(
                vec![verifier("password-a"), verifier("password-b")],
                SUFFIX.into(),
            );
            assert_eq!(
                s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap(),
                ScramStep::Continue(FIRST.as_bytes().into())
            );
            let (proof, server_final) = client_final(password);
            assert_eq!(
                s.handle_final(&proof).unwrap(),
                ScramStep::Done { server_final }
            );
        }
    }
    #[test]
    fn overlap_rejects_wrong_password() {
        let mut s = ScramServer::with_suffix(
            vec![verifier("password-a"), verifier("password-b")],
            SUFFIX.into(),
        );
        s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap();
        assert_eq!(
            s.handle_final(&client_final("wrong").0).unwrap_err(),
            denied()
        );
    }
    #[test]
    fn empty_or_incompatible_verifiers_are_refused() {
        let mut salt = verifier("b");
        salt.salt.push(0);
        let mut iterations = verifier("b");
        iterations.iterations += 1;
        for verifiers in [
            vec![],
            vec![verifier("a"), salt],
            vec![verifier("a"), iterations],
        ] {
            let mut s = ScramServer::new(verifiers, || [0; 18]);
            assert_eq!(
                s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap_err(),
                denied()
            );
            assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
        }
    }
    #[test]
    fn rfc7677_exact_exchange() {
        // RFC 7677 section 3 uses a printable suffix longer than the public
        // constructor's 18 random bytes; inject that literal internally.
        let mut s = server("pencil");
        assert_eq!(
            s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap(),
            ScramStep::Continue(FIRST.as_bytes().into())
        );
        assert_eq!(
            s.handle_final(FINAL).unwrap(),
            ScramStep::Done {
                server_final: b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".to_vec()
            }
        );
        assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
    }
    #[test]
    fn wrong_password_and_failed_exchange_cannot_retry() {
        let mut s = server("wrong");
        s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap();
        assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
        assert_eq!(
            s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap_err(),
            denied()
        );
        assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
    }
    #[test]
    fn verifier_roundtrip_and_strict_parsing() {
        let v = verifier("pencil");
        let parsed = ScramVerifier::parse(&v.to_pg_string()).unwrap();
        assert_eq!(parsed.to_pg_string(), v.to_pg_string());
        for bad in [
            "",
            "SCRAM-SHA-256$0:AA==$AA==:AA==",
            "SCRAM-SHA-256$+1:AA==$AA==:AA==",
            "SCRAM-SHA-256$1:AA==$AA==:AA==",
            "SCRAM-SHA-256$4294967296:AA==$AA==:AA==",
        ] {
            assert!(ScramVerifier::parse(bad).is_none());
        }
        let debug = format!("{v:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains(&BASE64.encode(&v.stored_key)));
        assert!(!debug.contains(&format!("{:?}", v.stored_key)));
    }
    #[test]
    fn session_user_is_exact() {
        assert!(parse_session_user("session:0123456789abcdef0123456789abcdef").is_some());
        for bad in [
            "standing",
            "task:0123456789abcdef0123456789abcdef",
            "session:",
            "session:0123456789ABCDEF0123456789abcdef",
            "session:0123456789abcdef0123456789abcde",
            "session:0123456789abcdef0123456789abcdef0",
        ] {
            assert!(parse_session_user(bad).is_none());
        }
    }
    #[test]
    fn public_constructor_encodes_nonce_and_advertises_plain_scram() {
        let mut s = ScramServer::new(vec![verifier("pencil")], || [0x31; 18]);
        assert_eq!(ScramServer::mechanisms(), vec!["SCRAM-SHA-256"]);
        let ScramStep::Continue(first) = s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap() else {
            panic!("expected continue")
        };
        assert!(std::str::from_utf8(&first).unwrap().starts_with(&format!(
            "r=rOprNGfwEbeRWgbNEkqO{},",
            BASE64.encode(&[0x31; 18])
        )));
        assert_eq!(
            s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap_err(),
            denied()
        );
        assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
    }
    #[test]
    fn malformed_initial_and_channel_binding_close_exchange() {
        for input in [
            b"".as_slice(),
            b"n,,n=user",
            b"n,,n=user,r=",
            b"n,,n=user,r=x,r=y",
            b"n,,m=x,n=user,r=x",
            b"n,a=user,n=user,r=x",
            b"n,,n=a=XX,r=x",
            b"n,,n=user,r=a\0",
            b"n,,n=user,r=\xff",
        ] {
            let mut s = server("pencil");
            assert_eq!(
                s.handle_initial("SCRAM-SHA-256", input).unwrap_err(),
                denied()
            );
            assert_eq!(
                s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap_err(),
                denied()
            );
        }
        for input in [
            b"y,,n=user,r=x".as_slice(),
            b"p=tls-server-end-point,,n=user,r=x",
        ] {
            let mut s = server("pencil");
            assert_eq!(
                s.handle_initial("SCRAM-SHA-256", input).unwrap_err(),
                unsupported()
            );
            assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
        }
        let mut s = server("pencil");
        assert_eq!(s.handle_initial("OTHER", INITIAL).unwrap_err(), denied());
        let mut s = server("pencil");
        assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
        assert_eq!(
            s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap_err(),
            denied()
        );
    }
    #[test]
    fn malformed_final_is_uniform_and_terminal() {
        for input in [
            b"".as_slice(),
            b"c=biws,r=wrong,p=AAAA",
            b"c=eSws,r=x,p=AAAA",
            b"c=biws,r=x",
            b"c=biws,r=x,p=AAAA,p=AAAA",
            b"\xff",
        ] {
            let mut s = server("pencil");
            s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap();
            assert_eq!(s.handle_final(input).unwrap_err(), denied());
            assert_eq!(s.handle_final(FINAL).unwrap_err(), denied());
        }
        let text = std::str::from_utf8(FINAL).unwrap();
        for bad in [
            text.replace("p=d", "p=a"),
            format!("{text},x=extra"),
            text.replace("p=dHz", "p=!!!"),
        ] {
            let mut s = server("pencil");
            s.handle_initial("SCRAM-SHA-256", INITIAL).unwrap();
            assert_eq!(s.handle_final(bad.as_bytes()).unwrap_err(), denied());
        }
    }
    #[test]
    fn sasl_initial_framing_exact() {
        let mut payload = b"SCRAM-SHA-256\0".to_vec();
        payload.extend_from_slice(&(INITIAL.len() as i32).to_be_bytes());
        payload.extend_from_slice(INITIAL);
        assert_eq!(
            parse_sasl_initial(&payload).unwrap(),
            ("SCRAM-SHA-256".into(), INITIAL.to_vec())
        );
        for n in 0..payload.len() {
            assert_eq!(parse_sasl_initial(&payload[..n]).unwrap_err(), denied());
        }
        payload.push(0);
        assert_eq!(parse_sasl_initial(&payload).unwrap_err(), denied());
        assert_eq!(
            parse_sasl_initial(b"m\0\xff\xff\xff\xff").unwrap_err(),
            denied()
        );
        assert_eq!(parse_sasl_initial(b"\0\0\0\0\0").unwrap_err(), denied());
    }
    #[test]
    fn saslprep_maps_and_falls_back_like_postgres() {
        let v = ScramVerifier::from_password("I\u{00ad}X", b"salt", 2);
        assert_eq!(
            v.to_pg_string(),
            ScramVerifier::from_password("IX", b"salt", 2).to_pg_string()
        );
        // A prohibited control character causes raw-byte fallback.
        let raw = ScramVerifier::from_password("a\u{0007}", b"salt", 1);
        let mut input = b"salt".to_vec();
        input.extend_from_slice(&1u32.to_be_bytes());
        let salted = mac(b"a\x07", &input);
        assert_eq!(
            raw.stored_key,
            <[u8; 32]>::from(Sha256::digest(mac(&salted, b"Client Key")))
        );
    }
}
