//! SCRAM-SHA-256 with raw UTF-8 passwords (no SASLprep). ASCII passwords match PostgreSQL.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use std::fmt;

pub const DEFAULT_ITERATIONS: u32 = 4096;
pub const MECHANISM: &str = "SCRAM-SHA-256";
const MAX_ITERATIONS: u32 = 10_000_000;

#[derive(Clone, PartialEq, Eq)]
pub struct Verifier {
    iterations: u32,
    salt: Vec<u8>,
    stored_key: [u8; 32],
    server_key: [u8; 32],
}
impl fmt::Debug for Verifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Verifier {{ iterations: {}, .. }}", self.iterations)
    }
}
impl Verifier {
    /// Parses PostgreSQL SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey text.
    /// Accepts 1..=10,000,000 iterations, 1..=64 salt bytes, and 32-byte keys.
    pub fn parse(value: &str) -> Option<Self> {
        let rest = value.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = rest.split_once('$')?;
        let (iterations, salt) = params.split_once(':')?;
        if iterations.is_empty() || !iterations.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let iterations = iterations.parse().ok()?;
        if !(1..=MAX_ITERATIONS).contains(&iterations) {
            return None;
        }
        let salt = STANDARD.decode(salt).ok()?;
        if !(1..=64).contains(&salt.len()) {
            return None;
        }
        let (stored_key, server_key) = keys.split_once(':')?;
        Some(Self {
            iterations,
            salt,
            stored_key: STANDARD.decode(stored_key).ok()?.try_into().ok()?,
            server_key: STANDARD.decode(server_key).ok()?.try_into().ok()?,
        })
    }
    /// Encodes this verifier in PostgreSQL format using standard padded base64.
    pub fn encode(&self) -> String {
        format!(
            "{MECHANISM}${}:{}${}:{}",
            self.iterations,
            STANDARD.encode(&self.salt),
            STANDARD.encode(self.stored_key),
            STANDARD.encode(self.server_key)
        )
    }
    /// Uses raw UTF-8 without SASLprep; ASCII passwords match PostgreSQL.
    ///
    /// # Panics
    /// Panics unless salt has 1..=64 bytes and iterations is 1..=10,000,000.
    pub fn derive(password: &str, salt: &[u8], iterations: u32) -> Self {
        assert!((1..=MAX_ITERATIONS).contains(&iterations));
        assert!((1..=64).contains(&salt.len()));
        let salted = pbkdf2(password.as_bytes(), salt, iterations);
        Self {
            iterations,
            salt: salt.to_vec(),
            stored_key: Sha256::digest(hmac(&salted, b"Client Key")).into(),
            server_key: hmac(&salted, b"Server Key"),
        }
    }
    /// Verifies a raw UTF-8 password using this verifier's salt and work factor.
    pub fn verify_password(&self, password: &str) -> bool {
        let salted = pbkdf2(password.as_bytes(), &self.salt, self.iterations);
        let stored: [u8; 32] = Sha256::digest(hmac(&salted, b"Client Key")).into();
        ct_eq(&stored, &self.stored_key)
    }
    /// Generates an encoded verifier with a random 16-byte salt and default iterations.
    /// Panics if operating system randomness is unavailable.
    pub fn generate(password: &str) -> String {
        let mut salt = [0; 16];
        getrandom::fill(&mut salt).expect("operating system random source failed");
        Self::derive(password, &salt, DEFAULT_ITERATIONS).encode()
    }
}
/// Returns whether text is a valid PostgreSQL SCRAM-SHA-256 verifier.
pub fn is_verifier(text: &str) -> bool {
    Verifier::parse(text).is_some()
}
fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|v| v ^ 0x36));
    inner.update(data);
    let mut outer = Sha256::new();
    outer.update(block.map(|v| v ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}
fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut input = salt.to_vec();
    input.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac(password, &input);
    let mut result = u;
    for _ in 1..iterations {
        u = hmac(password, &u);
        for (a, b) in result.iter_mut().zip(u) {
            *a ^= b;
        }
    }
    result
}
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (a, b)| diff | (a ^ b)) == 0
}
#[derive(Debug, PartialEq, Eq)]
pub enum ScramError {
    Mechanism,
    ChannelBinding,
    Malformed,
    Authentication,
}
struct Pending {
    header: String,
    bare: String,
    first: String,
    nonce: String,
}
enum State {
    Initial,
    Pending(Pending),
    Dead,
}
/// One terminal-on-error SCRAM authentication exchange for the startup username.
pub struct ServerExchange {
    verifier: Verifier,
    unknown: bool,
    server_nonce: String,
    state: State,
}
impl ServerExchange {
    /// Uses a random 18-byte server nonce; panics if system randomness fails.
    /// A missing verifier uses username-derived salt/iterations and always rejects authentication.
    pub fn new(username: &str, verifier: Option<Verifier>) -> Self {
        let mut nonce = [0; 18];
        getrandom::fill(&mut nonce).expect("operating system random source failed");
        Self::make(username, verifier, STANDARD.encode(nonce))
    }
    fn make(username: &str, verifier: Option<Verifier>, server_nonce: String) -> Self {
        let unknown = verifier.is_none();
        let verifier = verifier.unwrap_or_else(|| {
            let digest = Sha256::digest(username.as_bytes());
            // Same iteration count as generated verifiers, so the server-first
            // message does not reveal whether the role exists.
            Verifier {
                iterations: DEFAULT_ITERATIONS,
                salt: digest[..16].to_vec(),
                stored_key: hmac(&digest, b"fake stored key"),
                server_key: hmac(&digest, b"fake server key"),
            }
        });
        Self {
            verifier,
            unknown,
            server_nonce,
            state: State::Initial,
        }
    }
    #[cfg(test)]
    fn with_server_nonce(username: &str, verifier: Option<Verifier>, nonce: &str) -> Self {
        Self::make(username, verifier, nonce.to_owned())
    }
    /// Accepts a first message of at most 1024 bytes and returns server-first bytes.
    /// The n= value is ignored; the startup username selects the verifier.
    /// Any error permanently ends the exchange.
    pub fn client_first(&mut self, mechanism: &str, data: &[u8]) -> Result<Vec<u8>, ScramError> {
        let state = std::mem::replace(&mut self.state, State::Dead);
        if !matches!(state, State::Initial) {
            return Err(ScramError::Malformed);
        }
        if mechanism != MECHANISM {
            return Err(ScramError::Mechanism);
        }
        let text = message(data)?;
        let (flag, remainder) = text.split_once(',').ok_or(ScramError::Malformed)?;
        if flag.starts_with("p=") {
            return Err(ScramError::ChannelBinding);
        }
        if flag != "n" && flag != "y" {
            return Err(ScramError::Malformed);
        }
        let (authzid, bare) = remainder.split_once(',').ok_or(ScramError::Malformed)?;
        if !authzid.is_empty() {
            return Err(ScramError::Malformed);
        }
        let attrs = attributes(bare)?;
        if attrs.len() < 2 || attrs[0].0 != "n" || attrs[1].0 != "r" || !valid_nonce(attrs[1].1) {
            return Err(ScramError::Malformed);
        }
        let nonce = format!("{}{}", attrs[1].1, self.server_nonce);
        let first = format!(
            "r={nonce},s={},i={}",
            STANDARD.encode(&self.verifier.salt),
            self.verifier.iterations
        );
        let result = first.as_bytes().to_vec();
        self.state = State::Pending(Pending {
            header: format!("{flag},,"),
            bare: bare.to_owned(),
            first,
            nonce,
        });
        Ok(result)
    }
    /// Verifies a final message of at most 1024 bytes and returns the server signature.
    /// Success or error permanently ends the exchange.
    pub fn client_final(&mut self, data: &[u8]) -> Result<Vec<u8>, ScramError> {
        let state = std::mem::replace(&mut self.state, State::Dead);
        let State::Pending(pending) = state else {
            return Err(ScramError::Malformed);
        };
        let text = message(data)?;
        let attrs = attributes(text)?;
        if attrs.len() < 3
            || attrs[0].0 != "c"
            || attrs[1].0 != "r"
            || attrs.last().unwrap().0 != "p"
        {
            return Err(ScramError::Malformed);
        }
        let binding = STANDARD
            .decode(attrs[0].1)
            .map_err(|_| ScramError::Malformed)?;
        if binding != pending.header.as_bytes() || attrs[1].1 != pending.nonce {
            return Err(ScramError::Authentication);
        }
        let proof: [u8; 32] = STANDARD
            .decode(attrs.last().unwrap().1)
            .map_err(|_| ScramError::Malformed)?
            .try_into()
            .map_err(|_| ScramError::Malformed)?;
        let without_proof = text.rsplit_once(',').ok_or(ScramError::Malformed)?.0;
        let auth = format!("{},{},{}", pending.bare, pending.first, without_proof);
        let signature = hmac(&self.verifier.stored_key, auth.as_bytes());
        let client_key: [u8; 32] = std::array::from_fn(|i| proof[i] ^ signature[i]);
        let matches = ct_eq(&Sha256::digest(client_key), &self.verifier.stored_key);
        if !matches || self.unknown {
            return Err(ScramError::Authentication);
        }
        Ok(format!(
            "v={}",
            STANDARD.encode(hmac(&self.verifier.server_key, auth.as_bytes()))
        )
        .into_bytes())
    }
}
fn message(data: &[u8]) -> Result<&str, ScramError> {
    if data.len() > 1024 {
        return Err(ScramError::Malformed);
    }
    std::str::from_utf8(data).map_err(|_| ScramError::Malformed)
}
fn attributes(text: &str) -> Result<Vec<(&str, &str)>, ScramError> {
    let mut result = Vec::new();
    for attr in text.split(',') {
        let (key, value) = attr.split_once('=').ok_or(ScramError::Malformed)?;
        if key.len() != 1
            || !key.as_bytes()[0].is_ascii_alphabetic()
            || key == "m"
            || result.iter().any(|(k, _)| *k == key)
            || value.bytes().any(|b| b == 0 || b < 0x20 || b == 0x7f)
            || (value.is_empty() && key != "n")
        {
            return Err(ScramError::Malformed);
        }
        result.push((key, value));
    }
    Ok(result)
}
fn valid_nonce(nonce: &str) -> bool {
    (1..=256).contains(&nonce.len())
        && nonce
            .bytes()
            .all(|b| (0x20..=0x7e).contains(&b) && b != b',')
}

#[cfg(test)]
mod tests {
    use super::*;
    const CLIENT: &str = "rOprNGfwEbeRWgbNEkqO";
    const SERVER: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    fn verifier() -> Verifier {
        Verifier::derive(
            "pencil",
            &STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap(),
            4096,
        )
    }
    fn exchange() -> ServerExchange {
        ServerExchange::with_server_nonce("user", Some(verifier()), SERVER)
    }
    fn first(s: &mut ServerExchange) -> Vec<u8> {
        s.client_first(MECHANISM, format!("n,,n=user,r={CLIENT}").as_bytes())
            .unwrap()
    }
    fn client_response(password: &str, header: &str, bare: &str, first: &[u8]) -> Vec<u8> {
        let first = std::str::from_utf8(first).unwrap();
        let attrs = attributes(first).unwrap();
        let salt = STANDARD.decode(attrs[1].1).unwrap();
        let rounds = attrs[2].1.parse().unwrap();
        let salted = pbkdf2(password.as_bytes(), &salt, rounds);
        let key = hmac(&salted, b"Client Key");
        let stored: [u8; 32] = Sha256::digest(key).into();
        let final_bare = format!("c={},r={}", STANDARD.encode(header), attrs[0].1);
        let auth = format!("{bare},{first},{final_bare}");
        let signature = hmac(&stored, auth.as_bytes());
        let proof: [u8; 32] = std::array::from_fn(|i| key[i] ^ signature[i]);
        format!("{final_bare},p={}", STANDARD.encode(proof)).into_bytes()
    }
    #[test]
    fn hmac_rfc4231() {
        assert_eq!(
            STANDARD.encode(hmac(&[0x0b; 20], b"Hi There")),
            "sDRMYdjbOFNcqK/OrwvxK4gdwgDJgz2nJuk3bC4yz/c="
        );
        assert_eq!(
            STANDARD.encode(hmac(b"Jefe", b"what do ya want for nothing?")),
            "W9zBRr9gdU5qBCQmCJV1x1oAPwidJzmDnexYuWTsOEM="
        );
    }
    #[test]
    fn rfc7677() {
        let mut s = exchange();
        assert_eq!(
            first(&mut s),
            format!("r={CLIENT}{SERVER},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096").as_bytes()
        );
        let response =
            format!("c=biws,r={CLIENT}{SERVER},p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
        assert_eq!(
            s.client_final(response.as_bytes()).unwrap(),
            b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
        assert_eq!(
            s.client_final(response.as_bytes()),
            Err(ScramError::Malformed)
        );
    }
    #[test]
    fn verifier_roundtrip_and_redaction() {
        let v = verifier();
        assert!(v.verify_password("pencil"));
        assert!(!v.verify_password("wrong"));
        assert_eq!(Verifier::parse(&v.encode()), Some(v.clone()));
        assert!(is_verifier(&v.encode()));
        assert_eq!(format!("{v:?}"), "Verifier { iterations: 4096, .. }");
        for text in [
            "",
            "SCRAM-SHA-256$0:cw==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "SCRAM-SHA-256$10000001:cw==$eA==:eA==",
            "SCRAM-SHA-256$1:!$!:!",
        ] {
            assert!(Verifier::parse(text).is_none());
        }
        for salt in [vec![], vec![1; 65]] {
            let mut bad = v.clone();
            bad.salt = salt;
            assert!(Verifier::parse(&bad.encode()).is_none());
        }
        for len in [1, 64] {
            let v = Verifier::derive("p", &vec![1; len], 1);
            assert_eq!(Verifier::parse(&v.encode()), Some(v));
        }
        let mut v = v;
        v.iterations = MAX_ITERATIONS;
        assert!(Verifier::parse(&v.encode()).is_some());
    }
    #[test]
    fn generated_authentication_and_wrong_password() {
        let encoded = Verifier::generate("secret");
        let v = Verifier::parse(&encoded).unwrap();
        assert_eq!(v.salt.len(), 16);
        assert_eq!(v.iterations, 4096);
        for (password, success) in [("secret", true), ("wrong", false)] {
            let mut s = ServerExchange::new("user", Some(v.clone()));
            let first = first(&mut s);
            let final_msg = client_response(password, "n,,", &format!("n=user,r={CLIENT}"), &first);
            let result = s.client_final(&final_msg);
            if success {
                assert!(result.unwrap().starts_with(b"v="));
            } else {
                assert_eq!(result, Err(ScramError::Authentication));
            }
        }
    }
    #[test]
    fn unknown_user_stable_and_always_rejected() {
        let mut a = ServerExchange::with_server_nonce("missing", None, SERVER);
        let mut b = ServerExchange::with_server_nonce("missing", None, SERVER);
        assert_eq!(a.verifier, b.verifier);
        assert_eq!(first(&mut a), first(&mut b));
        a.verifier.stored_key = Sha256::digest([0u8; 32]).into();
        let State::Pending(p) = &a.state else {
            panic!()
        };
        // Construct a proof matching the synthetic verifier to exercise the explicit unknown-user guard.
        let final_bare = format!("c=biws,r={}", p.nonce);
        let auth = format!("{},{},{}", p.bare, p.first, final_bare);
        let sig = hmac(&a.verifier.stored_key, auth.as_bytes());
        let msg = format!("{final_bare},p={}", STANDARD.encode(sig));
        assert_eq!(
            a.client_final(msg.as_bytes()),
            Err(ScramError::Authentication)
        );
    }
    #[test]
    fn first_errors_are_terminal() {
        let cases: Vec<(&str, Vec<u8>, ScramError)> = vec![
            ("other", b"n,,n=user,r=x".to_vec(), ScramError::Mechanism),
            (
                MECHANISM,
                b"p=tls-server-end-point,,n=user,r=x".to_vec(),
                ScramError::ChannelBinding,
            ),
            (
                MECHANISM,
                b"n,a=user,n=user,r=x".to_vec(),
                ScramError::Malformed,
            ),
            (
                MECHANISM,
                b"n,,m=required,n=user,r=x".to_vec(),
                ScramError::Malformed,
            ),
            (
                MECHANISM,
                b"n,,n=user,r=x,r=y".to_vec(),
                ScramError::Malformed,
            ),
            (MECHANISM, b"n,,r=x,n=user".to_vec(), ScramError::Malformed),
            (MECHANISM, vec![b'a'; 1025], ScramError::Malformed),
            (
                MECHANISM,
                format!("n,,n=user,r={}", "x".repeat(257)).into_bytes(),
                ScramError::Malformed,
            ),
        ];
        for (mechanism, data, error) in cases {
            let mut s = exchange();
            assert_eq!(s.client_first(mechanism, &data), Err(error));
            assert_eq!(
                s.client_first(MECHANISM, b"n,,n=user,r=x"),
                Err(ScramError::Malformed)
            );
        }
    }
    #[test]
    fn nonce_and_header_boundaries() {
        for nonce in [" ".to_owned(), "x".repeat(256)] {
            let mut s = exchange();
            assert!(
                s.client_first(MECHANISM, format!("y,,n=,r={nonce}").as_bytes())
                    .is_ok()
            );
        }
        for nonce in ["", "x\t", "\u{e9}"] {
            let mut s = exchange();
            assert_eq!(
                s.client_first(MECHANISM, format!("n,,n=user,r={nonce}").as_bytes()),
                Err(ScramError::Malformed)
            );
        }
        let mut s = exchange();
        assert!(
            s.client_first(MECHANISM, b"y,,n=ignored=2Cname,r=x,z=extension")
                .is_ok()
        );
    }
    #[test]
    fn final_errors_and_order() {
        let mut s = exchange();
        assert_eq!(s.client_final(b""), Err(ScramError::Malformed));
        assert_eq!(
            s.client_first(MECHANISM, b"n,,n=u,r=x"),
            Err(ScramError::Malformed)
        );
        let mut s = exchange();
        first(&mut s);
        assert_eq!(
            s.client_first(MECHANISM, b"n,,n=u,r=x"),
            Err(ScramError::Malformed)
        );
        let valid = format!("c=biws,r={CLIENT}{SERVER},p={}", STANDARD.encode([0; 32]));
        let cases = vec![
            (valid.replace("biws", "eSws"), ScramError::Authentication),
            (
                valid.replace(CLIENT, "tampered"),
                ScramError::Authentication,
            ),
            (valid.replace("biws", "!"), ScramError::Malformed),
            (
                format!("c=biws,r={CLIENT}{SERVER},p=!"),
                ScramError::Malformed,
            ),
            (
                format!("c=biws,r={CLIENT}{SERVER},p=eA=="),
                ScramError::Malformed,
            ),
            (format!("{valid},z=extension"), ScramError::Malformed),
            (valid.replace("r=", "c=biws,r="), ScramError::Malformed),
            ("x".repeat(1025), ScramError::Malformed),
        ];
        for (data, error) in cases {
            let mut s = exchange();
            first(&mut s);
            assert_eq!(s.client_final(data.as_bytes()), Err(error));
            assert_eq!(s.client_final(valid.as_bytes()), Err(ScramError::Malformed));
        }
    }
    #[test]
    fn constant_time_comparison_lengths() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
    }
    #[test]
    fn exact_message_limits_and_authenticated_extensions() {
        let mut s = exchange();
        let prefix = "y,,n=bad=XX,r=x,z=";
        let first_message = format!("{prefix}{}", "a".repeat(1024 - prefix.len()));
        assert_eq!(first_message.len(), 1024);
        let server_first = s.client_first(MECHANISM, first_message.as_bytes()).unwrap();
        let server_first = std::str::from_utf8(&server_first).unwrap();
        let nonce = attributes(server_first).unwrap()[0].1;
        let prefix = format!("c=eSws,r={nonce},z=");
        // Comma-p-equals plus a 44-byte proof occupies 47 bytes.
        let final_bare = format!("{prefix}{}", "b".repeat(1024 - prefix.len() - 47));
        let auth = format!("{},{server_first},{final_bare}", &first_message[3..]);
        let salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let key = hmac(&pbkdf2(b"pencil", &salt, 4096), b"Client Key");
        let signature = hmac(&verifier().stored_key, auth.as_bytes());
        let proof: [u8; 32] = std::array::from_fn(|i| key[i] ^ signature[i]);
        let final_message = format!("{final_bare},p={}", STANDARD.encode(proof));
        assert_eq!(final_message.len(), 1024);
        assert!(
            s.client_final(final_message.as_bytes())
                .unwrap()
                .starts_with(b"v=")
        );
    }
    #[test]
    fn verifier_rejects_key_sizes_and_trailing_delimiters() {
        let valid_key = STANDARD.encode([0; 32]);
        for size in [0, 1, 31, 33] {
            let bad_key = STANDARD.encode(vec![0; size]);
            for (stored, server) in [(&bad_key, &valid_key), (&valid_key, &bad_key)] {
                assert!(
                    Verifier::parse(&format!("SCRAM-SHA-256$4096:cw==${stored}:{server}"))
                        .is_none()
                );
            }
        }
        let valid = verifier().encode();
        for suffix in [":", "$", ":junk", "$junk"] {
            assert!(Verifier::parse(&format!("{valid}{suffix}")).is_none());
        }
        let a = ServerExchange::with_server_nonce("missing", None, SERVER);
        let b = ServerExchange::with_server_nonce("missing", None, SERVER);
        let c = ServerExchange::with_server_nonce("different", None, SERVER);
        assert_eq!(a.verifier.iterations, b.verifier.iterations);
        assert_eq!(a.verifier.salt, b.verifier.salt);
        assert_eq!(c.verifier.iterations, DEFAULT_ITERATIONS);
        assert_ne!(a.verifier.salt, c.verifier.salt);
    }
}
