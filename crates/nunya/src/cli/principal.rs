//! Principal administration with locally derived credentials and fixed diagnostics.
use super::session::{ConnectionOpts, HelperError};
use crate::{
    mgmt::client::{ClientError, ConnectionView, CredentialSummary, MgmtClient, PrincipalView},
    pgwire::auth::ScramVerifier,
    state::model::{CredentialId, Mode, PrincipalKind, RouteId},
    token::PrincipalId,
};
use clap::{Args, Subcommand};
use std::{collections::BTreeSet, fmt};
use zeroize::Zeroizing;

/// Fixed administration failures, excluding all credential material.
#[derive(Debug)]
pub enum AdminError {
    /// Invalid local argument.
    Invalid(&'static str),
    /// Connection configuration failure.
    Connection(HelperError),
    /// Public management failure.
    Management(ClientError),
}
impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Connection(error) => error.fmt(f),
            Self::Management(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for AdminError {}
impl From<ClientError> for AdminError {
    fn from(error: ClientError) -> Self {
        Self::Management(error)
    }
}
impl From<HelperError> for AdminError {
    fn from(error: HelperError) -> Self {
        Self::Connection(error)
    }
}
/// Credential flags; omitting both creates a principal without a credential.
#[derive(Args, Default)]
pub struct CredentialOpts {
    /// Existing PostgreSQL SCRAM verifier.
    #[arg(long, conflicts_with = "generate_secret")]
    pub verifier: Option<String>,
    /// Generate a local 32-byte secret.
    #[arg(long)]
    pub generate_secret: bool,
}
impl fmt::Debug for CredentialOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialOpts { [REDACTED] }")
    }
}
/// Mutation result with an optional secret that only the CLI prints once.
pub struct Issued<T> {
    /// Public server response.
    pub value: T,
    /// Generated secret, protected on drop.
    pub secret: Option<Zeroizing<String>>,
}
impl<T> fmt::Debug for Issued<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Issued { [REDACTED] }")
    }
}
type Material = (Option<Zeroizing<String>>, Option<Zeroizing<String>>);
fn material(opts: &CredentialOpts) -> Result<Material, AdminError> {
    if opts.generate_secret && opts.verifier.is_some() {
        return Err(AdminError::Invalid("conflicting credential options"));
    }
    if let Some(verifier) = &opts.verifier {
        ScramVerifier::parse(verifier).ok_or(AdminError::Invalid("invalid SCRAM verifier"))?;
        return Ok((Some(Zeroizing::new(verifier.clone())), None));
    }
    if !opts.generate_secret {
        return Ok((None, None));
    }
    let mut bytes = Zeroizing::new([0u8; 32]);
    let mut salt = [0u8; 16];
    getrandom::fill(&mut *bytes).map_err(|_| AdminError::Invalid("randomness unavailable"))?;
    getrandom::fill(&mut salt).map_err(|_| AdminError::Invalid("randomness unavailable"))?;
    let secret = Zeroizing::new(data_encoding::HEXLOWER.encode(&*bytes));
    let verifier =
        Zeroizing::new(ScramVerifier::from_password(&secret, &salt, 4096).to_pg_string());
    Ok((Some(verifier), Some(secret)))
}
fn kind(text: &str) -> Result<PrincipalKind, String> {
    match text {
        "person" => Ok(PrincipalKind::Person),
        "agent" => Ok(PrincipalKind::Agent),
        "service" => Ok(PrincipalKind::Service),
        _ => Err("invalid principal kind".into()),
    }
}
/// Principal lifecycle and authorization commands.
#[derive(Subcommand)]
pub enum PrincipalCommand {
    /// Create an identity with an optional credential.
    Create {
        /// Management connection.
        #[command(flatten)]
        connection: ConnectionOpts,
        /// New principal name.
        name: String,
        /// Identity kind.
        #[arg(long, value_parser = kind)]
        kind: PrincipalKind,
        /// Grant administrator authority.
        #[arg(long)]
        admin: bool,
        /// Credential input.
        #[command(flatten)]
        credential: CredentialOpts,
    },
    /// List principal summaries.
    List(ConnectionOpts),
    /// Show public principal details.
    Show(TargetOpts),
    /// Disable an identity and end its sessions.
    Disable(TargetOpts),
    /// Enable an identity.
    Enable(TargetOpts),
    /// Manage principal credentials.
    Credential {
        /// Credential operation.
        #[command(subcommand)]
        command: CredentialCommand,
    },
    /// Replace all route grants.
    Grant {
        /// Principal and connection.
        #[command(flatten)]
        target: TargetOpts,
        /// Route and maximum mode, repeated as needed.
        #[arg(long, required_unless_present = "none", conflicts_with = "none")]
        route: Vec<String>,
        /// Remove every grant.
        #[arg(long)]
        none: bool,
    },
}
/// Principal name or hex identifier plus management connection.
#[derive(Args)]
pub struct TargetOpts {
    /// Management connection.
    #[command(flatten)]
    pub connection: ConnectionOpts,
    /// Principal name or 32-hex identifier.
    pub principal_target: String,
}
/// Principal credential operations.
#[derive(Subcommand)]
pub enum CredentialCommand {
    /// Add a verifier or generate a new secret.
    Add {
        /// Principal and connection.
        #[command(flatten)]
        target: TargetOpts,
        /// Credential input.
        #[command(flatten)]
        credential: CredentialOpts,
        /// Optional expiration duration.
        #[arg(long)]
        expires: Option<String>,
    },
    /// Delete a credential.
    Delete {
        /// Principal and connection.
        #[command(flatten)]
        target: TargetOpts,
        /// Credential hex identifier.
        credential_id: String,
    },
}
/// Connection inspection commands.
#[derive(Subcommand)]
pub enum ConnectionsCommand {
    /// List active connections.
    List(ConnectionOpts),
}
/// Resolve a name through the principal list, or normalize a hex identifier.
pub async fn resolve(client: &MgmtClient, principal: &str) -> Result<String, AdminError> {
    if let Some(id) = PrincipalId::from_hex(&principal.to_ascii_lowercase()) {
        return Ok(id.to_hex());
    }
    client
        .principals()
        .await?
        .into_iter()
        .find(|p| p.name == principal)
        .map(|p| p.id)
        .ok_or(AdminError::Invalid("principal not found"))
}
/// Create an identity, sending only a validated or locally derived verifier.
pub async fn create(
    client: &MgmtClient,
    name: &str,
    kind: PrincipalKind,
    admin: bool,
    credential: &CredentialOpts,
) -> Result<Issued<PrincipalView>, AdminError> {
    let (verifier, secret) = material(credential)?;
    let value = client
        .create_principal(name, kind, admin, verifier.as_deref().map(|s| s.as_str()))
        .await?;
    Ok(Issued { value, secret })
}
/// List public principal summaries.
pub async fn list(client: &MgmtClient) -> Result<Vec<PrincipalView>, AdminError> {
    Ok(client.principals().await?)
}
/// Read public principal details by name or id.
pub async fn show(client: &MgmtClient, principal: &str) -> Result<PrincipalView, AdminError> {
    Ok(client.principal(&resolve(client, principal).await?).await?)
}
/// Disable a principal by name or id.
pub async fn disable(client: &MgmtClient, principal: &str) -> Result<PrincipalView, AdminError> {
    Ok(client.disable(&resolve(client, principal).await?).await?)
}
/// Enable a principal by name or id.
pub async fn enable(client: &MgmtClient, principal: &str) -> Result<PrincipalView, AdminError> {
    Ok(client.enable(&resolve(client, principal).await?).await?)
}
/// Replace all route grants, rejecting duplicates before making any request.
pub async fn grant(
    client: &MgmtClient,
    principal: &str,
    routes: &[String],
) -> Result<PrincipalView, AdminError> {
    let mut seen = BTreeSet::new();
    let mut grants = Vec::new();
    for spec in routes {
        let (route, mode) = spec
            .split_once('=')
            .ok_or(AdminError::Invalid("invalid route grant"))?;
        if RouteId::new(route).is_none() {
            return Err(AdminError::Invalid("invalid route grant"));
        }
        if !seen.insert(route) {
            return Err(AdminError::Invalid("duplicate route grant"));
        }
        let mode = match mode {
            "read-only" => Mode::ReadOnly,
            "read-write" => Mode::ReadWrite,
            _ => return Err(AdminError::Invalid("invalid route grant")),
        };
        grants.push((route, mode));
    }
    Ok(client
        .set_grants(&resolve(client, principal).await?, &grants)
        .await?)
}
/// Add a credential, returning any generated secret without printing it.
pub async fn credential_add(
    client: &MgmtClient,
    principal: &str,
    credential: &CredentialOpts,
    expires: Option<&str>,
) -> Result<Issued<CredentialSummary>, AdminError> {
    if expires.is_some_and(|s| crate::config::parse_duration(s).is_none()) {
        return Err(AdminError::Invalid("invalid credential duration"));
    }
    let (verifier, secret) = material(credential)?;
    let verifier = verifier.ok_or(AdminError::Invalid("credential verifier required"))?;
    let value = client
        .add_credential(&resolve(client, principal).await?, &verifier, expires)
        .await?;
    Ok(Issued { value, secret })
}
/// Delete a credential by principal name or id.
pub async fn credential_delete(
    client: &MgmtClient,
    principal: &str,
    credential: &str,
) -> Result<CredentialSummary, AdminError> {
    let cid = CredentialId::from_hex(&credential.to_ascii_lowercase())
        .ok_or(AdminError::Invalid("invalid credential identifier"))?;
    Ok(client
        .delete_credential(&resolve(client, principal).await?, &cid.to_hex())
        .await?)
}
/// List active connection rows.
pub async fn connections_list(client: &MgmtClient) -> Result<Vec<ConnectionView>, AdminError> {
    Ok(client.connections().await?)
}
fn print_secret(secret: Option<Zeroizing<String>>) {
    if let Some(secret) = secret {
        println!("{}", secret.as_str());
        eprintln!("Save this secret now; it cannot be shown again.");
    }
}
fn print_principal(p: &PrincipalView) {
    println!(
        "{}\t{}\t{:?}\t{:?}\t{:?}\t{}",
        p.id, p.name, p.kind, p.state, p.source, p.administrator
    );
}
async fn dispatch(command: PrincipalCommand) -> Result<(), AdminError> {
    match command {
        PrincipalCommand::Create {
            connection,
            name,
            kind,
            admin,
            credential,
        } => {
            let issued = create(&connection.client()?, &name, kind, admin, &credential).await?;
            println!("{}", issued.value.id);
            print_secret(issued.secret);
        }
        PrincipalCommand::List(connection) => {
            for p in list(&connection.client()?).await? {
                print_principal(&p);
            }
        }
        PrincipalCommand::Show(target) => {
            let p = show(&target.connection.client()?, &target.principal_target).await?;
            print_principal(&p);
            println!("revision\t{}", p.revision);
            for g in p.grants {
                println!("grant\t{}\t{:?}", g.route, g.mode);
            }
            for c in p.credentials {
                println!(
                    "credential\t{}\t{:?}\t{}",
                    c.id,
                    c.state,
                    c.expires_ms
                        .map(|ms| ms.to_string())
                        .unwrap_or_else(|| "none".into())
                );
            }
        }
        PrincipalCommand::Disable(target) => {
            print_principal(
                &disable(&target.connection.client()?, &target.principal_target).await?,
            );
        }
        PrincipalCommand::Enable(target) => {
            print_principal(&enable(&target.connection.client()?, &target.principal_target).await?);
        }
        PrincipalCommand::Grant {
            target,
            route,
            none: _,
        } => {
            print_principal(
                &grant(
                    &target.connection.client()?,
                    &target.principal_target,
                    &route,
                )
                .await?,
            );
        }
        PrincipalCommand::Credential { command } => match command {
            CredentialCommand::Add {
                target,
                credential,
                expires,
            } => {
                let issued = credential_add(
                    &target.connection.client()?,
                    &target.principal_target,
                    &credential,
                    expires.as_deref(),
                )
                .await?;
                println!("{}", issued.value.id);
                print_secret(issued.secret);
            }
            CredentialCommand::Delete {
                target,
                credential_id,
            } => {
                println!(
                    "{}",
                    credential_delete(
                        &target.connection.client()?,
                        &target.principal_target,
                        &credential_id
                    )
                    .await?
                    .id
                );
            }
        },
    }
    Ok(())
}
fn runtime(future: impl std::future::Future<Output = Result<(), AdminError>>) -> i32 {
    let result = tokio::runtime::Runtime::new()
        .map_err(|_| AdminError::Invalid("runtime unavailable"))
        .and_then(|rt| rt.block_on(future));
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}
/// Execute principal administration in a dedicated runtime.
pub fn run_command(command: PrincipalCommand) -> i32 {
    runtime(dispatch(command))
}
/// Execute connection inspection in a dedicated runtime.
pub fn run_connections(command: ConnectionsCommand) -> i32 {
    runtime(async {
        match command {
            ConnectionsCommand::List(connection) => {
                for c in connections_list(&connection.client()?).await? {
                    println!("{}\t{}\t{}\t{}", c.id, c.session, c.principal, c.route);
                }
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_material_is_valid_and_diagnostics_redact_it() {
        let opts = CredentialOpts {
            verifier: None,
            generate_secret: true,
        };
        let (verifier, secret) = material(&opts).unwrap();
        let secret = secret.unwrap();
        let verifier = verifier.unwrap();
        let parsed = ScramVerifier::parse(&verifier).unwrap();
        assert_eq!(secret.len(), 64);
        assert_eq!(parsed.salt.len(), 16);
        assert_eq!(parsed.iterations, 4096);
        let derived = ScramVerifier::from_password(&secret, &parsed.salt, parsed.iterations);
        assert!(parsed.stored_key == derived.stored_key);
        assert!(parsed.server_key == derived.server_key);
        let issued = Issued {
            value: (),
            secret: Some(secret),
        };
        let debug = format!("{issued:?} {opts:?}");
        assert!(!debug.contains(issued.secret.as_ref().unwrap().as_str()));
        assert!(!debug.contains(verifier.as_str()));
    }
    #[test]
    fn credential_inputs_fail_with_fixed_diagnostics() {
        let opts = CredentialOpts {
            verifier: Some("private-invalid-verifier".into()),
            generate_secret: false,
        };
        assert_eq!(
            material(&opts).unwrap_err().to_string(),
            "invalid SCRAM verifier"
        );
        let opts = CredentialOpts {
            generate_secret: true,
            ..opts
        };
        assert_eq!(
            material(&opts).unwrap_err().to_string(),
            "conflicting credential options"
        );
        assert!(!format!("{opts:?}").contains("private-invalid-verifier"));
    }
}
