//! Declarative analysis of static full-data voting memberships.
//!
//! No report proves actual availability, independence, durability or recovery time.
//! All placement, reachability and capacity inputs are caller declarations.
//! Runtime adapters should construct a [`Topology`], call [`Validated::new`], and
//! retain the exact exported snapshot alongside its [`Report`].
//!
//! ```
//! use giggles_topology::majority;
//! assert_eq!(majority(5), Some(3));
//! assert_eq!(majority(0), None);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Maximum UTF-8 JSON input size, also enforced for typed input after validation.
pub const MAX_BYTES: usize = 1_048_576;
/// Maximum entries in each top-level collection (and group membership).
pub const MAX_ITEMS: usize = 256;
/// Maximum dependency references, summed over the graph.
pub const MAX_EDGES: usize = 2048;
/// Maximum dependency path length, including its leaf.
pub const MAX_DEPTH: usize = 32;
/// Conservative total report-work budget, in estimated bytes, across all scenarios.
pub const MAX_ANALYSIS_COST: usize = 64 * 1024 * 1024;
pub const DISCLAIMER: &str = "Declarative analysis only; not actual availability, independence, durability, RTO/RPO or qualification proof.";

macro_rules! record {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub struct $name { $(pub $field: $ty),* }
    };
}

record!(Topology {
    version: u32,
    providers: Vec<Provider>,
    regions: Vec<Region>,
    zones: Vec<Zone>,
    nodes: Vec<Node>,
    groups: Vec<Group>,
    dependencies: Vec<Dependency>,
    scenarios: Vec<Scenario>,
});
record!(Provider { id: String, independence: Option<String> });
record!(Region {
    id: String,
    provider: String
});
record!(Zone {
    id: String,
    region: String
});
record!(Node {
    id: String,
    zone: String
});

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    FullDataVoter,
    Learner,
}
record!(Member {
    node: String,
    role: Role
});

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Majority,
    ThreeIndependentProviders,
    FiveVoters221,
}

record!(Policy { allowed_regions: Vec<String>, allowed_providers: Vec<String> });
record!(Group {
    id: String,
    members: Vec<Member>,
    profile: Profile,
    policy: Policy,
    requirements: Vec<Requirement>,
    capacity: Option<Capacity>,
});

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    Application,
    Gateway,
    BootstrapDiscovery,
    Auth,
    Kms,
    Dns,
}
pub const CATEGORIES: [Category; 6] = [
    Category::Application,
    Category::Gateway,
    Category::BootstrapDiscovery,
    Category::Auth,
    Category::Kms,
    Category::Dns,
];
record!(Requirement {
    category: Category,
    dependency: String
});

/// Endpoint bindings are AND conditions: loss of any binding loses the endpoint.
/// Independent alternatives must be distinct endpoints under an explicit `any`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expression {
    Endpoint {
        bindings: Vec<Domain>,
        assumptions: String,
    },
    All {
        dependencies: Vec<String>,
    },
    Any {
        dependencies: Vec<String>,
    },
    Unknown {
        reason: String,
    },
}
record!(Dependency {
    id: String,
    expression: Expression
});

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Domain {
    Provider(String),
    Region(String),
    Zone(String),
    Node(String),
}
record!(Scenario { id: String, failed: Vec<Domain> });

// Additive capacity is scoped to this group; allocations and reserve are explicit assumptions.
record!(Capacity {
    unit: String,
    demand: u64,
    measurements: Vec<Measurement>,
    assumptions: String,
});
record!(Measurement {
    node: String,
    available: u64,
    source: String
});

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Error {
    pub code: String,
    pub path: String,
    pub reason: String,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}: {}", self.code, self.path, self.reason)
    }
}
impl std::error::Error for Error {}
fn err(code: &str, path: &str, reason: impl Into<String>) -> Error {
    Error {
        code: code.into(),
        path: path.into(),
        reason: reason.into(),
    }
}
type Result<T> = std::result::Result<T, Error>;

/// Minimal intersecting majority; zero voters has no quorum.
pub fn majority(voters: usize) -> Option<usize> {
    (voters != 0).then_some(voters / 2 + 1)
}

fn bounded(n: usize, max: usize, path: &str) -> Result<()> {
    if n > max {
        return Err(err("limit", path, format!("{n} exceeds {max}")));
    }
    Ok(())
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
}
fn text(s: &str, path: &str) -> Result<()> {
    if s.trim().is_empty() || s.len() > 4096 {
        return Err(err("text", path, "requires 1..4096 nonblank bytes"));
    }
    Ok(())
}
fn ids<'a>(values: impl Iterator<Item = &'a str>, path: &str) -> Result<()> {
    let mut seen = BTreeSet::new();
    for s in values {
        if !identifier(s) {
            return Err(err("id", path, format!("invalid identifier {s:?}")));
        }
        if !seen.insert(s) {
            return Err(err("duplicate", path, format!("duplicate {s}")));
        }
    }
    bounded(seen.len(), MAX_ITEMS, path)
}
fn require(found: bool, path: &str, id: &str) -> Result<()> {
    if !found {
        return Err(err("reference", path, format!("undefined {id}")));
    }
    Ok(())
}

/// Validated canonical static snapshot. Private state prevents mutation after validation.
#[derive(Clone, Debug)]
pub struct Validated {
    topology: Topology,
    /// Longest path first is not required; this order has children before parents.
    dependency_order: Vec<String>,
}

impl Validated {
    /// Parse strict JSON with bounded bytes, serde recursion and graph limits.
    pub fn from_json(input: &[u8]) -> Result<Self> {
        bounded(input.len(), MAX_BYTES, "input")?;
        let topology =
            serde_json::from_slice(input).map_err(|e| err("json", "input", e.to_string()))?;
        Self::new(topology)
    }

    /// Validate typed adapter input using the same rules as JSON input.
    pub fn new(mut t: Topology) -> Result<Self> {
        if t.version != 1 {
            return Err(err("version", "version", "only version 1 supported"));
        }
        ids(t.providers.iter().map(|v| v.id.as_str()), "providers")?;
        ids(t.regions.iter().map(|v| v.id.as_str()), "regions")?;
        ids(t.zones.iter().map(|v| v.id.as_str()), "zones")?;
        ids(t.nodes.iter().map(|v| v.id.as_str()), "nodes")?;
        ids(t.groups.iter().map(|v| v.id.as_str()), "groups")?;
        ids(t.dependencies.iter().map(|v| v.id.as_str()), "dependencies")?;
        ids(t.scenarios.iter().map(|v| v.id.as_str()), "scenarios")?;
        if t.groups.is_empty() {
            return Err(err("membership", "groups", "at least one group required"));
        }
        for p in &t.providers {
            if let Some(i) = &p.independence {
                require(identifier(i), "providers.independence", i)?;
            }
        }
        for r in &t.regions {
            require(
                t.providers.iter().any(|p| p.id == r.provider),
                "regions.provider",
                &r.provider,
            )?;
        }
        for z in &t.zones {
            require(
                t.regions.iter().any(|r| r.id == z.region),
                "zones.region",
                &z.region,
            )?;
        }
        for n in &t.nodes {
            require(
                t.zones.iter().any(|z| z.id == n.zone),
                "nodes.zone",
                &n.zone,
            )?;
        }
        for s in &t.scenarios {
            if s.id == "baseline" || s.id.starts_with("loss:") {
                return Err(err("reserved", "scenarios.id", &s.id));
            }
            validate_domains(&t, &s.failed, "scenarios.failed", false)?;
        }
        for g in &t.groups {
            validate_group(&t, g)?;
        }
        let mut edges = 0usize;
        for d in &t.dependencies {
            match &d.expression {
                Expression::Endpoint {
                    bindings,
                    assumptions,
                } => {
                    validate_domains(&t, bindings, "dependencies.bindings", true)?;
                    text(assumptions, "dependencies.assumptions")?;
                }
                Expression::Unknown { reason } => text(reason, "dependencies.reason")?,
                Expression::All { dependencies } | Expression::Any { dependencies } => {
                    if dependencies.is_empty() {
                        return Err(err("expression", "dependencies", "empty AND/OR"));
                    }
                    ids(
                        dependencies.iter().map(String::as_str),
                        "dependencies.children",
                    )?;
                    edges += dependencies.len();
                    bounded(edges, MAX_EDGES, "dependency_edges")?;
                    for id in dependencies {
                        require(
                            t.dependencies.iter().any(|d| &d.id == id),
                            "dependencies.children",
                            id,
                        )?;
                    }
                }
            }
        }
        let mut order = Vec::new();
        let mut heights = BTreeMap::new();
        let mut active = BTreeSet::new();
        for d in &t.dependencies {
            visit(&t, &d.id, &mut active, &mut heights, &mut order)?;
        }
        canonicalize(&mut t);
        let serialized_len = serde_json::to_vec(&t)
            .map_err(|e| err("json", "topology", e.to_string()))?
            .len();
        bounded(serialized_len, MAX_BYTES, "topology")?;
        // Limit the cross product too: bounded collections alone can still yield
        // hundreds of megabytes of repeated group explanations over all failures.
        // These conservative weights include JSON escaping, IDs and explanations.
        let scenarios = 1 + t.providers.len() + t.regions.len() + t.zones.len() + t.scenarios.len();
        let cost = scenarios * (serialized_len * 8 + t.groups.len() * 128 * 1024);
        bounded(cost, MAX_ANALYSIS_COST, "analysis_cost")?;
        Ok(Self {
            topology: t,
            dependency_order: order,
        })
    }

    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// Canonical, round-trippable JSON. Order-insensitive declarations are sorted.
    pub fn export_json(&self) -> String {
        serde_json::to_string_pretty(&self.topology).expect("validated JSON types serialize")
    }

    /// Baseline, every single zone/region/provider loss, and explicit simultaneous sets.
    pub fn scenarios(&self) -> Vec<Scenario> {
        let t = &self.topology;
        let mut s = vec![Scenario {
            id: "baseline".into(),
            failed: vec![],
        }];
        for p in &t.providers {
            s.push(Scenario {
                id: format!("loss:provider:{}", p.id),
                failed: vec![Domain::Provider(p.id.clone())],
            });
        }
        for r in &t.regions {
            s.push(Scenario {
                id: format!("loss:region:{}", r.id),
                failed: vec![Domain::Region(r.id.clone())],
            });
        }
        for z in &t.zones {
            s.push(Scenario {
                id: format!("loss:zone:{}", z.id),
                failed: vec![Domain::Zone(z.id.clone())],
            });
        }
        s.extend(t.scenarios.clone());
        s.sort_by(|a, b| a.id.cmp(&b.id));
        s
    }

    /// Analyze generated scenarios and all declared groups.
    pub fn analyze(&self) -> Report {
        Report {
            version: 1,
            disclaimer: DISCLAIMER.into(),
            scenarios: self.scenarios().iter().map(|s| self.evaluate(s)).collect(),
        }
    }

    /// Analyze one explicit scenario with the same bounds/reference checks.
    pub fn analyze_scenario(&self, scenario: &Scenario) -> Result<ScenarioReport> {
        require(identifier(&scenario.id), "scenario.id", &scenario.id)?;
        validate_domains(&self.topology, &scenario.failed, "scenario.failed", false)?;
        let mut s = scenario.clone();
        s.failed.sort();
        Ok(self.evaluate(&s))
    }

    fn evaluate(&self, scenario: &Scenario) -> ScenarioReport {
        let t = &self.topology;
        let failed: BTreeSet<_> = scenario.failed.iter().cloned().collect();
        let lost: BTreeSet<_> = t
            .nodes
            .iter()
            .filter(|n| node_lost(t, n, &failed))
            .map(|n| n.id.clone())
            .collect();
        let mut deps: BTreeMap<String, Assessment> = BTreeMap::new();
        for id in &self.dependency_order {
            let d = t
                .dependencies
                .iter()
                .find(|d| &d.id == id)
                .expect("validated dependency");
            let a = match &d.expression {
                Expression::Unknown { reason } => {
                    assessment(Verdict::Unknown, format!("{id}: {reason}"))
                }
                Expression::Endpoint {
                    bindings,
                    assumptions,
                } => {
                    let losses: Vec<_> = bindings
                        .iter()
                        .filter(|d| domain_lost(t, d, &failed, &lost))
                        .cloned()
                        .collect();
                    assessment(
                        if losses.is_empty() {
                            Verdict::Pass
                        } else {
                            Verdict::Fail
                        },
                        format!(
                            "{id}: endpoint lost bindings {}; declared assumption: {assumptions}",
                            serde_json::to_string(&losses).expect("domains serialize")
                        ),
                    )
                }
                Expression::All { dependencies } | Expression::Any { dependencies } => {
                    let all = matches!(d.expression, Expression::All { .. });
                    let children: Vec<_> = dependencies.iter().map(|k| &deps[k]).collect();
                    let mut a = combine(&children, all);
                    a.reasons.insert(
                        0,
                        format!(
                            "{id}: {} alternatives {dependencies:?}",
                            if all { "AND" } else { "OR" }
                        ),
                    );
                    a
                }
            };
            deps.insert(id.clone(), a);
        }
        let groups = t.groups.iter().map(|g| {
            let voters: Vec<_> = g.members.iter().filter(|m| m.role == Role::FullDataVoter).collect();
            let survivors: Vec<_> = voters.iter().filter(|m| !lost.contains(&m.node)).map(|m| m.node.clone()).collect();
            let q = majority(voters.len()).expect("nonzero validated voters");
            let voter_arithmetic = assessment(if survivors.len() >= q { Verdict::Pass } else { Verdict::Fail }, format!("{} of {} full-data voters survive; quorum requires {q}; learners contribute zero votes", survivors.len(), voters.len()));
            let requirements: BTreeMap<_,_> = CATEGORIES.iter().map(|c| {
                let a = g.requirements.iter().find(|r| &r.category == c).map(|r| deps[&r.dependency].clone()).unwrap_or_else(|| assessment(Verdict::Unknown, format!("{c:?}: requirement not declared")));
                (*c, a)
            }).collect();
            let dependency_reachability = combine(&requirements.values().collect::<Vec<_>>(), true);
            GroupReport { id: g.id.clone(), voter_count: voters.len(), quorum: q, surviving_voters: survivors, voter_arithmetic, dependency_reachability, requirements, capacity: capacity(g, &lost) }
        }).collect();
        ScenarioReport {
            id: scenario.id.clone(),
            failed: scenario.failed.clone(),
            lost_nodes: lost.into_iter().collect(),
            dependencies: deps,
            groups,
        }
    }
}

fn validate_domains(t: &Topology, ds: &[Domain], path: &str, nonempty: bool) -> Result<()> {
    bounded(ds.len(), MAX_ITEMS, path)?;
    if nonempty && ds.is_empty() {
        return Err(err(
            "binding",
            path,
            "endpoint requires explicit failure-domain binding",
        ));
    }
    let mut seen = BTreeSet::new();
    for d in ds {
        if !seen.insert(d) {
            return Err(err("duplicate", path, "duplicate domain"));
        }
        let (found, id) = match d {
            Domain::Provider(id) => (t.providers.iter().any(|p| &p.id == id), id),
            Domain::Region(id) => (t.regions.iter().any(|r| &r.id == id), id),
            Domain::Zone(id) => (t.zones.iter().any(|z| &z.id == id), id),
            Domain::Node(id) => (t.nodes.iter().any(|n| &n.id == id), id),
        };
        require(found, path, id)?;
    }
    Ok(())
}

fn locality<'a>(t: &'a Topology, node: &str) -> (&'a Zone, &'a Region, &'a Provider) {
    let n = t
        .nodes
        .iter()
        .find(|n| n.id == node)
        .expect("validated node");
    let z = t
        .zones
        .iter()
        .find(|z| z.id == n.zone)
        .expect("validated zone");
    let r = t
        .regions
        .iter()
        .find(|r| r.id == z.region)
        .expect("validated region");
    let p = t
        .providers
        .iter()
        .find(|p| p.id == r.provider)
        .expect("validated provider");
    (z, r, p)
}

fn validate_group(t: &Topology, g: &Group) -> Result<()> {
    ids(g.members.iter().map(|m| m.node.as_str()), "group.members")?;
    ids(
        g.policy.allowed_regions.iter().map(String::as_str),
        "policy.allowed_regions",
    )?;
    ids(
        g.policy.allowed_providers.iter().map(String::as_str),
        "policy.allowed_providers",
    )?;
    for id in &g.policy.allowed_regions {
        require(
            t.regions.iter().any(|r| &r.id == id),
            "policy.allowed_regions",
            id,
        )?;
    }
    for id in &g.policy.allowed_providers {
        require(
            t.providers.iter().any(|p| &p.id == id),
            "policy.allowed_providers",
            id,
        )?;
    }
    if g.policy.allowed_regions.is_empty() || g.policy.allowed_providers.is_empty() {
        return Err(err(
            "policy",
            &g.id,
            "explicit nonempty residency/provider allowlists required",
        ));
    }
    let mut counts = BTreeMap::new();
    let mut regions = BTreeSet::new();
    let mut zones = BTreeSet::new();
    for m in &g.members {
        require(
            t.nodes.iter().any(|n| n.id == m.node),
            "group.members.node",
            &m.node,
        )?;
        let (z, r, p) = locality(t, &m.node);
        if !g.policy.allowed_regions.contains(&r.id) || !g.policy.allowed_providers.contains(&p.id)
        {
            return Err(err(
                "residency",
                &g.id,
                format!("{} outside policy", m.node),
            ));
        }
        if m.role == Role::FullDataVoter {
            *counts.entry(p.id.clone()).or_insert(0usize) += 1;
            regions.insert(r.id.clone());
            zones.insert(z.id.clone());
        }
    }
    let n: usize = counts.values().sum();
    if n == 0 {
        return Err(err(
            "membership",
            &g.id,
            "at least one full-data voter required",
        ));
    }
    if g.profile != Profile::Majority {
        let mut layout: Vec<_> = counts.values().copied().collect();
        layout.sort();
        let expected = if g.profile == Profile::ThreeIndependentProviders {
            vec![1, 1, 1]
        } else {
            vec![1, 2, 2]
        };
        if layout != expected || zones.len() != n || regions.len() < 3 {
            return Err(err(
                "profile",
                &g.id,
                "requires declared 1/1/1 or 2/2/1 provider layout, unique voter zones and at least three regions",
            ));
        }
        let mut independence = BTreeSet::new();
        for pid in counts.keys() {
            let p = t
                .providers
                .iter()
                .find(|p| &p.id == pid)
                .expect("validated provider");
            let Some(i) = &p.independence else {
                return Err(err(
                    "independence",
                    &g.id,
                    format!("provider {pid} independence unknown"),
                ));
            };
            if !independence.insert(i) {
                return Err(err(
                    "independence",
                    &g.id,
                    "providers share independence identity",
                ));
            }
        }
    }
    bounded(g.requirements.len(), CATEGORIES.len(), "group.requirements")?;
    let mut categories = BTreeSet::new();
    for r in &g.requirements {
        if !categories.insert(r.category) {
            return Err(err("duplicate", &g.id, "duplicate dependency category"));
        }
        require(
            t.dependencies.iter().any(|d| d.id == r.dependency),
            "requirements.dependency",
            &r.dependency,
        )?;
    }
    if let Some(c) = &g.capacity {
        text(&c.unit, "capacity.unit")?;
        text(&c.assumptions, "capacity.assumptions")?;
        ids(
            c.measurements.iter().map(|m| m.node.as_str()),
            "capacity.measurements",
        )?;
        let mut sum = 0u64;
        for m in &c.measurements {
            require(
                g.members.iter().any(|n| n.node == m.node),
                "capacity.node",
                &m.node,
            )?;
            text(&m.source, "capacity.source")?;
            sum = sum
                .checked_add(m.available)
                .ok_or_else(|| err("overflow", "capacity", "total exceeds u64"))?;
        }
    }
    Ok(())
}

fn visit(
    t: &Topology,
    id: &str,
    active: &mut BTreeSet<String>,
    heights: &mut BTreeMap<String, usize>,
    order: &mut Vec<String>,
) -> Result<usize> {
    if let Some(h) = heights.get(id) {
        return Ok(*h);
    }
    if !active.insert(id.into()) {
        return Err(err("cycle", "dependencies", id));
    }
    bounded(active.len(), MAX_DEPTH, "dependency_depth")?;
    let d = t
        .dependencies
        .iter()
        .find(|d| d.id == id)
        .expect("validated reference");
    let mut h = 1;
    if let Expression::All { dependencies } | Expression::Any { dependencies } = &d.expression {
        for child in dependencies {
            h = h.max(visit(t, child, active, heights, order)? + 1);
        }
    }
    bounded(h, MAX_DEPTH, "dependency_depth")?;
    active.remove(id);
    heights.insert(id.into(), h);
    order.push(id.into());
    Ok(h)
}

fn canonicalize(t: &mut Topology) {
    t.providers.sort_by(|a, b| a.id.cmp(&b.id));
    t.regions.sort_by(|a, b| a.id.cmp(&b.id));
    t.zones.sort_by(|a, b| a.id.cmp(&b.id));
    t.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    t.groups.sort_by(|a, b| a.id.cmp(&b.id));
    t.dependencies.sort_by(|a, b| a.id.cmp(&b.id));
    t.scenarios.sort_by(|a, b| a.id.cmp(&b.id));
    for s in &mut t.scenarios {
        s.failed.sort();
    }
    for d in &mut t.dependencies {
        match &mut d.expression {
            Expression::Endpoint { bindings, .. } => bindings.sort(),
            Expression::All { dependencies } | Expression::Any { dependencies } => {
                dependencies.sort()
            }
            Expression::Unknown { .. } => {}
        }
    }
    for g in &mut t.groups {
        g.members.sort_by(|a, b| a.node.cmp(&b.node));
        g.policy.allowed_regions.sort();
        g.policy.allowed_providers.sort();
        g.requirements.sort_by_key(|r| r.category);
        if let Some(c) = &mut g.capacity {
            c.measurements.sort_by(|a, b| a.node.cmp(&b.node));
        }
    }
}

fn node_lost(t: &Topology, n: &Node, failed: &BTreeSet<Domain>) -> bool {
    let (z, r, p) = locality(t, &n.id);
    failed.contains(&Domain::Node(n.id.clone()))
        || failed.contains(&Domain::Zone(z.id.clone()))
        || failed.contains(&Domain::Region(r.id.clone()))
        || failed.contains(&Domain::Provider(p.id.clone()))
}
fn domain_lost(
    t: &Topology,
    d: &Domain,
    failed: &BTreeSet<Domain>,
    lost: &BTreeSet<String>,
) -> bool {
    if failed.contains(d) {
        return true;
    }
    match d {
        Domain::Provider(_) => false,
        Domain::Region(id) => {
            let r = t
                .regions
                .iter()
                .find(|r| &r.id == id)
                .expect("validated region");
            failed.contains(&Domain::Provider(r.provider.clone()))
        }
        Domain::Zone(id) => {
            let z = t
                .zones
                .iter()
                .find(|z| &z.id == id)
                .expect("validated zone");
            domain_lost(t, &Domain::Region(z.region.clone()), failed, lost)
        }
        Domain::Node(id) => lost.contains(id),
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Fail,
    Unknown,
    NotEvaluated,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Assessment {
    pub verdict: Verdict,
    pub reasons: Vec<String>,
}
fn assessment(verdict: Verdict, reason: String) -> Assessment {
    Assessment {
        verdict,
        reasons: vec![reason],
    }
}
fn combine(children: &[&Assessment], all: bool) -> Assessment {
    let decisive = if all { Verdict::Fail } else { Verdict::Pass };
    let fallback = if all { Verdict::Pass } else { Verdict::Fail };
    let verdict = if children.iter().any(|a| a.verdict == decisive) {
        decisive
    } else if children
        .iter()
        .any(|a| matches!(a.verdict, Verdict::Unknown | Verdict::NotEvaluated))
    {
        Verdict::Unknown
    } else {
        fallback
    };
    // Reference child assessments rather than expanding a shared DAG into exponential text.
    Assessment {
        verdict,
        reasons: vec![format!(
            "{} of {} declared inputs => {verdict:?}",
            if all { "AND" } else { "OR" },
            children.len()
        )],
    }
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CapacityReport {
    pub assessment: Assessment,
    pub unit: Option<String>,
    pub demand: Option<u64>,
    pub surviving_available: Option<u64>,
    pub assumptions: Option<String>,
}
fn capacity(g: &Group, lost: &BTreeSet<String>) -> CapacityReport {
    let Some(c) = &g.capacity else {
        return CapacityReport {
            assessment: assessment(Verdict::NotEvaluated, "no supplied capacity model".into()),
            unit: None,
            demand: None,
            surviving_available: None,
            assumptions: None,
        };
    };
    let missing: Vec<_> = g
        .members
        .iter()
        .filter(|m| !lost.contains(&m.node) && !c.measurements.iter().any(|v| v.node == m.node))
        .map(|m| m.node.clone())
        .collect();
    let available = if missing.is_empty() {
        Some(
            c.measurements
                .iter()
                .filter(|m| !lost.contains(&m.node))
                .map(|m| m.available)
                .sum::<u64>(),
        )
    } else {
        None
    };
    let a = match available {
        None => assessment(
            Verdict::NotEvaluated,
            format!("missing surviving member measurements: {missing:?}"),
        ),
        Some(n) => assessment(
            if n >= c.demand {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
            format!(
                "supplied additive survivor capacity {n} {} against demand {}; provenance and allocations are caller declarations",
                c.unit, c.demand
            ),
        ),
    };
    CapacityReport {
        assessment: a,
        unit: Some(c.unit.clone()),
        demand: Some(c.demand),
        surviving_available: available,
        assumptions: Some(c.assumptions.clone()),
    }
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct GroupReport {
    pub id: String,
    pub voter_count: usize,
    pub quorum: usize,
    pub surviving_voters: Vec<String>,
    pub voter_arithmetic: Assessment,
    pub dependency_reachability: Assessment,
    pub requirements: BTreeMap<Category, Assessment>,
    pub capacity: CapacityReport,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ScenarioReport {
    pub id: String,
    pub failed: Vec<Domain>,
    pub lost_nodes: Vec<String>,
    pub dependencies: BTreeMap<String, Assessment>,
    pub groups: Vec<GroupReport>,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Report {
    pub version: u32,
    pub disclaimer: String,
    pub scenarios: Vec<ScenarioReport>,
}

impl Report {
    /// Fail dominates unknown; no aggregate is an availability verdict.
    /// CLI exits: 0 all declared checks pass, 3 known failure, 4 incomplete analysis.
    pub fn exit_code(&self) -> u8 {
        let mut incomplete = false;
        for s in &self.scenarios {
            for g in &s.groups {
                for a in [
                    &g.voter_arithmetic,
                    &g.dependency_reachability,
                    &g.capacity.assessment,
                ] {
                    if a.verdict == Verdict::Fail {
                        return 3;
                    }
                    if matches!(a.verdict, Verdict::Unknown | Verdict::NotEvaluated) {
                        incomplete = true;
                    }
                }
            }
        }
        if incomplete { 4 } else { 0 }
    }
}
