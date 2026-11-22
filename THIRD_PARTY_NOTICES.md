# Third-party notices

GigglesDB's MPL-2.0 license does not replace upstream licenses. Retain the
original notices when redistributing covered components. This index points to
existing notices and provenance; it does not modify them.

| Path | Upstream / component | License |
| --- | --- | --- |
| [crates/giggles-core/THIRD_PARTY_NOTICES.md](crates/giggles-core/THIRD_PARTY_NOTICES.md) | PostgreSQL degree math and WIDE Project MD5 ports | PostgreSQL AND BSD-3-Clause |
| [crates/giggles-core/tests/fixtures/e1_constraints/PROVENANCE.txt](crates/giggles-core/tests/fixtures/e1_constraints/PROVENANCE.txt) | PostgreSQL 18.6 oracle captures | PostgreSQL (upstream); capture provenance |
| [crates/giggles-pg-catalog/src/timezonesets/POSTGRESQL-COPYRIGHT](crates/giggles-pg-catalog/src/timezonesets/POSTGRESQL-COPYRIGHT) | PostgreSQL timezone abbreviation sets | PostgreSQL |
| [crates/giggles-pg-json/upstream/COPYRIGHT](crates/giggles-pg-json/upstream/COPYRIGHT) | PostgreSQL retained reference sources | PostgreSQL |
| [crates/giggles-pg-json/upstream/provenance.json](crates/giggles-pg-json/upstream/provenance.json) | PostgreSQL pinned JSON reference sources (independently written Rust implementation) | PostgreSQL |
| [crates/giggles-pg-types/FLOAT-BOOST-LICENSE](crates/giggles-pg-types/FLOAT-BOOST-LICENSE) | Boost-derived floating-point conversion code | BSL-1.0 |
| [crates/giggles-pg-types/FLOAT-POSTGRESQL-LICENSE](crates/giggles-pg-types/FLOAT-POSTGRESQL-LICENSE) | PostgreSQL floating-point code | PostgreSQL |
| [crates/giggles-pg-types/INTEGER-PG-NOTICE](crates/giggles-pg-types/INTEGER-PG-NOTICE) | PostgreSQL integer conversion code | PostgreSQL |
| [crates/giggles-pg-types/tests/fixtures/POSTGRESQL-COPYRIGHT](crates/giggles-pg-types/tests/fixtures/POSTGRESQL-COPYRIGHT) | PostgreSQL numeric reference sources and fixtures | PostgreSQL |
| [crates/giggles-pg-types/tests/fixtures/NUMERIC-PROVENANCE.txt](crates/giggles-pg-types/tests/fixtures/NUMERIC-PROVENANCE.txt) | PostgreSQL numeric algorithms and oracle fixtures | PostgreSQL |
| [crates/giggles-pg-types/tests/fixtures/NUMERIC-POWER-PROVENANCE.txt](crates/giggles-pg-types/tests/fixtures/NUMERIC-POWER-PROVENANCE.txt) | PostgreSQL numeric power algorithms and oracle fixtures | PostgreSQL |
| [crates/giggles-pg-types/tests/fixtures/TYPMOD-PROVENANCE.txt](crates/giggles-pg-types/tests/fixtures/TYPMOD-PROVENANCE.txt) | PostgreSQL 18.6 typmod oracle captures | PostgreSQL (upstream); capture provenance |
| [crates/giggles-pg-uuid/reference/COPYRIGHT](crates/giggles-pg-uuid/reference/COPYRIGHT) | PostgreSQL UUID reference sources and adapted logic | PostgreSQL |
| [crates/skedaddle/licenses/Miniflux-2.0.3-LICENSE](crates/skedaddle/licenses/Miniflux-2.0.3-LICENSE) | Miniflux 2.0.3 SQL migration fixture | Apache-2.0 |
| [crates/skedaddle/licenses/Miniflux-PROVENANCE.md](crates/skedaddle/licenses/Miniflux-PROVENANCE.md) | Miniflux pinned SQL migration source | Apache-2.0 |
| [scripts/postgres-functions/POSTGRESQL-LICENSE](scripts/postgres-functions/POSTGRESQL-LICENSE) | PostgreSQL function oracle/reference material | PostgreSQL |
| [crates/giggles-pg-types/src/temporal/tzdata/](crates/giggles-pg-types/src/temporal/tzdata/) | IANA Time Zone Database 2026f, via Debian/PostgreSQL; version and hashes in manifest.json | Public-domain tz database data |
| [crates/giggles-glibm/COPYING.LIB](crates/giggles-glibm/COPYING.LIB) | Portable binary64 math routines derived from GNU C Library (glibc) 2.41 (Debian 2.41-12+deb13u4), including Sun Microsystems and IBM routines; source notices retained in each module | LGPL-2.1-or-later; retained Sun Microsystems permission notices also apply |
| [crates/giggles-glibm-loader/](crates/giggles-glibm-loader/) | First-party runtime loader for the giggles-glibm dynamic library | MIT |
| [Unicode case and normalization tables](crates/giggles-core/src/relational/query/functions/unicode_data.rs) | Unicode Character Database and unicode-normalization | [Unicode-3.0](crates/giggles-core/UNICODE-LICENSE); unicode-normalization Rust code MIT OR Apache-2.0 |
| [scripts/icu/](scripts/icu/) | Unicode ICU 76.1 runtime, fetched separately; provision.sh retains upstream LICENSE beside installed libraries | Unicode-3.0; accompanying third-party notices in upstream LICENSE apply |
| [crates/nunya/Cargo.toml](crates/nunya/Cargo.toml) | Optional `kubernetes` feature: kube 5.1 and k8s-openapi 0.29, fetched by Cargo, not vendored; permissive transitive dependencies including webpki-root-certs | Apache-2.0; transitive licenses include CDLA-Permissive-2.0 |

The PostgreSQL oracle captures under `crates/giggles-server/tests/fixtures/dd68/`
and other `tests/fixtures/dd68/` directories retain their existing provenance;
see also [the PostgreSQL license](scripts/postgres-functions/POSTGRESQL-LICENSE).
They are not relicensed by this index.

The filename search also matches
`compat/corpus/pg_routines/pgp_pl_notice.sql` (a first-party RAISE NOTICE test),
`scripts/postgres-functions/capture_provenance.py` and
`scripts/postgres-functions/run_provenance_campaign.py` (first-party capture
tools). These are not third-party license texts.

Cargo dependencies retain their upstream licenses, recorded in their package
metadata and distributed source notices. The ICU runtime is provisioned outside
Cargo; consult its installed LICENSE for the complete bundled notices.

giggles-glibm is built and distributed as a separate dynamic library
(`giggles_glibm.dll`, `libgiggles_glibm.so`, `libgiggles_glibm.dylib`). GigglesDB does not link it:
no GigglesDB crate depends on it in Cargo, and giggles-glibm-loader opens it
at run time. A modified build that exports the same interface and identity
string can replace it. ICU 76.1 is likewise downloaded and loaded at run time rather
than linked.
