# PostgreSQL 18.6 collation oracle

`oracle.json` contains the 700 decoded U& literals from gen.sql, all 871 ICU
O records from orders.txt, the 24 ICU option P/O/G records from options.txt,
the three standard-form NOTICE bodies, and all 871 ICU pg_collation versions.
Non-ICU controls are intentionally excluded. Original capture SHA-256 values
are embedded. Numeric ordering lists stay as CSV strings to keep this compact.
Regenerate with `python tests/prepare_fixture.py <capture-directory>` from the
crate. Tests have no dependency on the original capture or a J: drive.
