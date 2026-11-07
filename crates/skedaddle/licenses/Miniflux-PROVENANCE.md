# Miniflux SQL fixture provenance

Copyright 2017 Frédéric Guillot. All rights reserved.
Licensed under Apache License 2.0; see Miniflux-2.0.3-LICENSE.

`artifacts/spikes/s3-ddl/miniflux-upgrade.sql` includes the two CREATE INDEX
statements from Miniflux 2.0.3 sql/schema_version_13.sql at committed revision
73a0a25b6cf9e2b11476c9b6504c69e7d33f4ccc. The GD-0049 fixture modifies the
execution wrapper with search_path, BEGIN/COMMIT, and a schema_version update.
The original retained fixture is unchanged by this curation increment.

Source: https://github.com/miniflux/v2/blob/73a0a25b6cf9e2b11476c9b6504c69e7d33f4ccc/sql/schema_version_13.sql
License: https://github.com/miniflux/v2/blob/73a0a25b6cf9e2b11476c9b6504c69e7d33f4ccc/LICENSE
The upstream tag has no NOTICE file. No other Miniflux migration source
snapshots are retained by this increment.
