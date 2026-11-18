# giggles-icu

Pinned ICU4C collation and case-mapping support for GigglesDB and Rust adapters.

**Status:** Requires ICU4C 76.1 and its data; persisted sort keys must be rebuilt when ICU changes.

Part of [GigglesDB](../../README.md).

## Usage

Provision the pinned runtime with the [ICU scripts](../../scripts/icu). Set
`GIGGLES_ICU_DIR` to its library directory, or use the provisioning cache
(`GIGGLES_ICU_CACHE`). `Icu::load_from` selects a directory explicitly.
`Collator::open` opens a locale; comparison and sort-key APIs use that collator.

## Limits

UTF-8 comparison and sort keys require valid UTF-8; invalid input panics.
Sort keys include ICU's terminating zero. Keep the runtime and its data with
persisted keys. See [the library](src/lib.rs) for loading and locale APIs.
