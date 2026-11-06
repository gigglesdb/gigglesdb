# Project memory

## Implementation delegation

All implementation work must be performed by SOL subagents. Use the SOL model
family (currently `gpt-6.1-sol`) when assigning implementation tasks. The primary
agent coordinates work, investigates, reviews changes, and reports results.

This is a standing project instruction from the user, recorded on 2026-11-06.

## Archdocs space

All GigglesDB design docs and related architecture documents must be bound to
the GigglesDB space: 5714972c-8f62-4f17-8ebc-b62fbf4420b9.
Pass this spaceId explicitly when creating documents and verify the saved
spaceId afterward. Never leave GigglesDB documents in My Docs. Include this
requirement and spaceId in every coordinator and worker brief that may write
to Archdocs. This applies to documents created by any agent or thread.

Standing user instruction, recorded on 2026-11-06.
