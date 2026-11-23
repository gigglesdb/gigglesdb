# Native wire protocol

`giggles-server` now serves the native API over a framed binary protocol, the
native wire. It runs the same requests as native HTTP and gRPC, with the same
certificate bindings, task accounting and admission limits, at much lower
server cost per request. The full specification is the
[native wire protocol reference](../reference/native-wire-protocol.md).

## What it is

- **One session per connection.** A client signs in once with `HELLO` and
  keeps the session for the life of the connection. It never sends a session
  token per request.
- **Protocol Buffers in 12-byte-header frames**, using the `giggles.v1`
  messages in [giggles.proto](../../crates/giggles-server/proto/giggles/v1/giggles.proto).
- **Pipelining.** A client can send up to 64 requests before reading their
  replies. The server runs them in order, one at a time, and matches each
  reply to its request ID.
- **Prepared handles.** `PREPARE` compiles a statement once, and `RUN`
  executes it by handle. Result column shapes are sent once and then
  referenced by ID.
- **Batches.** One `BATCH` frame carries up to 1,024 requests.
- **`CANCEL` and `GOODBYE`** control frames, which the server reads even while
  a request is running.

## Enable it

| Config key | Listener | Transport and sign-in |
| --- | --- | --- |
| `wire` | Ordinary | TLS with a required client certificate, mapped through the certificate bindings |
| `agent.wire` | Agent | TLS with a certificate, with an optional task selector |
| `role_api.wire` | SQL role | Set by `role_api.wire_tls`: `required` (TLS), `disabled` (plain TCP) or `optional` (both on one port). Plain connections sign in with SCRAM-SHA-256 or configured loopback trust. |

For example, added to an existing configuration (the `role_api` section also
needs its usual `tls` key, and `certificate` and `private_key` for TLS):

```json
{
  "wire": "127.0.0.1:7432",
  "role_api": { "wire": "127.0.0.1:7440", "wire_tls": "optional" }
}
```

The `giggles-client` binary that ships with `giggles-server` uses the native
wire. See the [server guide](../../crates/giggles-server/README.md#configured-mtls-service).

## Changes to existing configurations

- **`native` is a deprecated alias for `wire`.** A configuration that sets only
  `native` still starts the listener on that address and prints a deprecation
  warning. Setting both `native` and `wire` is a startup error.
- **The JSON-frame protocol on that port has been removed.** Clients that sent
  length-prefixed JSON requests to the `native` address must switch to the
  native wire, HTTP or gRPC.
- **A server bound to a non-loopback address in `disabled` or `optional` mode**
  prints a startup warning that plaintext sessions cross the network in clear.

## Performance

Measured on release builds with paired runs (HTTP and the native wire took
turns inside each round, 4 rounds per case), 32 clients, mTLS:

| Workload | Server CPU per request, HTTP -> wire | Throughput |
| --- | --- | --- |
| Primary-key read, 1 request in flight per client | -31% | about 3x |
| Primary-key read, 16 in flight per client | -65% | about 7x |
| Single-row insert, 1 in flight | -11% | about 10% lower |
| Single-row insert, 16 in flight | -12% | +14% |
| App transaction (6 requests, using handles and batches), 1 in flight | -31% | about 2x |
| App transaction, 16 in flight | -53% | about 4x |

Most of the saving is in the transport (socket, framing, TLS, encoding) and
in the hand-offs between threads. The machine was running other work during
these runs, so treat the percentages as the result, not the absolute times.

## Choosing a transport

- **Native wire:** the default for services that call GigglesDB often. It
  costs the least server CPU per request, and pipelining, handles and batches
  cut round trips further.
- **HTTP/JSON:** for clients that can't hold a long-lived connection or can't
  generate Protocol Buffers code, and for occasional calls.
- **gRPC:** for clients that already use gRPC tooling.
- **PostgreSQL wire:** for SQL drivers, ORMs and tools such as psql.

Things to know when you pipeline on the native wire:
- One connection runs its requests in order. With 16 inserts queued on each
  connection, the last waits for the 15 ahead of it, so insert p99 latency
  measured 244 ms on the wire against 40 ms over HTTP. Spread latency-sensitive
  writes across connections, or keep pipelines short for them.
- Single-row inserts sent one at a time complete about 10% fewer per second
  than over HTTP, while using less CPU per insert. With 16 in flight per
  connection, insert throughput measured 14% higher than HTTP.
- On `role_api.wire`, plain TCP used about 16% more server CPU per read than TLS
  at 16 requests in flight, and the same at 1 in flight.
- Keep reading replies while you send. A client that stops reading is paused
  at 16 MiB of pending replies and is disconnected when a write times out.
