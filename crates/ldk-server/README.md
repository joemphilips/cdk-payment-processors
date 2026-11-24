# CDK Payment Processor - LDK Server

A CDK payment processor backed by an [LDK Server](https://github.com/lightningdevkit/ldk-server) node. Exposes the node to `cdk-mintd` over the CDK payment processor gRPC protocol, with BOLT11 and BOLT12 (offers) support.

The processor correlates outgoing LDK payment events with CDK melt quote IDs.
Every BOLT11 melt result and event reports the invoice payment hash, which is
the lookup ID of the BOLT11 melt quote. A BOLT12 melt result reports the LDK
payment ID, because a BOLT12 melt quote has no lookup ID before the payment
starts. A status check by payment hash reads the BOLT11 send directly, because
LDK Node uses the payment hash as the payment ID of a BOLT11 send.

LDK Node dispatches a BOLT11 payment before it stores its Pending record. When
`Bolt11Send` fails after the request can have reached LDK Server (a server
error other than "Node is not running", or a transport error other than a
failed connection), the payment can be in flight while the store still holds
the Failed record of an earlier attempt. A `DuplicatePayment` error behind a
Failed or missing record also means that LDK still tracks a send. The
processor then marks the payment hash and reports `Unknown` for it until a
Succeeded record, a Failed record newer than the record before the send, or 14
days after the mark (twice the 1,008-block CLTV limit). A mint keeps such a
melt pending; it does not release the proofs.

The marks are in the file at `ldk.state_path`, so they survive a restart. The
processor marks each BOLT11 send in the file before it dispatches the send, and
removes the mark when the send call gives a definitive result. Thus a call that
the mint cancels during the send keeps its mark. The processor writes the file
atomically. When it cannot write a mark, it stops with exit code 70 before the
send; fix the state directory (space, permissions) and start it again.

A mark ends after 14 days only when LDK Server has a record of the send. A
mark without a record stays: a send whose Pending write failed can have paid,
and LDK Node writes the result only to a record that exists. Such a payment is
`Unknown` until an operator resolves it.

A status check by payment hash reports `Failed` for a hash that LDK Server
has no record of, that has no mark, and that no running send uses, when the
state file is older than 14 days. Every send has a mark before its dispatch,
so such a payment was never sent, for example because LDK Server was down at
the melt. The mint then returns the proofs. While the state file is younger
than 14 days, such a hash is `Unknown`, because a lost earlier file can have
held its mark.

### State file

- Create the file once for a new processor:
  `cdk-payment-processor-ldk-server --init-state`. The command refuses a file
  that exists. The file records its creation time.
- The processor does not start when the file is missing, or when it cannot be
  read or parsed. Never move the file away, delete it, or create a new one to
  start the processor: a lost mark can make the mint release the proofs of a
  paid melt. Restore or repair the file instead.
- To resolve one mark, check the payment with
  `ldk-server-cli get-payment-details <hash>`, and check with `list-channels`
  that no HTLC of it is pending. When the payment did not pay and cannot pay
  any more, stop the processor and run
  `cdk-payment-processor-ldk-server --clear-mark <hash>`. The mint then gets a
  definite result at its next status check.
- A running processor holds an exclusive lock on `<state file>.lock`.
  `--init-state` and `--clear-mark` refuse while it runs, and a second
  processor with the same state file does not start. Do not delete the lock
  file while a processor runs.
- Run one processor for one LDK Server node. A second processor has its own
  marks, so after a double fault it can report a stale Failed record.

When a payment initially returns as pending, a later success event finalizes
the melt and a permanent failure event allows CDK to compensate it. Terminal
events that arrive before the quote ID is known are buffered briefly and
replayed when `make_payment` registers the payment, so the event stream never
sleeps waiting for correlation.

```text
cdk-mintd (payment_backend = "grpcprocessor")
  -> gRPC        cdk-payment-processor-ldk-server (this crate)
  -> gRPC+TLS    ldk-server
```

## Usage

```bash
cp config.toml.example config.toml   # fill in your LDK Server connection details
cargo run --release
```

Point `cdk-mintd` at the processor:

```toml
[payment_backend]
backend = "grpcprocessor"
unit = "msat"

[grpc_processor]
supported_units = ["msat"]
address = "127.0.0.1"
port = 50051
allow_insecure = true
```

CDK 0.18 chooses the URI scheme from `tls_dir`, so `address` must not contain
`http://` or `https://`. Existing mint operators must migrate and initialize
their database-backed configuration before starting 0.18; follow the
[CDK v0.18 migration guide](https://github.com/cashubtc/cdk/blob/main/docs/migrations/v0.18.md).

## Configuration

See [config.toml.example](config.toml.example). A `config.toml` in the current
directory is optional, and environment variables take precedence over its
values.

| `config.toml` key | Environment variable | Default |
| --- | --- | --- |
| `address` | `SERVER_ADDRESS` | `127.0.0.1` |
| `port` | `SERVER_PORT` | `50051` |
| `tls_enable` | `TLS_ENABLE` | `false` |
| `allow_insecure` | `ALLOW_INSECURE` | `false` |
| `tls_cert_path` | `TLS_CERT_PATH` | `certs/server.crt` |
| `tls_key_path` | `TLS_KEY_PATH` | `certs/server.key` |
| `tls_client_ca_path` | `TLS_CLIENT_CA_PATH` | `certs/ca.pem` |
| `ldk.address` | `LDK_ADDRESS` | Required |
| `ldk.macaroon` | `LDK_MACAROON` | This or `ldk.macaroon_path` |
| `ldk.macaroon_path` | `LDK_MACAROON_PATH` | This or `ldk.macaroon` |
| `ldk.state_path` | `LDK_STATE_PATH` | Required |
| `ldk.tls_cert_path` | `LDK_TLS_CERT_PATH` | Required |
| `ldk.fee_reserve_min_sat` | `LDK_FEE_RESERVE_MIN_SAT` | `2` |
| `ldk.fee_reserve_percent` | `LDK_FEE_RESERVE_PERCENT` | `0.01` |
| `ldk.max_payment_scan_pages` | `LDK_MAX_PAYMENT_SCAN_PAGES` | `200` |
| `ldk.payment_methods` | `LDK_PAYMENT_METHODS` | all supported |

Boolean environment variables accept only the literal values `true` and
`false`.

### LDK Server credential

LDK Server authenticates each request with a macaroon. The processor needs only
these permissions: `node:read`, `invoices:create`, `payments:read`,
`payments:send` and `events:read`. Create a separate macaroon for the
processor, so that you can revoke it alone:

```bash
ldk-server-cli create-macaroon cdk-processor \
  --permissions node:read invoices:create payments:read payments:send events:read
```

Put the returned `token` in a file that only the processor can read, and set
`ldk.macaroon_path` to it. `ldk.macaroon` takes the token inline instead. Set
one of them, not both. The processor never prints or serializes the token.
Keep it private. The client binds a
copy of it to each request, so the clocks of the processor and LDK Server must
agree within 60 seconds.

With TLS enabled, `tls_client_ca_path` must contain the CA certificate that
signed the mint's `client.pem`; clients without a trusted certificate are
rejected. Configure the mint's `[grpc_processor].tls_dir` with `ca.pem`,
`client.pem`, and `client.key`.

Without TLS, startup fails unless `allow_insecure = true` (or
`ALLOW_INSECURE=true`) is explicitly configured. The opt-in permits
cleartext on any bind address so it can be used in containers; startup logs a
warning with the effective address and a stronger exposure warning for
non-loopback binds. Configure mutual TLS whenever the network is not fully
trusted.

### Advertised payment methods

A CDK mint selects its backend per `(unit, method)` pair, and registers a
backend for every method that backend advertises in its settings. By default
this processor advertises `bolt11` and `bolt12`, so a mint backed only by it
serves both without extra configuration.

That default makes it the only Lightning backend a mint can have, because a
second backend offering `bolt11` would collide on the same `(unit, method)` pair
and the mint rejects the duplicate. Set `ldk.payment_methods` to advertise a
subset instead, which frees the remaining rails for another backend:

```toml
[ldk]
# Keep bolt12 here and leave bolt11 to another processor.
payment_methods = ["bolt12"]
```

The environment form takes a comma-separated list:

```bash
LDK_PAYMENT_METHODS=bolt12
```

Supported values are `bolt11` and `bolt12`. Names are case-insensitive and
surrounding whitespace is ignored. An unrecognised name fails at startup rather
than being skipped, so a typo cannot silently drop a rail. Leaving the setting
unset, empty, or absent advertises everything.

## Startup self-check

After binding, the processor calls its own `GetSettings` from the local host
(using loopback for unspecified addresses such as `0.0.0.0`) and **exits
non-zero** if it does not answer. This fails fast on port conflicts instead of
looking healthy while another service owns the port.

## Regtest integration tests

The crate ships an opt-in regtest suite that runs the backend and the real
processor binary against two live `ldk-server` daemons (a payer node funding a
channel to the mint-side node) on a regtest `bitcoind`. It covers BOLT11 and
BOLT12 receive/send, quote fee math, held-HTLC pending/failed semantics,
invoice expiry, processor restarts, event streaming, and a full Cashu
mint/melt round trip. It prints progress between scenario groups and applies a
timeout to Cashu melt confirmation so event-delivery regressions fail with a
clear error instead of waiting indefinitely.
Outgoing payment-hash checks also wait within the test timeout for LDK Server's
event handler to populate payment history, which can lag live payment-ID status.

```bash
just test-regtest
```

Prerequisites: `git`, `protoc` (required by the `cdk-signatory` build),
network access, and `bitcoind`. If `BITCOIND_EXE` is unset, bitcoind is
downloaded automatically at build time via `corepc-node`.
The `ldk-server` daemon itself is resolved as follows:

- Set `LDK_SERVER_EXE=/path/to/ldk-server` to use an existing binary.
- Otherwise the upstream repository is cloned into
  `target/ldk-server-src` at the same revision pinned in `Cargo.toml` and
  built once into `target/ldk-server-build/debug/ldk-server`.

Artifacts (daemon logs, configs, mint database) are kept under
`target/ldk-server-regtest/run-<timestamp>/`; set `TEST_DIRECTORY` to change
the root.

## Notes

- Depends on `ldk-server-client` via a git rev; this crate is a binary and is not published to crates.io.
- Battle-tested in production on the Hedwig mint (BOLT11 mint/melt, BOLT12 mint quotes, mainnet). Originally developed at [vincenzopalazzo/cdk-ldk-server-processor](https://github.com/vincenzopalazzo/cdk-ldk-server-processor).

## License

Dual-licensed under the [MIT License](LICENSE-MIT) or the
[Apache License 2.0](LICENSE-APACHE), at your option. See
[LICENSE.md](LICENSE.md).
