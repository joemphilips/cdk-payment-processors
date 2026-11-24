# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Updated all CDK dependencies from `0.18.1` to `0.18.2`.
- Ported the backend to LDK Server `d7e7a2e`: macaroon authentication replaces
  the HMAC API key (`ldk.macaroon` / `LDK_MACAROON` replaces `ldk.api_key` /
  `LDK_API_KEY`), page tokens are strings, payments carry `payment_id`, and the
  new channel and splice events are ignored. The regtest suite gives the
  processor a macaroon with only the permissions that it needs.
- `ldk.macaroon_path` (`LDK_MACAROON_PATH`) reads the macaroon from a file.
  It is mutually exclusive with `ldk.macaroon`. The debug output and the
  serialized configuration no longer contain the macaroon.
- bitCaster fork only: a `[patch.crates-io]` section builds the crate against
  the bitCaster CDK fork `fee0f9d4`. Remove it before an upstream pull request.

### Fixed

- BOLT11 `make_payment` results and outgoing success events now report the
  invoice payment hash, the lookup ID of the BOLT11 melt quote, instead of the
  LDK payment ID. A mint that requires the lookup ID of the quote no longer
  rejects every BOLT11 melt or drops its success event. BOLT12 results keep the
  LDK payment ID.
- An outgoing status check by payment hash reads the BOLT11 send directly
  before it scans the payment history.
- A melt quote that is melted again after a failed attempt gets the events of
  the new attempt. Before, the quote stayed finalized for 24 hours, so the new
  attempt registered no context and its success event was dropped. A failure
  event that is not newer than the stored record before the attempt belongs
  to an earlier attempt and is dropped.
- A BOLT11 `Bolt11Send` that fails with a server or transport error does not
  prove that nothing was dispatched: LDK Node dispatches before it stores the
  Pending record. While such a send is uncertain, a status check reports
  `Unknown` instead of an older Failed record. A Succeeded record, a newer
  Failed record or 14 days end the uncertainty. After a restart, a Failed
  BOLT11 record from before the start is `Unknown` for 14 days, unless this
  process sent the payment again.
- An incoming status check by payment hash ends at the newest match. A scan
  that reaches `ldk.max_payment_scan_pages` returns the matches that it found
  and fails only when it found none. Before, a node with more than 1,600
  payments failed every incoming status check, so a deposit that the event
  stream missed was not credited. The default cap is now 200 pages (10,000
  payments).
- Fixed intermittent `Payment not found` failures in the direct BOLT11 send and
  gRPC melt regtest scenarios by waiting for payment history to catch up with
  live payment status before asserting hash-based results.

## [0.1.0] - 2026-09-02

### Added

- `ldk.payment_methods` (`LDK_PAYMENT_METHODS`) to restrict which payment
  methods the processor advertises, so it can run alongside another payment
  backend instead of claiming every rail. Defaults to advertising everything.

- A crate-local `Cargo.lock` and locked development commands for reproducible
  standalone builds.
- An opt-in regtest integration suite (`--features regtest-tests`,
  `just test-regtest`) that runs the backend and the processor binary against
  two real `ldk-server` daemons on a regtest `bitcoind`, covering BOLT11 and
  BOLT12 receive/send, held-HTLC failure semantics, invoice expiry,
  processor restarts, and a full Cashu mint/melt round trip. The daemon
  binary is taken from `LDK_SERVER_EXE` when set, otherwise it is cloned and
  built from the same upstream revision pinned in `Cargo.toml`.
- A `lib.rs` target exposing the `backend`, `error`, and `settings` modules
  so integration tests can exercise the backend directly. The binary now
  consumes the library instead of declaring the modules itself.

### Changed

- Renamed the backend-specific `config.toml` section from `[backend]` to
  `[ldk]`.
- Updated the CDK integration to `0.18.0` and payment-processor protocol
  4.0.0, including BOLT12 description capability advertisement and
  scheme-free client hosts.
- TLS mode now authenticates mint clients with `tls_client_ca_path`.
- Updated the documented `cdk-mintd` configuration for the 0.18
  database-authoritative payment-backend model.

- Replaced the `CDK_LDK_*` environment namespace with shared `SERVER_*`,
  `TLS_*`, and `ALLOW_INSECURE` variables plus backend-specific `LDK_*`
  variables. Environment values are now applied through Figment providers,
  and booleans accept only `true` or `false`.

### Fixed

- Updated the opt-in regtest backend configuration for the advertised payment
  methods setting.
- Outgoing LDK payment success and failure events are now correlated with CDK
  melt quote IDs, allowing pending melts to finalize or compensate instead of
  waiting indefinitely. The regtest suite also reports scenario progress and
  bounds Cashu melt confirmation waits.
- Unmatched outgoing LDK payment events are buffered and joined when the melt
  quote ID is registered, instead of polling the event stream. This keeps
  incoming payment notifications moving if a success/failure arrives before
  `make_payment` stores its correlation keys.
- The payment processor now fails closed without TLS unless plaintext is
  explicitly enabled with `allow_insecure`, warning more strongly when the
  effective bind address is not loopback.

[Unreleased]: https://github.com/cashubtc/cdk-payment-processors/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cashubtc/cdk-payment-processors/releases/tag/v0.1.0
