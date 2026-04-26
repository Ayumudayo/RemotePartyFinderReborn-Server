# Remote Party Finder Reborn - Server

A Rust backend server that aggregates Party Finder listings, integrates with FFLogs, and serves a modern web interface.

## Disclaimer

This is a fork of the [original Remote Party Finder project](https://github.com/zeroeightysix/remote-party-finder)'s server part by zeroeightysix.

**Note**: This project is a proof of concept created for personal interest. There are no plans for a public release.

## Key Features

- **Web Interface**: A responsive, clean web UI to view current Party Finder listings.
- **FFLogs Integration**: Receive parse data (Best Perfs) from clients and cache for display party members' Best Perfs.
- **Real-time Updates**: Receives data from connected Dalamud plugins.

## Architecture

The server is built with:
- **Language**: Rust
- **Web Framework**: Warp
- **Database**: MongoDB
- **Templating**: Askama (HTML templates)

## Setup & Usage

### Prerequisites

- **Rust** (Stable toolchain)
- **MongoDB** (Running instance)

### Configuration

1.  Copy `config.example.toml` to `config.toml`.
2.  Edit `config.toml` with your environment details:

    ```toml
    [web]

    # Network
    host = "127.0.0.1:8000"

    # FFLogs job scheduling
    fflogs_jobs_limit = 20
    fflogs_hidden_cache_ttl_hours = 24

    # Console monitor snapshot (seconds, 0 to disable; default off for low-noise console)
    monitor_snapshot_interval_seconds = 0

    # MongoDB write concurrency
    listing_upsert_concurrency = 16
    player_upsert_concurrency = 32

    # Request body size limits (bytes)
    max_body_bytes_contribute = 262144
    max_body_bytes_multiple = 1048576
    max_body_bytes_players = 524288
    max_body_bytes_detail = 131072
    max_body_bytes_fflogs_results = 524288

    # Batch size limits (items per request)
    max_multiple_batch_size = 200
    max_players_batch_size = 100
    max_fflogs_results_batch_size = 100
    max_detail_member_count = 48

    # Per-client rate limits (requests per minute)
    ingest_rate_limit_contribute_per_minute = 120
    ingest_rate_limit_multiple_per_minute = 60
    ingest_rate_limit_players_per_minute = 120
    ingest_rate_limit_detail_per_minute = 120
    ingest_rate_limit_fflogs_jobs_per_minute = 30
    ingest_rate_limit_fflogs_results_per_minute = 60

    # Signature/replay protection
    # Keep false for public plugin distribution; true is for controlled/private deployments.
    ingest_require_signature = false
    ingest_shared_secret = "rpf-reborn-public-ingest-v1"
    ingest_clock_skew_seconds = 300
    ingest_nonce_ttl_seconds = 300

    # Protected endpoint capabilities
    # Keep false during rollout, then enable after upgraded plugin builds are deployed.
    ingest_require_capabilities_for_protected_endpoints = false
    ingest_capability_secret = ""
    ingest_capability_session_ttl_seconds = 43200
    ingest_capability_detail_ttl_seconds = 900

    [mongo]
    url = "mongodb://localhost:27017/remote-party-finder"
    ```

For signature-enforced operation, start from `config.secure.example.toml` instead.
Set `ingest_shared_secret` to a strong random value and use the same value in the plugin's `IngestSharedSecret`.

Recommended mode for public plugin distribution is `ingest_require_signature = false`.
Shared-secret signatures are suitable for controlled/private deployments only; they are not strong client authentication for openly distributed binaries.
Protected endpoint capabilities are designed for a staged rollout: keep
`ingest_require_capabilities_for_protected_endpoints = false` until updated plugin builds are live, then enable it with a separate `ingest_capability_secret`.

### Materialized Listings Deployment

For low-resource hosting, run the web server in materialized snapshot mode and run
`listings_snapshot_worker` on a separate machine. The worker writes the compressed
snapshot into MongoDB and sends a signed refresh request to the web server.

Use `config.toml` as the single source of truth for both the Koyeb web server
and the Pi-side worker. Avoid mixing deployment environment overrides with file
configuration, because split configuration makes the effective runtime state
harder to audit.

Web server config:

```toml
[web]
host = "0.0.0.0:8000"
listings_snapshot_source = "materialized"
listings_snapshot_document_id = "current"
listing_source_state_document_id = "current"
listings_revision_coalesce_millis = 250
materialized_snapshot_reconcile_interval_seconds = 5
snapshot_refresh_shared_secret = "<same-long-random-secret-as-worker>"
snapshot_refresh_client_id = "listings-snapshot-worker"

[mongo]
url = "<mongodb-connection-string>"
```

Pi-side worker config must use the same MongoDB URL, refresh secret, refresh
client ID, and snapshot document ID. Its refresh URL must point at the web
server's internal refresh endpoint:

```toml
[web]
listings_snapshot_source = "materialized"
listings_snapshot_document_id = "current"
snapshot_refresh_shared_secret = "<same-long-random-secret-as-web-server>"
snapshot_refresh_client_id = "listings-snapshot-worker"

[mongo]
url = "<same-mongodb-connection-string-as-web-server>"

[snapshot_worker]
enabled = true
refresh_url = "https://<web-host>/internal/listings/snapshot/refresh"
tick_seconds = 1
force_rebuild_interval_seconds = 300
lease_ttl_seconds = 120
owner_id = "rpi-listings-snapshot-worker"
```

### Recommended Presets

Tune these four values together based on server capacity and contributor count:

| Preset | `fflogs_jobs_limit` | `fflogs_hidden_cache_ttl_hours` | `listing_upsert_concurrency` | `player_upsert_concurrency` |
| --- | ---: | ---: | ---: | ---: |
| Low-load / single-node | 10 | 24 | 8 | 16 |
| Balanced (default) | 20 | 24 | 16 | 32 |
| High-throughput | 30 | 12 | 24 | 48 |

- Higher `fflogs_jobs_limit` increases refresh speed but raises FFLogs/API pressure.
- Lower `fflogs_hidden_cache_ttl_hours` re-checks hidden characters sooner but increases query volume.
- `monitor_snapshot_interval_seconds` controls periodic one-line `[MONITOR]` logs for Koyeb console observability (default `0` = off).
- Increase upsert concurrency only if MongoDB has headroom; otherwise keep defaults.
- `ingest_require_signature = true` enables HMAC+timestamp+nonce verification for `/contribute*` requests.
- `ingest_require_capabilities_for_protected_endpoints = true` requires server-issued `X-RPF-Capability` tokens for `/contribute/detail` and `/contribute/fflogs/*`.
- `ingest_capability_session_ttl_seconds` controls the FFLogs worker token lifetime; `ingest_capability_detail_ttl_seconds` controls per-listing detail upload lifetime.
- `ingest_rate_limit_*` limits are keyed by remote IP when available (fallback: `X-RPF-Client-Id`) and return `429` with `Retry-After`.
- Restart the server after editing `config.toml`.

### Legacy `account_id` Migration

`players.account_id` is stored as `String` by design. If legacy documents still contain numeric values, the server can now read both formats, but you should normalize old records once.

Run this in Mongo shell:

```javascript
db.players.updateMany(
  { account_id: { $type: ["int", "long", "double", "decimal"] } },
  [
    {
      $set: {
        account_id: { $toString: "$account_id" }
      }
    }
  ]
)
```

### Running the Server

```bash
# Debug run
cargo run

# Release build and run
cargo run --release
```

The server listens on `http://127.0.0.1:8000` by default.

### Validation

```bash
# Type check on stable
CARGO_INCREMENTAL=0 cargo +stable check

# Targeted regression tests used by recent listings/member fixes
CARGO_INCREMENTAL=0 cargo +stable test resolve_member_player
```

### Docker

`Dockerfile` uses a stable Rust builder image (`rust:bookworm`).

## License

No open-source license has been declared for this repository at this time.

Please read `LEGAL_NOTICE.md` for provenance and policy details.
