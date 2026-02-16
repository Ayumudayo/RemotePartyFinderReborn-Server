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
    host = "127.0.0.1:8000"
    fflogs_jobs_limit = 20
    fflogs_hidden_cache_ttl_hours = 24
    listing_upsert_concurrency = 16
    player_upsert_concurrency = 32

    [mongo]
    url = "mongodb://localhost:27017/remote-party-finder"
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
- Increase upsert concurrency only if MongoDB has headroom; otherwise keep defaults.
- Restart the server after editing `config.toml`.

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

No license specified yet.
Since the original repository also does not have a license set, the license configuration is postponed.
