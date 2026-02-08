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
- **Web Framework**: Axum (implied, or specialized handler setup)
- **Database**: MongoDB
- **Templating**: Askama (HTML templates)

## Setup & Usage

### Prerequisites

- **Rust** (Latest Stable)
- **MongoDB** (Running instance)

### Configuration

1.  Copy `config.example.toml` to `config.toml`.
2.  Edit `config.toml` with your environment details:

    ```toml
    [web]
    host = "127.0.0.1:8000"

    [mongo]
    url = "mongodb://localhost:27017/remote-party-finder"
    ```

### Running the Server

```bash
# Debug run
cargo run

# Release build and run
cargo run --release
```

The server listens on `http://127.0.0.1:8000` by default.

## License

No license specified yet.
Since the original repository also does not have a license set, the license configuration is postponed.
