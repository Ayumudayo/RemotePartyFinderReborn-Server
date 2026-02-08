# Use a multi-stage build to keep the final image small
# 1. Builder stage (Use Nightly as required by codebase)
FROM rustlang/rust:nightly-bookworm as builder

WORKDIR /usr/src/app

# Copy Cargo.toml and Cargo.lock first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
# This is a common pattern to cache dependencies layer only if they haven't changed
RUN mkdir src && \
    echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source code
COPY src ./src
COPY templates ./templates

# Build the actual application
# We need to touch the main.rs file to trigger a rebuild
RUN touch src/main.rs && cargo build --release

# 2. Runtime stage
FROM debian:bookworm-slim

# Install necessary runtime dependencies (e.g. OpenSSL) if needed
# rust:1.84-slim-bookworm is based on debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/app/target/release/RemotePartyFinder-Reborn ./server

# Copy static assets and templates
COPY assets ./assets
# We don't copy templates here as they are compiled into the binary by Askama, 
# unless we are using dynamic templates which Askama is usually not (it's compile-time).
# However, checking the Cargo.toml, `askama` is used. Standard Askama logic compiles templates.
# Just in case there are runtime assets in templates (unlikely for Askama), we can skip them 
# unless I see evidence they are loaded at runtime.
# Wait, I see `askama = { version = "0.11", features = ["with-warp"] }`.
# Askama compiles templates into the binary, so we don't need the templates folder at runtime.

# Copy config file (or rely on volume mount)
# config.docker.toml serves as a template or default for docker context
COPY config.docker.toml ./config.toml

# Expose the port
EXPOSE 8000

# Run the application
CMD ["./server"]
