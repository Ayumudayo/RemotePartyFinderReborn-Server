# Use a multi-stage build to keep the final image small
# 1. Builder stage (stable toolchain)
FROM rust:bookworm AS builder

WORKDIR /usr/src/app

# Copy Cargo.toml and Cargo.lock first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create dummy crate roots to build dependency layers against the current
# lib + bin package layout.
RUN mkdir -p src && \
    echo "pub fn dependency_cache_placeholder() {}" > src/lib.rs && \
    echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source code
COPY src ./src
COPY templates ./templates

# Build the actual application.
# The dependency-cache layer above compiles placeholder crate roots, so after
# copying the real sources we force Cargo to reconsider the library root, bin
# entrypoints, and Askama template inputs instead of only rebuilding main.rs.
RUN touch src/lib.rs src/main.rs && \
    if [ -d src/bin ]; then find src/bin -type f -exec touch {} +; fi && \
    find templates -type f -exec touch {} + && \
    cargo build --release

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
