#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$1"
ARCHIVE_PATH="$2"

for profile in "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.bashrc"; do
  if [[ -f "$profile" ]]; then
    # shellcheck disable=SC1090
    source "$profile"
  fi
done

if [[ -d "$HOME/.nvm/versions/node" ]]; then
  latest_node_bin="$(find "$HOME/.nvm/versions/node" -path '*/bin/node' -printf '%h\n' 2>/dev/null | sort | tail -n 1 || true)"
  if [[ -n "$latest_node_bin" ]]; then
    export PATH="$latest_node_bin:$PATH"
  fi
fi

if ! command -v pm2 >/dev/null 2>&1; then
  if [[ -d "$HOME/.nvm/versions/node" ]]; then
    latest_pm2_dir="$(find "$HOME/.nvm/versions/node" -path '*/bin/pm2' -printf '%h\n' 2>/dev/null | sort | tail -n 1 || true)"
    if [[ -n "$latest_pm2_dir" ]]; then
      export PATH="$latest_pm2_dir:$PATH"
    fi
  fi
fi

mkdir -p "$APP_DIR" "$APP_DIR/scripts" "$APP_DIR/logs"
tar -C "$APP_DIR" -xf "$ARCHIVE_PATH"
rm -f "$ARCHIVE_PATH"

chmod +x "$APP_DIR"/scripts/*.sh "$APP_DIR"/listings_snapshot_worker

actual_config="$APP_DIR/config.toml"
example_config="$APP_DIR/config.example.toml"
if [[ ! -f "$actual_config" && -f "$example_config" ]]; then
  cp "$example_config" "$actual_config"
  echo "Created $actual_config from $example_config. Fill in real values and rerun deployment." >&2
  exit 1
fi

if [[ ! -f "$actual_config" ]]; then
  echo "Missing $actual_config and no config.example.toml was bundled." >&2
  exit 1
fi

if ! command -v pm2 >/dev/null 2>&1; then
  echo "PATH=$PATH" >&2
  echo "pm2 is not installed on the target host." >&2
  exit 1
fi

cd "$APP_DIR"

echo "Using pm2 from: $(command -v pm2)"
echo "Using node from: $(command -v node || echo missing)"

if ! pm2 startOrReload ecosystem.snapshot-worker.config.js --update-env; then
  echo "pm2 startOrReload failed. Dumping pm2 status and recent logs..." >&2
  pm2 status || true
  pm2 logs rpf-listings-snapshot-worker --lines 80 --nostream || true
  exit 1
fi

if ! pm2 save >/dev/null 2>&1; then
  echo "Warning: pm2 save failed. Processes were restarted, but the PM2 process list was not saved." >&2
fi
