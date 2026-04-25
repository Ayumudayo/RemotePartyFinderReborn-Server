module.exports = {
  apps: [
    {
      name: "rpf-listings-snapshot-worker",
      cwd: __dirname,
      script: "./listings_snapshot_worker",
      args: "--config ./config.toml",
      interpreter: "none",
      exec_mode: "fork",
      autorestart: true,
      restart_delay: 5000,
      kill_timeout: 10000,
      out_file: "./logs/rpf-listings-snapshot-worker.out.log",
      error_file: "./logs/rpf-listings-snapshot-worker.err.log",
      merge_logs: false,
      time: true,
    },
  ],
};
