# jaeyunha/clawhip fork

This fork (github.com:jaeyunha/clawhip) layers agent-ops behavior on top of
upstream clawhip (github.com:Yeachan-Heo/clawhip). This file is the map for
humans and agents doing upstream merges or deciding where new code belongs.

**Direction is pull-only.** We periodically merge upstream in; we never
contribute back. Don't spend effort making changes PR-able to upstream —
the only reason to keep upstream-facing modules low-diff is to keep our own
upstream merges cheap.

## What is fork-specific

| Concern | Where it lives |
|---|---|
| Durable lane ledger (SQLite: sessions, watch intents, lane board) | `src/ledger.rs` |
| Lane CLI (`clawhip lane board/inspect/reconcile/ignore`) | `src/lane.rs`, `src/cli.rs`, `src/main.rs` |
| tmux session registration, restore, watch intents | `src/tmux_wrapper.rs`, `src/source/tmux.rs`, `src/daemon.rs` |
| Per-lane Discord channel/thread target hints | `src/events.rs` (`EventTargetHint`), `src/router.rs`, `src/discord.rs` |
| GitHub CI/issue polling expansion + stale-CI suppression | `src/source/github.rs`, `src/dispatch.rs` (`GitHubCiBatcher`) |
| Shared shell quoting / keyword-window default | `src/shell.rs` |

## Policy lives in config, not code

Personal/site-specific policy is deliberately **not** hardcoded. It is set in
`~/.clawhip/config.toml` under `[monitors]`:

- `[[monitors.session_owners]]` — repo name → owner label written to the
  lane ledger (`SessionOwner::Named`). Unmapped repos resolve to `unknown`.
- `infra_session_prefixes` — tmux session prefixes hinted as infra
  candidates on the lane board. Default: empty (opt-in).
- `lane_worktree_roots` — worktree roots audited by `clawhip lane inspect`
  when no `--worktree-root` flag is given. Absolute paths; no `~` expansion.

When adding fork features, follow the same rule: site-specific names, paths,
and owners go in config; source modules stay generic.

## Upstream merge rules

- Keep `src/source/*`, `src/discord*.rs`, `src/router.rs`, `src/dispatch.rs`
  close to upstream's shape — not for contributing back (we don't), but so
  upstream merges land with few conflicts.
- Fork-only modules (`src/ledger.rs`, `src/lane.rs`, `src/shell.rs`) rarely
  conflict; conflicts concentrate in `src/daemon.rs`, `src/main.rs`,
  `src/cli.rs`, `src/events.rs` where the fork hooks into upstream flow.
- History warning: both this fork and origin independently merged upstream
  v0.6.9, so `git merge-base` can return multiple bases (criss-cross). If a
  merge proposes deleting fork code that upstream never touched, check
  whether the incoming side actually changed the file
  (`git diff <base> <theirs> -- <file>`) before accepting the hunk.
- Merge upstream with `git fetch upstream && git merge upstream/main`;
  verify with `cargo fmt --check && cargo clippy --all-targets
  --all-features -- -D warnings && cargo test` before pushing.

## Deployment (this machine)

The daemon runs via launchd (`ai.clawhip.daemon`) from the binary at
`~/.cargo/bin/clawhip`. Shipping a change requires reinstalling the binary,
not just restarting:

```sh
cargo install --path . --force
~/.clawhip/bin/restart-daemon.sh   # health-checks http://127.0.0.1:25294/health
```
