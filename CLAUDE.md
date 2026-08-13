# balances-watcher

Chain-scoped Rust service (Axum + Alloy) that streams ERC20 balance updates over SSE.
One process serves exactly one EVM chain, selected by `NETWORK`. Multi-chain coverage
is N replicas behind a path-based ingress — see README.md for the full feature list.

Source layout: `src/api`, `src/evm`, `src/domain`, `src/services`, `src/ws_connection`,
`src/graceful_shutdown`, `src/tracing`, `src/config`, `src/metrics.rs`, `src/args.rs`,
`src/app_state.rs`, `src/app_error.rs`. Match `rustfmt.toml` formatting.

## Comments

Keep comments compact. No restating-the-obvious comments, no multi-paragraph
explanations of what the code already says. Prefer, in order:

1. Clean, self-explanatory code (good names, small functions) over a comment.
2. A `tracing::debug!`/`trace!` line instead of a comment where the "why" is
   really runtime/operational context (e.g. why a branch was taken, what value
   was seen) — it documents the behavior AND is useful at runtime, a plain
   comment is neither.
3. A short one-liner comment only for a genuinely non-obvious invariant, a
   workaround, or a hidden constraint — not for what the code visibly does.

Same bar for doc comments: a brief one-or-two-line `//!`/`///` per file, crate,
or module is welcome (and occasionally per method when the signature alone
doesn't convey intent), but skip long-winded doc blocks.

## CI (GitHub Actions)

- `.github/workflows/code-checks.yml` — `cargo test`, `clippy -D warnings`, `rustfmt`
  check, `cargo audit` (via rustsec/audit-check), and an anvil-backed integration suite
  (`cargo test --test integration|session_lifecycle|token_list_update -- --ignored`).
- `.github/workflows/build-and-release.yml` — builds/pushes the Docker image on every
  PR (smoke test) and on merge to `main`; tags a semver release and pushes
  `:vX.Y.Z`/`:vX.Y`/`:latest` to `ghcr.io/cowprotocol/balances-watcher`.

## Deployment (Flux)

Deployment is not controlled from this repo. It lives in `cowprotocol/infrastructure`,
which holds the Flux/Kustomize GitOps config for this service:

- `cluster/flux-apps/balances-watcher/` — base Deployment/Service.
- `cluster/staging/balances-watcher/<chain>/` — one Kustomize component per chain.
  Staging runs Flux `ImageUpdateAutomation`, so new image tags auto-promote.
- `cluster/prod/balances-watcher/<chain>/` — prod pins the image tag manually via
  `images.newTag` in `cluster/prod/balances-watcher/kustomization.yaml`; promotion is a
  deliberate edit after burn-in in staging, not automatic.

Adding or removing a chain is one directory plus one line in the environment's
`kustomization.yaml` `components` list. Treat this repo as reference/GitOps source of
truth — changes there reconcile straight into the live cluster via Flux.
