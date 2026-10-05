# Contributing

Thanks for your interest in decision-model-operator. This is an open-source
Kubernetes operator; contributions are welcome.

By participating you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
Questions go to [GitHub Discussions](https://github.com/maks3201/decision-model-operator/discussions);
security issues are reported privately as described in [SECURITY.md](SECURITY.md).

## Development setup

Requirements: Go (see `go.mod` for the version), Docker (OrbStack works on
macOS), `kind` for local clusters, and `make`.

Common workflow before opening a PR:

```sh
make generate manifests   # regenerate deepcopy, CRDs, RBAC (and sync the Helm chart)
make lint                 # golangci-lint (if installed)
make test                 # unit + envtest (controller-runtime downloads envtest binaries)
make build                # go vet + build the manager
```

`make test` uses [envtest](https://book.kubebuilder.io/reference/envtest.html);
the test setup downloads the control-plane binaries automatically.

### Git hooks

Install the repository's git hooks (a gitleaks secret-scan pre-commit hook):

```sh
./hack/install-git-hooks.sh
```

This copies the hooks from `.githooks/` into your local `.git/hooks/` and makes
no changes to your git config.

### End-to-end tests

E2E runs a full lifecycle on `kind` with the Ollaya runtime. It is heavy
(pulls model weights) and is intended to run **in CI on this repository**, not
routinely on a laptop:

```sh
make test-e2e            # brings a kind cluster up, runs the suite, tears it down
```

A GPU e2e path exists as a manual workflow — see `docs/gpu-ci.md`.

## Pull requests

- Keep PRs focused; update docs when behaviour changes.
- Use [Conventional Commits](https://www.conventionalcommits.org/) for commit
  messages and the PR title, e.g. `feat: add eval-gated rollout`,
  `fix(engine): honour Retry-After on 429`.
- Make sure `make lint test build` is green and no secrets are committed.
- A DCO sign-off is **not** required.

## Reporting bugs and requesting features

Use the issue templates under **New issue**. For security issues, follow
[SECURITY.md](SECURITY.md) (report privately, not as a public issue).

## License

By contributing, you agree that your contributions are licensed under the
Apache-2.0 license (see `LICENSE`).
