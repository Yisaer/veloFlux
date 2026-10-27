# Contributing to veloFlux

Thank you for your interest in contributing! Please follow these guidelines to ensure your contributions pass CI checks.

## Development Requirements

- Rust (stable toolchain)
- `rustfmt` and `clippy` components

## Code Quality Standards

### Formatting

All code must be formatted with `rustfmt`:

```bash
cargo fmt --all
```

To check formatting without making changes:

```bash
cargo fmt --all -- --check
```

### Linting

Code must pass Clippy with no warnings:

```bash
make clippy
```

### Testing

All tests must pass:

```bash
make test
```

### Code Coverage

The project maintains a **50% minimum line coverage** requirement. To check coverage locally, install [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov):

```bash
cargo install cargo-llvm-cov
```

Then run:

```bash
cargo llvm-cov --workspace --all-features --fail-under-lines 50
```

## Before Submitting a Pull Request

1. Run `cargo fmt --all`
2. Run `make clippy` and fix any warnings
3. Run `make test` and ensure all tests pass
4. Ensure code coverage is at least 50%

## Pull Request Titles

This project follows [Conventional Commits](https://www.conventionalcommits.org/) specification.

> **Note:** CI validates only the **PR title** (not individual commits), so ensure your PR title follows the conventional commit format. We use squash merge, so the PR title becomes the final commit message.

### Format

```
<type>(<scope>): <subject>
```

### Rules

- Keep PR title under 50 characters
- Use lowercase for type and scope
- Do not end subject line with a period

### Types

| Type       | Description                                      |
|------------|--------------------------------------------------|
| `feat`     | A new feature                                    |
| `fix`      | A bug fix                                        |
| `docs`     | Documentation only changes                       |
| `style`    | Formatting, missing semicolons, etc.             |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf`     | Performance improvement                          |
| `test`     | Adding or updating tests                         |
| `build`    | Changes to build system or dependencies          |
| `ci`       | Changes to CI configuration                      |
| `chore`    | Other changes that don't modify src or test files|

### Examples

```
feat(decoder): add SPI signal decoding support
fix(encoder): handle null values correctly
docs: update contributing guidelines
```

## Dependency and workflow checks

CI checks dependencies with [cargo-deny](https://embarkstudios.github.io/cargo-deny/)
0.20.2. Install it with `cargo install cargo-deny --locked --version 0.20.2`, then run:

```sh
cargo deny --workspace --locked check advisories
cargo deny --workspace --locked check licenses bans sources
```

`deny.toml` includes all workspace members, optional features and platform-specific
dependencies. License checks exclude unpublished workspace crates and enforce
third-party dependency licenses; other checks still include the full workspace.
Vulnerabilities, unsoundness advisories, unmaintained crates and
yanked versions fail the advisory check. License and source policies run separately
so advisory failures do not hide their results. Duplicate versions and wildcard
requirements produce warnings, including versionless local path dependencies.
Uncommon licenses have explicit crate/version exceptions; review those exceptions
when updating dependencies. Dependency scanning does not replace an assessment of
whether a reported issue is reachable in the application, or an audit of native
libraries installed outside Cargo.

Do not suppress an advisory merely to pass CI. Review its affected versions,
features and call paths, then update the dependency or document the rationale for
any narrowly scoped exception. Reviewed exceptions and their direct-dependency constraints are recorded in
[Dependency advisory review](docs/development/dependency-advisories.md).

CI uses [Gitleaks](https://github.com/gitleaks/gitleaks) 8.30.1 to scan both the
history reachable from the checked-out commit and its tracked file contents.
After installing that release, reproduce the checks with:

```sh
gitleaks git --redact --log-opts=HEAD .
(
  scan_dir=$(mktemp -d)
  trap 'rm -rf "$scan_dir"' EXIT
  git archive HEAD | tar -x -C "$scan_dir"
  gitleaks dir --redact "$scan_dir"
)
```

These commands check committed content; scan individual edited files with
`gitleaks dir --redact path/to/file` before committing. Use a full clone to scan
history. Secret detection cannot identify every kind of confidential information.

Install [actionlint](https://github.com/rhysd/actionlint) 1.7.12 and
[ShellCheck](https://www.shellcheck.net/) 0.11.0, ensure both are on `PATH`, then run
`actionlint -color` to validate all workflows and their embedded shell scripts.
CI pins tool versions and verifies the downloaded Gitleaks/actionlint archives
against SHA-256 digests. Update the version and digest together when upgrading.

Dependency checks run on pull requests, pushes to `main`, daily and on demand.
Secret and workflow checks run on pull requests, pushes to `main` and on demand.
Dependabot checks Cargo and GitHub Actions dependencies weekly once its config
lands on the default branch. Minor/patch Cargo updates are grouped; major updates
remain separate. Automated security-update enablement is a repository setting,
separate from this version-update configuration. Tool versions embedded in shell
commands or action inputs require manual updates.

CodeQL is not configured yet. Enabling GitHub code scanning requires an eligible
repository (public, or private with Code Security enabled). Revisit Rust and
GitHub Actions CodeQL analysis when that prerequisite is met. Notification
preferences are managed in GitHub settings, not by these workflows.
