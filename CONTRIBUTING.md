# Contributing to Beacon

## Branches

- `main` — stable releases only, protected
- `dev` — active development, all PRs target this branch

## Workflow

1. Fork or create a branch from `dev`
2. Make your changes
3. Open a pull request against `dev`
4. One commit per PR (squash before opening or use squash merge)

## Commit messages

Use the conventional commits format:

- `feat: add route search endpoint`
- `fix: correct lat/lon divisor for advert payloads`
- `chore: update airports.csv`
- `docs: update README project layout`

## Code style

- Run `gofmt -w .` before committing
- Run `go vet ./...` — no warnings
- Run `go build ./...` — must compile

## Tests

- Add tests for any new pure functions
- Run `go test ./...` before opening a PR
- Integration tests are not yet required but welcome

## Dependencies

When adding a new dependency please add it to [SHOULDERS.md](SHOULDERS.md) with
a brief description of what it does.

## Pull requests

- Target `dev` not `main`
- One logical change per PR
- Include a brief description of what changed and why
- Reference any related issues

## Releases

Merges from `dev` to `main` are done by maintainers and represent a versioned
release.
