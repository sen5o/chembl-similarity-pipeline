# Contributing

## Branch flow

```
feature/* ──▶ dev ──▶ prod
```

- Work happens on `feature/<name>` branches, one per iteration.
- Each iteration is merged into `dev` via pull request.
- Stable, reviewed work is promoted from `dev` to `prod`.

## Commits — Conventional Commits

Format: `type: short description` (imperative mood).

| type | use |
|------|-----|
| `feat` | new functionality |
| `fix` | bug fix |
| `refactor` | behaviour-preserving change |
| `test` | tests only |
| `build` | docker, dependencies |
| `chore` | config, tooling, housekeeping |
| `docs` | documentation |

One commit = one logical change. Enforced locally by `pre-commit` (commit-msg hook).

## Setup

```bash
pip install pre-commit
pre-commit install --hook-type pre-commit --hook-type commit-msg
```
