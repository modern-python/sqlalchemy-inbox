# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`sqlalchemy-inbox` is a transactional inbox ledger for SQLAlchemy asyncio on PostgreSQL.
[`CONTEXT.md`](CONTEXT.md) opens with what it does and owns the vocabulary. Read it before naming a
concept in code, a test name, or an issue title.

## Commands

`just` (task runner) and `uv` (package manager). The [`justfile`](justfile) is the source of truth.
The things it does not say:

- `just test [args]` forwards args **unquoted**, so a spaced `-k` expression word-splits and fails.
- Every test needs PostgreSQL and skips if `POSTGRES_DSN` is unreachable. The compose Postgres
  publishes no host port, so run tests through `just test`. The coverage gate is on in CI; pass
  `--no-cov` while iterating locally if you add `--cov`.

## Architecture

- `Inbox` never commits, rolls back, begins, or opens a session or connection. Transaction
  ownership stays with the caller. Any new method must keep that.
- `InboxCleaner` is the only code that begins and commits, and only on connections it opens from
  the caller's engine. It never takes a caller's session and never disposes the engine.
- `Inbox.claim` requires an open transaction on its session. That is what turns a wrongly wired
  second session into an error instead of a marker that never commits.
- Concurrency claims are proven only by tests on separate connections, never one shared test
  transaction.

## Workflow

Every link in `README.md` must be absolute: `https://github.com/modern-python/sqlalchemy-inbox/blob/main/<path>`.
`README.md` is also the PyPI long description, and PyPI does not rewrite relative links.

## Code Style

- **Never use local/inline imports**, tests included. `ruff` catches them.
- Annotate every function argument.
- Docstrings: public API documents the contract; internal helpers get a one-line contract at most.
