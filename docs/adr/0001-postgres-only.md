# PostgreSQL only

The package supports PostgreSQL and no other database. Its value is the guarantees in the README, and
those come from how Postgres behaves: `INSERT ... ON CONFLICT DO NOTHING` waits on an uncommitted
duplicate under READ COMMITTED, and cleanup relies on `FOR UPDATE SKIP LOCKED` and `DELETE ...
RETURNING`. Each extra dialect would need its own code paths, its own guarantees and its own CI matrix.

## Considered Options

- SQLite as well, for tests without a server. The insert syntax matches, but SQLite has one writer at a
  time and no row locks, so it would test a different concurrency model from the one in production.
- MySQL. It has no `RETURNING`, rejects `LIMIT` in an `IN` subquery, and its `INSERT IGNORE` turns
  unrelated errors into warnings, so both claim and cleanup would need separate implementations.
