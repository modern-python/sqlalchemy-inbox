# sqlalchemy-inbox

A processed-message ledger for consumers on SQLAlchemy's asyncio extension and PostgreSQL: one row
per (scope, message id), inserted in the caller's transaction so it commits or rolls back with the
business writes. Postgres-only. It stores no payload and runs no worker.

## Language

A term is listed only when there is a synonym to reject, or a meaning subtle enough that code and
docs must agree on it. General programming vocabulary does not belong here, however heavily this
project uses it.

**Marker**:
One ledger row, keyed by `(scope, message_id)`. Its presence after commit means the database
effects for that key have committed.
_Avoid_: lock, processed event, dedup record

**Scope**:
A stable string naming one logical consumer of a message, such as `chats.create-chat.v1`. Two
consumers that process the same message independently use different scopes.
_Avoid_: handler name, consumer group (both change on refactors and deployment renames)

**Message id**:
The producer-assigned identity of a message, the same on every delivery.
_Avoid_: offset, delivery id (both change between deliveries)

**Claim**:
Inserting a marker in the caller's open transaction. A claim that inserted returns `True`; one
that found a committed marker returns `False`.
_Avoid_: acquire, lock, register

**Retention**:
How long a marker is kept. After it is deleted, the same key is new again.
_Avoid_: TTL (nothing expires on its own; cleanup deletes)

**Cutoff**:
The database timestamp, taken once per cleanup run, before which markers are deleted.
_Avoid_: deadline, expiry time
