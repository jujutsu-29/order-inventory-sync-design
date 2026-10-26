# Order & Inventory Sync Service: Design

Design-only. It describes a service that keeps orders and stock in
sync across four channels (own website, Nykaa, Flipkart, Blinkit) and the ERP
(Microsoft Business Central).
## What's here

| File | Purpose |
|---|---|
| [DESIGN.md](DESIGN.md) | Full design: architecture, data model, ingestion, inventory consistency, stock push, failure handling, observability, assumptions, trade-offs |
| [architecture.png](architecture.png) | Architecture diagram |
| [architecture.dot](architecture.dot) | Diagram source (Graphviz) |

## Setup

Nothing to install to read it: open `DESIGN.md` on GitHub, or in any Markdown
viewer, with `architecture.png` in the same folder.

To regenerate the diagram after editing the source (optional):

    # requires Graphviz (https://graphviz.org/download/)
    dot -Tpng -Gdpi=110 architecture.dot -o architecture.png

## Approach in brief

- **PostgreSQL is the source of truth for stock.** Orders reserve stock with
  one atomic conditional `UPDATE`, so two channels can never take the same
  last unit. Every change is written to an append-only ledger for audit.
- **Nothing is lost.** Incoming events, stock pushes and ERP orders are stored
  in PostgreSQL (inbox/outbox) before acknowledgement; Redis/BullMQ only wakes
  workers and enforces rate limits.
- **Stock pushes use desired-vs-pushed state.** Each channel gets the latest
  absolute quantity, coalesced and rate-limited per channel, with low-stock
  SKUs pushed first. A safety buffer covers the delay on channels.
- **Failures are isolated.** Per-channel workers and circuit breakers, ERP jobs
  queued through the Sunday downtime, retries with backoff, and a failed-jobs
  screen for ops.
- **Stack:** Node.js/NestJS, PostgreSQL, Redis, AWS (ECS Fargate, RDS). No
  extra components beyond this.

See [DESIGN.md](DESIGN.md) for the details, assumptions and trade-offs.