import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # slimlink DAG fan-out

    `FullPipeline.run()` now supports fan-out: when several pipelines share
    a parent topic, every parent row is offered to every sibling. The
    strategy argument controls what happens on failure:

    - `strategy="strict"` (default) — per-row atomic across siblings. If
      any sibling raises on a row, no target write for that row commits
      and the parent row stays `new`. The exception propagates
      immediately.
    - `strategy="best_effort"` — siblings run independently per row.
      Successful sibling writes commit. The parent row is only marked
      `handled` once every sibling succeeded on it. Failures are
      collected into a single `FanOutError` raised at the end of the run.

    `best_effort` retries are idempotent because target topics carry
    `dedup`: a second pass produces the same payload and the duplicate
    insert is treated as "already done for this sibling." That's why
    `Source.topic(..., dedup=...)` is now a required keyword — pass
    `dedup=None` to opt out explicitly.

    This notebook walks four scenarios against the same DAG.
    """)
    return


@app.cell
def _():
    from pydantic import BaseModel

    from slimlink import FanOutError, Source

    class Order(BaseModel):
        order_id: str
        customer: str
        price_cents: int

    class HighValueAlert(BaseModel):
        order_id: str
        price_cents: int

    class CustomerHit(BaseModel):
        name: str

    return CustomerHit, FanOutError, HighValueAlert, Order, Source


@app.cell(hide_code=True)
def _(mo):
    mo.md("## DAG: one parent, two children")
    return


@app.cell
def _(CustomerHit, HighValueAlert, Order, Source):
    def build(src):
        orders = src.topic("orders", Order, dedup=("order_id",))
        alerts = src.topic("high_alerts", HighValueAlert, dedup=("order_id",))
        customers = src.topic("customer_hits", CustomerHit, dedup=("name",))

        def to_alert(order):
            return HighValueAlert(
                order_id=order.order_id, price_cents=order.price_cents
            )

        def to_customer(order):
            return CustomerHit(name=order.customer)

        full = src.full_pipeline(
            orders.pipe(to_alert).to(alerts),
            orders.pipe(to_customer).to(customers),
        )
        return orders, alerts, customers, full

    BATCH = [
        {"order_id": "o-001", "customer": "Nina", "price_cents": 1_500},
        {"order_id": "o-002", "customer": "Omar", "price_cents": 25_000},
        {"order_id": "o-003", "customer": "Priya", "price_cents": 8_000},
    ]
    return BATCH, build


@app.cell
def _(Source, build, mo):
    _src = Source(":memory:")
    *_topics, full_for_plot = build(_src)
    mo.mermaid(full_for_plot.plot())
    return


@app.cell
def _():
    def counts(*topics):
        return {
            topic.name: {
                "new": len(list(topic.iter_new())),
                "handled": len(list(topic.iter_handled())),
            }
            for topic in topics
        }

    return (counts,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Scenario 1 — `strict` + happy path

    Both children's transforms succeed. Every parent row should reach
    both children, and the parent topic should be fully handled.
    """)
    return


@app.cell
def _(BATCH, Source, build, counts):
    src1 = Source(":memory:")
    orders1, alerts1, customers1, full1 = build(src1)
    for _payload in BATCH:
        orders1.append(_payload)

    full1.run(strategy="strict")
    snapshot_strict_ok = counts(orders1, alerts1, customers1)
    snapshot_strict_ok
    return (snapshot_strict_ok,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Scenario 2 — `strict` + mid-batch failure

    `customer_fn_buggy` raises on Omar's row. The first row commits to
    both children, the second row's transaction never opens, and the
    exception propagates. No partial state for the failing row, and the
    third row is never reached.
    """)
    return


@app.cell
def _(BATCH, CustomerHit, HighValueAlert, Order, Source, counts):
    src2 = Source(":memory:")
    orders2 = src2.topic("orders", Order, dedup=("order_id",))
    alerts2 = src2.topic("high_alerts", HighValueAlert, dedup=("order_id",))
    customers2 = src2.topic("customer_hits", CustomerHit, dedup=("name",))

    def customer_fn_buggy_2(order):
        if order.customer == "Omar":
            raise RuntimeError("simulated downstream failure for Omar")
        return CustomerHit(name=order.customer)

    full2 = src2.full_pipeline(
        orders2.pipe(
            lambda o: HighValueAlert(order_id=o.order_id, price_cents=o.price_cents)
        ).to(alerts2),
        orders2.pipe(customer_fn_buggy_2).to(customers2),
    )

    for _payload in BATCH:
        orders2.append(_payload)

    try:
        full2.run(strategy="strict")
        outcome_strict_fail = "no exception"
    except Exception as _exc:
        outcome_strict_fail = f"{type(_exc).__name__}: {_exc}"

    snapshot_strict_fail = {
        "outcome": outcome_strict_fail,
        **counts(orders2, alerts2, customers2),
        "orders_new_ids": [r.order_id for r in orders2.iter_new()],
        "orders_handled_ids": [r.order_id for r in orders2.iter_handled()],
    }
    snapshot_strict_fail
    return (snapshot_strict_fail,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Scenario 3 — `best_effort` + mid-batch failure

    Same buggy customer transform, but with `strategy="best_effort"`.
    The alert sibling now writes every parent row. The customer sibling
    writes for Nina and Priya, raises on Omar. Omar's parent row stays
    `new` because not every sibling succeeded on it. At the end of the
    run, a single `FanOutError` is raised with the captured failure.
    """)
    return


@app.cell
def _(BATCH, CustomerHit, FanOutError, HighValueAlert, Order, Source, counts):
    src3 = Source(":memory:")
    orders3 = src3.topic("orders", Order, dedup=("order_id",))
    alerts3 = src3.topic("high_alerts", HighValueAlert, dedup=("order_id",))
    customers3 = src3.topic("customer_hits", CustomerHit, dedup=("name",))

    def customer_fn_buggy_3(order):
        if order.customer == "Omar":
            raise RuntimeError("simulated downstream failure for Omar")
        return CustomerHit(name=order.customer)

    full3 = src3.full_pipeline(
        orders3.pipe(
            lambda o: HighValueAlert(order_id=o.order_id, price_cents=o.price_cents)
        ).to(alerts3),
        orders3.pipe(customer_fn_buggy_3).to(customers3),
    )

    for _payload in BATCH:
        orders3.append(_payload)

    captured_failures_3 = []
    try:
        full3.run(strategy="best_effort")
        outcome_best_effort = "no exception"
    except FanOutError as _exc:
        captured_failures_3 = [
            {
                "record_id": f.record_id,
                "source": f.source,
                "target": f.target,
                "exception": f"{type(f.exception).__name__}: {f.exception}",
            }
            for f in _exc.failures
        ]
        outcome_best_effort = str(_exc)

    snapshot_best_effort = {
        "outcome": outcome_best_effort,
        "captured_failures": captured_failures_3,
        **counts(orders3, alerts3, customers3),
        "orders_new_ids": [r.order_id for r in orders3.iter_new()],
        "orders_handled_ids": [r.order_id for r in orders3.iter_handled()],
        "alerts_ids": [r.order_id for r in alerts3.iter_new()],
        "customers_names": [r.name for r in customers3.iter_new()],
    }
    snapshot_best_effort
    return alerts3, customer_fn_buggy_3, customers3, full3, orders3, snapshot_best_effort


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Scenario 4 — idempotent retry under `best_effort`

    With the bug fixed (Omar is no longer special), re-running the same
    `FullPipeline` should finish what scenario 3 left undone. The alert
    sibling sees Omar's row as `new` and writes it. The customer sibling
    re-runs on Nina and Priya too — their inserts hit `DuplicateMessageError`
    via the target `dedup` and are silently treated as already-done.
    After the run every parent row is `handled`.
    """)
    return


@app.cell
def _(CustomerHit, alerts3, customers3, full3, orders3, snapshot_best_effort):
    snapshot_best_effort  # ensure prior scenario ran first

    fixed_pipelines = list(full3.pipelines)
    fixed_pipelines[1].fn = lambda order: CustomerHit(name=order.customer)

    full3.run(strategy="best_effort")

    snapshot_retry = {
        "orders_new_ids": [r.order_id for r in orders3.iter_new()],
        "orders_handled_ids": [r.order_id for r in orders3.iter_handled()],
        "alerts_ids": [r.order_id for r in alerts3.iter_new()],
        "customers_names": [r.name for r in customers3.iter_new()],
    }
    snapshot_retry
    return (snapshot_retry,)


@app.cell(hide_code=True)
def _(mo, snapshot_retry):
    snapshot_retry
    mo.md(rf"""
    **Final state after retry**: `{snapshot_retry}`.

    All three parent rows are `handled`, both target topics carry all
    three downstream rows, and no duplicates landed because the target
    `dedup` config absorbed the re-attempted inserts on Nina and Priya.
    """)
    return


if __name__ == "__main__":
    app.run()
