# Obol

**Write distributed stateful workflows as ordinary sequential Python — let the compiler produce the distributed program.**

Obol is a source-to-source compiler that takes type-annotated, object-oriented Python and compiles it into the asynchronous, message-passing operator functions required by the [Styx](https://github.com/delftdata/styx) stateful Function-as-a-Service (SFaaS) runtime. You write entities and call their methods as if everything ran in one process and Obol synthesizes the routing, state persistence, and continuation management.

## The problem it solves

On a distributed SFaaS runtime, each cross-entity call becomes a network message and the local stack frame is destroyed at every suspension point. A naturally sequential method must be hand-decomposed into a chain of callbacks, each with its own serialized context, return-address record, and explicit state-persistence call. Obol removes the need for this and instead distributed control flow is handled by the compiler.

```python
# What you write (Obol)
@entity
class User:
    def __init__(self, name: str, balance: int):
        self.name = name
        self.balance = balance
        self.myitems: list[Item] = []

    def __key__(self) -> str:
        return self.name

    def buy_item(self, amount: int, item: Item) -> bool:
        total_price = amount * item.get_price()   # cross-entity call, written as a normal call
        if self.balance < total_price:
            raise NotEnoughBalance("Not enough balance.")
        item.update_stock(-amount)
        self.balance -= total_price
        self.myitems.append(item)
        return True
```

Obol compiles `buy_item` into a chain of registered Styx step functions, split at each remote call, with live variables threaded across every asynchronous boundary via an explicit `reply_to` continuation stack.

## The programming model

The surface is deliberately small:

- **Entities** — a distributed stateful object is a class annotated with `@entity`. Its `__init__` attributes are its persistent state.
- **Keys** — each entity defines `__key__()` returning its routing key (a value, or a tuple for composite keys). Styx partitions instances by this key.
- **Methods** — plain synchronous Python over `self` and parameters. No `async def`, no `await`, no manual context dictionaries.
- **Calls** — a call on a value typed as an `@entity` class (e.g. `item.get_price()`) compiles to an asynchronous remote dispatch; everything else stays a local call. The receiver's *type* decides — there is no annotation to mark a call as remote. Use `get_entity_by_key(Item, key)` when you need a reference by key rather than as a parameter.

Supported control flow includes `if`/`else`, `while`/`for` loops, `break`/`continue`, and recursion, with remote calls allowed anywhere inside them.

### Concurrency

By default, calls are sequential (each awaits the previous reply). Two constructs opt into parallelism:

```python
from obol.api import send_async, gather

# fire-and-forget: dispatch without awaiting; return value is suppressed
send_async(user.add_money(50))

# fan-out / fan-in: dispatch independent calls concurrently, bind results once all complete
price, discount = gather(item.get_price(), coupon.get_discount())

# dynamic fan-out over a comprehension
prices = gather(*[item.get_price() for item in self.myitems])
```

`gather` mirrors `asyncio.gather` but compiles to a *persistent* synchronization barrier that survives worker failure. It either yields all results or the whole transaction aborts (no partial tuples, no `return_exceptions`).

> `send_async`, `gather`, `get_entity_by_key`, and `exists` are **compiler intrinsics** — they are recognized and rewritten at compile time and raise if executed directly in plain Python. The `examples/original/` programs are compiler *inputs*, not scripts to run.

## How it works

Obol is a multi-stage pipeline over the [libcst](https://github.com/Instagram/LibCST) concrete syntax tree:

1. **Syntactic preparation** — expand comprehensions containing remote calls into explicit loops, guard short-circuit (`and`/`or`) operands, and linearize so every remote call is a standalone top-level assignment (partial A-Normal Form).
2. **Type resolution** — use `mypy` metadata to resolve each call's receiver type and identify which call sites target `@entity` classes. Unresolvable call sites are rejected.
3. **Live-variable analysis** — a backward dataflow analysis (via [`libcst-dfa`](https://pypi.org/project/libcst-dfa/)) computes the minimal set of variables to serialize across each asynchronous boundary.
4. **Function splitting** — partition each method at its remote-call boundaries into a chain of step functions (CPS + defunctionalization). Loops become tail-recursive step functions; recursion reuses the `reply_to` stack as a distributed call stack.

Every cross-entity call in the source compiles to exactly one asynchronous dispatch in the output — no extra round-trips are introduced.

## Limitations

Obol compiles a typed subset of Python, not the whole language.

**Static typing.** All entity-typed values must carry **type annotations**; a program that doesn't fully type-check under `mypy`, or has a call site whose receiver type can't be resolved, is rejected. The full message-passing structure of the output must be determined statically.

**No aliasing of mutable entity state across a remote call.** Local variables are serialized into the continuation context at each split point, while entity state (`self`) is re-read fresh at the start of every step. A local bound to a *mutable* state field therefore becomes a stale snapshot the moment a remote call splits the method:

```python
items = self.myitems        # local alias of a mutable state field
total = other.compute()     # remote call → split; `items` is now frozen
items.append(x)             # mutates the snapshot, not the live state — lost on resume
```

Re-read `self.myitems` after the call, or take an explicit copy when you only need a read-only view. Immutable values and entity references (which travel as keys, not objects) are safe to bind to locals.

**No inheritance.** Entities are flat. The compiler reads each entity's state from its own `__init__` and compiles only the methods defined directly on the `@entity` class — base classes are ignored. An `@entity` class cannot subclass another entity or inherit state/methods from a shared base.

**Entity shape.** Every `@entity` class must define `__init__` (which declares its persistent state) and `__key__` returning `self.<field>` or a tuple of `self.<field>`s; key components must be `__init__` parameters. Composite keys are concatenated into a single string key.

**Other constructs.**
- **Generator expressions** containing remote calls are rejected (eager materialization would change their lazy semantics) — use a list comprehension or an explicit loop.
- `exists()` is only supported as `exists(self)`.
- Fire-and-forget (`send_async`) suppresses the callee's return value by design, and `gather` provides no `return_exceptions` equivalent — a fanned-out failure aborts the whole transaction.

---

## Development

Obol is a self-contained [uv](https://docs.astral.sh/uv/) project. Run all commands from this directory (`obol/`).

### Install

```bash
uv sync                # runtime + dev dependencies
uv sync --no-dev       # runtime only
uv run prek install    # optional: install the pre-commit hooks
```

### Compile a program

```bash
uv run obol <input.py> [output.py]
```

- `input` — path to the program to compile.
- `output` — optional; where to write the compiled code. Defaults to `examples/compiled/<input filename>`.

```bash
uv run obol                                  # default: examples/original/user_item.py -> examples/compiled/user_item.py
uv run obol path/to/program.py               # -> examples/compiled/program.py
uv run obol path/to/program.py out/result.py # explicit input and output paths
```

### Test, lint, docs

```bash
uv run pytest --cov=src ./tests      # tests
uv run prek run --all-files          # ruff + yamlfix + checks
uv run mkdocs serve --watch ./       # docs locally
```

## License

Distributed under the terms of the [Apache 2.0 License](LICENSE).
