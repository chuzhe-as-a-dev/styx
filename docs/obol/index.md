# Obol

Obol is a source-to-source compiler that turns sequential, type-annotated,
object-oriented Python into the asynchronous, message-passing Styx operator
functions. You write entities and call their methods as if everything ran in a
single process, and Obol synthesizes the routing, state persistence, and
continuation management required by the Styx runtime — without weakening its
serializable, exactly-once transactional guarantees.

```python
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

Obol compiles this into a chain of registered Styx step functions, split at each
remote call, with live variables threaded across every asynchronous boundary via
an explicit `reply_to` continuation stack.

The compiler lives under [`obol/`](https://github.com/delftdata/styx/tree/main/obol)
in the repository; see its `README.md` for the full programming model, the
compilation pipeline, limitations, and development instructions.

## API Reference

The Obol surface a program uses is the set of DSL intrinsics in `obol.api`:
`entity`, `send_async`, `gather`, `get_entity_by_key`, and `exists`. (The
compiler internals under `obol/src/obol` are not part of the public API.)

::: obol.api
    options:
      show_submodules: false
      show_root_heading: true
      show_overloads: false
