"""
Randomized differential test: local entities vs compiled Styx operators.

A seeded generator produces random sequences of entity operations. Each
operation runs on the local Python classes (the reference) and is replayed
on Styx, comparing return values and, after every op, the observable state
of all entities. Known semantic differences are normalized away:
  - entity references: objects locally, key strings on Styx
  - domain exceptions: raised locally, returned as error strings by Styx
One extra detail makes the local reference sound: Styx rolls back the whole
transaction on an exception, plain Python doesn't (e.g. bulk_purchase
mutates earlier items before raising for a later one). So we snapshot the
local world before each op and restore it when a domain exception fires.

Requires a running Styx deployment

Run:  uv run pytest tests/test_differential.py -v   (with Styx running)
Reproduce a failure: set SEED to the seed printed in the failure message.
Tune with env vars: SEED, SEQUENCES, OPS, OBOL_EXAMPLE.
"""

import copy
import importlib.util
import os
import pathlib
import random
import socket
import sys
import unittest
import uuid
from collections import Counter
from time import sleep

from styx.client.sync_client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXAMPLE = os.environ.get("OBOL_EXAMPLE", "user_item")


def _load_module(path: pathlib.Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


m = _load_module(ROOT / "examples" / "original" / f"{EXAMPLE}.py", "obol_reference")

m.gather = lambda *args: args

compiled = _load_module(ROOT / "examples" / "compiled" / f"{EXAMPLE}.py", "obol_compiled")
item_operator = compiled.item_operator
user_operator = compiled.user_operator
coupon_operator = compiled.coupon_operator

SEED = int(os.environ.get("SEED", "42"))
N_SEQUENCES = int(os.environ.get("SEQUENCES", "10"))
OPS_PER_SEQ = int(os.environ.get("OPS", "300"))

# Connection targets (override via env to match your deployment).
STYX_HOST = os.environ.get("STYX_HOST", "localhost")
STYX_PORT = int(os.environ.get("STYX_PORT", "8886"))
KAFKA_URL = os.environ.get("KAFKA_URL", "localhost:9092")
styx = None


def _reachable(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def setUpModule():
    global styx
    kafka_host, _, kafka_port = KAFKA_URL.partition(":")
    if not _reachable(STYX_HOST, STYX_PORT) or not _reachable(kafka_host, kafka_port or 9092):
        raise unittest.SkipTest(
            f"Styx is not running: this differential test replays operations on a live Styx "
            f"deployment and needs the coordinator at {STYX_HOST}:{STYX_PORT} and Kafka at "
            f"{KAFKA_URL}. Start Styx first (e.g. `docker compose up` from the styx repo root), "
            f"then re-run."
        )
    try:
        print(f"[setup] connecting to Styx at {STYX_HOST}:{STYX_PORT}, kafka={KAFKA_URL}", flush=True)
        styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
        g = StateflowGraph("wdm-difftest", operator_state_backend=LocalStateBackend.DICT)
        item_operator.set_n_partitions(1)
        user_operator.set_n_partitions(1)
        coupon_operator.set_n_partitions(1)
        g.add_operators(item_operator, user_operator, coupon_operator)
        print("[setup] submitting dataflow graph...", flush=True)
        styx.submit_dataflow(g)
        print("[setup] graph submitted; waiting for workers to register...", flush=True)
        sleep(10)
        print("[setup] opening client (starting Kafka consumers)...", flush=True)
        styx.open(consume=True)
        print("[setup] ready.", flush=True)
    except Exception as e:
        raise unittest.SkipTest(f"Could not connect to a running Styx deployment: {e}") from e


def tearDownModule():
    if styx:
        styx.close()


# --------------------------------------------------------------------------
# World: paired local objects and Styx entities, created from one seed
# --------------------------------------------------------------------------


class Ref:
    """Placeholder for an entity in generated args: resolved to the local
    object on the local side and to the key string on the Styx side."""

    def __init__(self, kind, idx):
        self.kind, self.idx = kind, idx

    def __repr__(self):
        return f"<{self.kind}{self.idx}>"


class World:
    def __init__(self, rng):
        self.rng = rng
        self.users, self.items, self.coupons = [], [], []  # local objects
        self.user_keys, self.item_keys, self.coupon_keys = [], [], []  # styx keys
        for _ in range(rng.randint(2, 3)):
            self._new_user(rng.randint(0, 300))
        for _ in range(rng.randint(3, 5)):
            self._new_item(rng.randint(1, 100), rng.randint(0, 12))
        for _ in range(rng.randint(1, 3)):
            self._new_coupon(rng.randint(0, 60))

    def _send(self, op, key, fn, params=()):
        return styx.send_event(operator=op, key=key, function=fn, params=params).get().response

    def _new_user(self, balance):
        key = str(uuid.uuid4())
        u = m.User(key)
        self._send(user_operator, key, "insert", (key,))
        if balance:
            u.add_balance(balance)
            self._send(user_operator, key, "add_balance", (balance,))
        self.users.append(u)
        self.user_keys.append(key)

    def _new_item(self, price, stock):
        key = str(uuid.uuid4())
        it = m.Item(key, price)
        self._send(item_operator, key, "insert", (key, price))
        if stock:
            it.update_stock(stock)
            self._send(item_operator, key, "update_stock", (stock,))
        self.items.append(it)
        self.item_keys.append(key)

    def _new_coupon(self, discount):
        key = str(uuid.uuid4())
        c = m.Coupon(key, discount)
        self._send(coupon_operator, key, "insert", (key, discount))
        self.coupons.append(c)
        self.coupon_keys.append(key)

    def _lists(self, kind):
        return {
            "user": (self.users, self.user_keys),
            "item": (self.items, self.item_keys),
            "coupon": (self.coupons, self.coupon_keys),
        }[kind]

    # token maps so both sides normalize to the same canonical names
    def token(self, key):
        if key in self.user_keys:
            return f"<user{self.user_keys.index(key)}>"
        if key in self.item_keys:
            return f"<item{self.item_keys.index(key)}>"
        if key in self.coupon_keys:
            return f"<coupon{self.coupon_keys.index(key)}>"
        return key

    def resolve_local(self, a):
        if isinstance(a, Ref):
            return self._lists(a.kind)[0][a.idx]
        return [self.resolve_local(x) for x in a] if isinstance(a, list) else a

    def resolve_styx(self, a):
        if isinstance(a, Ref):
            return self._lists(a.kind)[1][a.idx]
        return [self.resolve_styx(x) for x in a] if isinstance(a, list) else a

    def normalize(self, v):
        """Canonicalize for comparison: entities/keys -> tokens, tuples -> lists."""
        if isinstance(v, (m.User, m.Item, m.Coupon)):
            return self.token(v.__key__())
        if isinstance(v, str):
            return self.token(v)
        if isinstance(v, dict):
            return {self.normalize(k): self.normalize(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [self.normalize(x) for x in v]
        return v

    def local_state(self):
        s = {}
        for i, u in enumerate(self.users):
            s[f"<user{i}>"] = (u.get_balance(), [self.token(it.__key__()) for it in u.get_items()])
        for i, it in enumerate(self.items):
            s[f"<item{i}>"] = (it.get_price(), it.get_stock())
        for i, c in enumerate(self.coupons):
            s[f"<coupon{i}>"] = c.get_discount()
        return s

    def styx_state(self):
        s = {}
        for i, key in enumerate(self.user_keys):
            items = self._send(user_operator, key, "get_items") or []
            s[f"<user{i}>"] = (self._send(user_operator, key, "get_balance"), [self.token(k) for k in items])
        for i, key in enumerate(self.item_keys):
            s[f"<item{i}>"] = (self._send(item_operator, key, "get_price"), self._send(item_operator, key, "get_stock"))
        for i, key in enumerate(self.coupon_keys):
            s[f"<coupon{i}>"] = self._send(coupon_operator, key, "get_discount")
        return s


# --------------------------------------------------------------------------
# Operation generators: (kind, idx, method, args) with args biased toward
# boundaries. Each may inspect the live local world; None if not applicable.
# --------------------------------------------------------------------------


def _items(rng, w, nmax, nmin=0):
    return [Ref("item", rng.randrange(len(w.items))) for _ in range(rng.randint(nmin, nmax))]


def _coupons(rng, w, nmax, nmin=0):
    return [Ref("coupon", rng.randrange(len(w.coupons))) for _ in range(rng.randint(nmin, nmax))]


def g_update_stock(rng, w):
    i = rng.randrange(len(w.items))
    stock = w.items[i].get_stock()
    amt = rng.choice([rng.randint(0, 10), -stock, -(stock + 1)])
    return "item", i, "update_stock", (amt,)


def g_add_balance(rng, w):
    return "user", rng.randrange(len(w.users)), "add_balance", (rng.randint(0, 150),)


def g_buy_item(rng, w):
    u, i = rng.randrange(len(w.users)), rng.randrange(len(w.items))
    price = w.items[i].get_price()
    opts = [rng.randint(0, 5), w.items[i].get_stock(), w.items[i].get_stock() + 1]
    if price > 0:
        opts.append(min(w.users[u].get_balance() // price, 30))  # exactly affordable
    return "user", u, "buy_item", (max(rng.choice(opts), 0), Ref("item", i))


def g_drain_stock(rng, w):
    small = [i for i, it in enumerate(w.items) if it.get_stock() <= 10]
    if not small:
        return None
    return "user", rng.randrange(len(w.users)), "drain_stock", (Ref("item", rng.choice(small)),)


def g_bulk(rng, w):
    items = _items(rng, w, 3, nmin=1)
    qty = [rng.choice([rng.randint(0, 8), rng.randint(11, 14), rng.randint(51, 55)]) for _ in items]
    return "user", rng.randrange(len(w.users)), "bulk_purchase_with_tiers", (items, qty)


def g_process_cart(rng, w):
    small = [i for i, it in enumerate(w.items) if it.get_stock() <= 8]
    cart = [Ref("item", rng.choice(small)) for _ in range(rng.randint(0, 4))] if small else []
    return "user", rng.randrange(len(w.users)), "process_cart_with_limits", (cart, rng.randint(0, 150))


def g_transfer(rng, w):
    a, b = rng.randrange(len(w.users)), rng.randrange(len(w.users))
    bal = w.users[a].get_balance()
    amt = rng.choice([rng.randint(0, bal + 20), bal, bal + 1])
    return "user", a, "transfer_balance", (Ref("user", b), amt)


def g_multi_restock(rng, w):
    items = _items(rng, w, 4)
    n = max(len(items) + rng.randint(-1, 1), 0)  # zip truncation edge
    return "user", rng.randrange(len(w.users)), "multi_restock", (items, [rng.randint(0, 10) for _ in range(n)])


def g_user_noargs(method):
    return lambda rng, w: ("user", rng.randrange(len(w.users)), method, ())


def g_item_list(method, nmax):
    return lambda rng, w: ("user", rng.randrange(len(w.users)), method, (_items(rng, w, nmax),))


def g_item_arg(method):
    return lambda rng, w: ("user", rng.randrange(len(w.users)), method, (Ref("item", rng.randrange(len(w.items))),))


def g_discounted_sum(rng, w):
    return "user", rng.randrange(len(w.users)), "discounted_sum", (_items(rng, w, 5), rng.randint(0, 100))


def g_demo2(rng, w):
    arg = None if rng.random() < 0.3 else Ref("item", rng.randrange(len(w.items)))
    return "user", rng.randrange(len(w.users)), "demo2", (arg,)


# -- coupon / gather operations ---------------------------------------------


def g_get_discounted_price(rng, w):
    # discount may exceed price (clamped by max(...,0)) — both cases occur
    return (
        "user",
        rng.randrange(len(w.users)),
        "get_discounted_price",
        (Ref("item", rng.randrange(len(w.items))), Ref("coupon", rng.randrange(len(w.coupons)))),
    )


def g_buy_with_coupon(rng, w):
    coupon = None if rng.random() < 0.3 else Ref("coupon", rng.randrange(len(w.coupons)))
    return "user", rng.randrange(len(w.users)), "buy_with_coupon", (Ref("item", rng.randrange(len(w.items))), coupon)


def g_gather_in_loop(rng, w):
    # zip truncation edge again, this time around a gather inside the loop
    return "user", rng.randrange(len(w.users)), "gather_in_loop", (_items(rng, w, 3), _coupons(rng, w, 3))


def g_price_check(rng, w):
    # 3-way gather across two entity types: stresses barrier tag ordering
    return (
        "user",
        rng.randrange(len(w.users)),
        "price_check",
        (
            Ref("item", rng.randrange(len(w.items))),
            Ref("item", rng.randrange(len(w.items))),
            Ref("coupon", rng.randrange(len(w.coupons))),
        ),
    )


GENERATORS = [
    (g_update_stock, 4),
    (g_add_balance, 3),
    (g_buy_item, 5),
    (g_drain_stock, 3),
    (g_bulk, 3),
    (g_process_cart, 3),
    (g_transfer, 3),
    (g_multi_restock, 3),
    (g_discounted_sum, 2),
    (g_demo2, 1),
    (g_get_discounted_price, 3),
    (g_buy_with_coupon, 4),
    (g_gather_in_loop, 3),
    (g_price_check, 3),
    (g_user_noargs("get_balance"), 1),
    (g_user_noargs("get_items"), 1),
    (g_user_noargs("inventory_value"), 2),
    (g_user_noargs("my_item_prices"), 2),
    (g_user_noargs("most_valuable_item_price"), 1),
    (g_user_noargs("inventory_value_gather"), 2),
    (g_item_list("simple_loop", 5), 2),
    (g_item_list("recursion_test", 5), 2),
    (g_item_list("comprehensions", 4), 2),
    (g_item_list("can_afford_cart", 4), 1),
    (g_item_list("group_items_by_price_bucket", 5), 2),
    (g_item_arg("ret_tuple"), 1),
    (g_item_arg("ret_dict"), 1),
    (g_item_arg("is_in_stock"), 1),
    (g_item_arg("temp_func"), 1),
]
# Excluded by design: fire_and_forget/demo (send_async is a compile-time
# marker; a local stub cannot suppress evaluation of its argument, so the
# reference cannot execute it faithfully) and type_test (compiler smoke test).

DOMAIN_EXC = (m.NotEnoughBalance, m.OutOfStock)


# --------------------------------------------------------------------------
# The test
# --------------------------------------------------------------------------


class TestDifferential(unittest.TestCase):
    def _run_op(self, w, kind, idx, method, args):
        """Run one op on both sides; return mismatch string or None."""
        # local, with transactional-rollback semantics
        snapshot = copy.deepcopy((w.users, w.items, w.coupons))
        target = w._lists(kind)[0][idx]
        try:
            local = ("ok", getattr(target, method)(*[w.resolve_local(a) for a in args]))
        except DOMAIN_EXC as e:
            w.users, w.items, w.coupons = snapshot
            local = ("exc", str(e))
        except Exception:
            w.users, w.items, w.coupons = snapshot
            return "SKIP"  # generator produced an ill-formed op; discard

        # styx
        op = {"user": user_operator, "item": item_operator, "coupon": coupon_operator}[kind]
        key = w._lists(kind)[1][idx]
        resp = (
            styx.send_event(operator=op, key=key, function=method, params=tuple(w.resolve_styx(a) for a in args))
            .get()
            .response
        )

        # compare results (Styx returns the exception message string verbatim)
        if local[0] == "exc":
            if not isinstance(resp, str) or resp != local[1]:
                return f"error mismatch: local={local[1]!r} styx={resp!r}"
        elif w.normalize(local[1]) != w.normalize(resp):
            return f"result: local={w.normalize(local[1])!r} styx={w.normalize(resp)!r}"

        # compare full observable state
        ls, ss = w.local_state(), w.styx_state()
        if ls != ss:
            diff = {k: (ls.get(k), ss.get(k)) for k in ls if ls.get(k) != ss.get(k)}
            return f"state diverged (local, styx): {diff}"
        return None

    def test_random_sequences(self):
        coverage = Counter()
        for seq in range(N_SEQUENCES):
            seed = SEED + seq
            rng = random.Random(seed)
            w = World(rng)
            trace = []
            done = 0
            while done < OPS_PER_SEQ:
                gen = rng.choices([g for g, _ in GENERATORS], weights=[wt for _, wt in GENERATORS])[0]
                built = gen(rng, w)
                if built is None:
                    continue
                kind, idx, method, args = built
                mismatch = self._run_op(w, kind, idx, method, args)
                if mismatch == "SKIP":
                    continue
                trace.append(f"<{kind}{idx}>.{method}{args}")
                done += 1
                coverage[method] += 1
                self.assertIsNone(
                    mismatch, f"\nSEED={seed} step {done}: {trace[-1]}\n{mismatch}\ntrace:\n  " + "\n  ".join(trace)
                )
            print(f"seq {seq + 1}/{N_SEQUENCES} (seed {seed}): {done} ops ok")
        print("\noperation coverage:")
        for method, count in sorted(coverage.items()):
            print(f"  {method:32s} {count}")
        print(f"  total: {sum(coverage.values())} operations, {len(coverage)} distinct methods")


if __name__ == "__main__":
    unittest.main()
