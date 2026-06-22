import importlib.util
import os
import pathlib
import sys
import uuid
from timeit import default_timer as timer

from sanic import Sanic, json, text
from styx.client import AsyncStyxClient
from styx.client.styx_future import StyxResponse
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

EXAMPLE = os.environ.get("OBOL_EXAMPLE", "user_item")
_compiled_path = pathlib.Path(__file__).resolve().parent.parent / "examples" / "compiled" / f"{EXAMPLE}.py"
_spec = importlib.util.spec_from_file_location("obol_compiled_app", _compiled_path)
_compiled = importlib.util.module_from_spec(_spec)
sys.modules["obol_compiled_app"] = _compiled
_spec.loader.exec_module(_compiled)

item_operator = _compiled.item_operator
user_operator = _compiled.user_operator
coupon_operator = _compiled.coupon_operator
OutOfStock = _compiled.OutOfStock
NotEnoughBalance = _compiled.NotEnoughBalance

app = Sanic("obol-app")

STYX_HOST = os.environ.get("STYX_HOST", "localhost")
STYX_PORT = int(os.environ.get("STYX_PORT", "8888"))
KAFKA_URL = os.environ.get("KAFKA_URL", "localhost:9092")

styx_client = AsyncStyxClient(STYX_HOST, STYX_PORT, KAFKA_URL)


# ── CORS ──────────────────────────────────────────────────────────────────────


@app.on_request
async def handle_options(request):
    if request.method == "OPTIONS":
        return text(
            "",
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Max-Age": "86400",
            },
        )
    return None


@app.on_response
async def add_cors(request, response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"


# ── Lifecycle ─────────────────────────────────────────────────────────────────


@app.listener("before_server_start")
async def setup_styx(app, loop):
    await styx_client.open(consume=True)


@app.get("/")
async def health_check(_):
    return text("User Item App is running")


# ── Dataflow ──────────────────────────────────────────────────────────────────


@app.post("/submit/<n_partitions>")
async def submit_dataflow_graph(_, n_partitions: str):
    partitions = int(n_partitions)
    g = StateflowGraph("wdm-project", operator_state_backend=LocalStateBackend.DICT)

    item_operator.set_n_partitions(partitions)
    user_operator.set_n_partitions(partitions)
    coupon_operator.set_n_partitions(partitions)
    g.add_operators(item_operator, user_operator, coupon_operator)

    await styx_client.submit_dataflow(g)
    return json({"graph_submitted": True})


# ── User Endpoints ────────────────────────────────────────────────────────────


@app.post("/user/create")
async def create_user(request):
    body = request.json or {}
    name = body.get("name", "unknown")
    future = await styx_client.send_event(operator=user_operator, key=name, function="insert", params=(name,))
    result: StyxResponse = await future.get()
    return json({"user_id": result.response})


@app.get("/user/<user_id>/balance")
async def get_balance(request, user_id: str):
    future = await styx_client.send_event(operator=user_operator, key=user_id, function="get_balance")
    result: StyxResponse = await future.get()
    return json({"balance": result.response})


@app.get("/user/<user_id>/items")
async def get_user_items(request, user_id: str):
    future = await styx_client.send_event(operator=user_operator, key=user_id, function="get_items")
    result: StyxResponse = await future.get()
    return json({"items": result.response})


@app.post("/user/<user_id>/add_balance/<amount>")
async def add_balance(request, user_id: str, amount: str):
    future = await styx_client.send_event(
        operator=user_operator, key=user_id, function="add_balance", params=(int(amount),)
    )
    result: StyxResponse = await future.get()
    return json({"success": result.response})


# ── Item Endpoints ────────────────────────────────────────────────────────────


@app.post("/item/create")
async def create_item(request):
    body = request.json or {}
    name = body.get("name", str(uuid.uuid4()))
    price = int(body.get("price", 0))
    future = await styx_client.send_event(operator=item_operator, key=name, function="insert", params=(name, price))
    result: StyxResponse = await future.get()
    return json({"item_id": result.response})


@app.get("/item/<item_id>/stock")
async def get_stock(request, item_id: str):
    future = await styx_client.send_event(operator=item_operator, key=item_id, function="get_stock")
    result: StyxResponse = await future.get()
    return json({"stock": result.response})


@app.post("/item/<item_id>/add_stock/<amount>")
async def add_stock(request, item_id: str, amount: str):
    future = await styx_client.send_event(
        operator=item_operator, key=item_id, function="update_stock", params=(int(amount),)
    )
    result: StyxResponse = await future.get()
    return json({"success": result.response})


@app.get("/item/<item_id>/price")
async def get_price(request, item_id: str):
    future = await styx_client.send_event(operator=item_operator, key=item_id, function="get_price")
    result: StyxResponse = await future.get()
    return json({"price": result.response})


# ── Coupon Endpoints ──────────────────────────────────────────────────────────


@app.post("/coupon/create")
async def create_coupon(request):
    body = request.json or {}
    code = body.get("code", str(uuid.uuid4()))
    discount = int(body.get("discount", 0))
    future = await styx_client.send_event(
        operator=coupon_operator, key=code, function="insert", params=(code, discount)
    )
    result: StyxResponse = await future.get()
    return json({"coupon_id": result.response})


@app.get("/coupon/<coupon_id>/discount")
async def get_discount(request, coupon_id: str):
    future = await styx_client.send_event(operator=coupon_operator, key=coupon_id, function="get_discount")
    result: StyxResponse = await future.get()
    return json({"discount": result.response})


# ── Transaction Endpoints ─────────────────────────────────────────────────────


@app.post("/buy_item/<user_id>/<item_id>/<amount>")
async def buy_item(request, user_id: str, item_id: str, amount: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="buy_item", params=(int(amount), item_id)
        )
        result: StyxResponse = await future.get()
        return json({"purchase_successful": result.response, "latency": result.styx_latency_ms})
    except (OutOfStock, NotEnoughBalance) as e:
        return json({"purchase_successful": False, "error": str(e)}, status=400)


@app.post("/user/<user_id>/transfer/<recipient_id>/<amount>")
async def transfer_balance(request, user_id: str, recipient_id: str, amount: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="transfer_balance", params=(recipient_id, int(amount))
        )
        result: StyxResponse = await future.get()
        return json({"success": result.response})
    except NotEnoughBalance as e:
        return json({"success": False, "error": str(e)}, status=400)


@app.get("/user/<user_id>/is_in_stock/<item_id>")
async def is_in_stock(request, user_id: str, item_id: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="is_in_stock", params=(item_id,)
        )
        result: StyxResponse = await future.get()
        return json({"is_in_stock": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.get("/user/<user_id>/discounted_price/<item_id>/<coupon_id>")
async def get_discounted_price(request, user_id: str, item_id: str, coupon_id: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="get_discounted_price", params=(item_id, coupon_id)
        )
        result: StyxResponse = await future.get()
        return json({"discounted_price": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.post("/user/<user_id>/gather_in_loop")
async def gather_in_loop(request, user_id: str):
    body = request.json or {}
    items: list[str] = body.get("items", [])
    coupons: list[str] = body.get("coupons", [])
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="gather_in_loop", params=(items, coupons)
        )
        result: StyxResponse = await future.get()
        return json({"total": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.post("/user/<user_id>/buy_with_coupon/<item_id>")
async def buy_with_coupon(request, user_id: str, item_id: str):
    body = request.json or {}
    coupon = body.get("coupon")  # may be None
    try:
        start = timer()
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="buy_with_coupon", params=(item_id, coupon)
        )
        result: StyxResponse = await future.get()
        end = timer()
        c_lat = round((end - start) * 1000, 0)
        return json(
            {
                "purchase_successful": result.response,
                "latency": result.styx_latency_ms,
                "total_time": c_lat,
                "client_added_latency": c_lat - result.styx_latency_ms,
            }
        )
    except (OutOfStock, NotEnoughBalance) as e:
        return json({"purchase_successful": False, "error": str(e)}, status=400)


# ── Bulk / cart endpoints ─────────────────────────────────────────────────────


@app.post("/bulk_purchase_with_tiers/<user_id>")
async def bulk_purchase_with_tiers(request, user_id: str):
    body = request.json or {}
    cart: list[str] = body.get("cart", [])
    quantities: list[int] = body.get("quantities", [])
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="bulk_purchase_with_tiers", params=(cart, quantities)
        )
        result: StyxResponse = await future.get()
        return json({"success": result.response, "latency": result.styx_latency_ms})
    except (OutOfStock, NotEnoughBalance) as e:
        return json({"success": False, "error": str(e)}, status=400)


@app.post("/user/<user_id>/process_cart_with_limits")
async def process_cart_with_limits(request, user_id: str):
    body = request.json or {}
    cart: list[str] = body.get("cart", [])
    max_spend = int(body.get("max_spend", 0))
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="process_cart_with_limits", params=(cart, max_spend)
        )
        result: StyxResponse = await future.get()
        return json({"purchased": result.response})
    except (OutOfStock, NotEnoughBalance) as e:
        return json({"purchased": {}, "error": str(e)}, status=400)


@app.post("/user/<user_id>/can_afford_cart")
async def can_afford_cart(request, user_id: str):
    body = request.json or {}
    items: list[str] = body.get("items", [])
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="can_afford_cart", params=(items,)
        )
        result: StyxResponse = await future.get()
        return json({"can_afford": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.post("/user/<user_id>/multi_restock")
async def multi_restock(request, user_id: str):
    body = request.json or {}
    items: list[str] = body.get("items", [])
    amounts: list[int] = body.get("amounts", [])
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="multi_restock", params=(items, amounts)
        )
        result: StyxResponse = await future.get()
        return json({"total_added": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.post("/user/<user_id>/group_items_by_price_bucket")
async def group_items_by_price_bucket(request, user_id: str):
    body = request.json or {}
    items: list[str] = body.get("items", [])
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="group_items_by_price_bucket", params=(items,)
        )
        result: StyxResponse = await future.get()
        return json({"buckets": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.post("/recursion/<user_id>")
async def recursion_test(request, user_id: str):
    body = request.json or {}
    items = body.get("items", [])
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="recursion_test", params=(items,)
        )
        result: StyxResponse = await future.get()
        return json({"recursion_test": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


# ── Read-only / utility endpoints ─────────────────────────────────────────────


@app.get("/inventory_value/<user_id>")
async def inventory_value(request, user_id: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="inventory_value", params=()
        )
        result: StyxResponse = await future.get()
        return json({"inventory_value": result.response, "latency": result.styx_latency_ms})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.get("/inventory_value_gather/<user_id>")
async def inventory_value_gather(request, user_id: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="inventory_value_gather", params=()
        )
        result: StyxResponse = await future.get()
        return json({"inventory_value_gather": result.response, "latency": result.styx_latency_ms})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.get("/my_item_prices/<user_id>")
async def my_item_prices(request, user_id: str):
    try:
        future = await styx_client.send_event(operator=user_operator, key=user_id, function="my_item_prices", params=())
        result: StyxResponse = await future.get()
        return json({"my_item_prices": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


@app.get("/user/<user_id>/most_valuable_item_price")
async def most_valuable_item_price(request, user_id: str):
    try:
        future = await styx_client.send_event(
            operator=user_operator, key=user_id, function="most_valuable_item_price", params=()
        )
        result: StyxResponse = await future.get()
        return json({"most_valuable_item_price": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


# ── Item bulk create (itemidx removed) ───────────────────────────────────────

# Commented out: depended on the itemidx operator (Phase 0 created the index),
# which no longer exists. Bulk item/stock insertion is preserved here for
# reference; re-enable once a replacement for the index is decided.
# @app.post('/items/batch_create')
# async def batch_create_items(request):
#     """Create the item index, insert items with prices, and set their stock.
#
#     Body: {names, prices, stocks, idx_id}
#         names:  list[str]  e.g. ["widget","gadget","thingy"]
#         prices: list[int]  parallel to names
#         stocks: list[int]  parallel to names (0 stocks are skipped)
#         idx_id: str        index key to create (default "items")
#
#     Call this first on a fresh deployment. Phase 0 creates the index, and
#     item.insert auto-registers each item into the "items" index, so the
#     itemidx queries work afterwards. NOTE: item.insert hardcodes the index
#     name "items", so leave idx_id at its default unless the compiled
#     functions are changed to match.
#     """
#     body = request.json or {}
#     names: List[str] = body.get('names', [])
#     prices: List[int] = [int(p) for p in body.get('prices', [])]
#     stocks: List[int] = [int(s) for s in body.get('stocks', [])]
#     idx_id: str = body.get('idx_id', 'items')
#
#     if not (len(names) == len(prices) == len(stocks)):
#         return json({'error': 'names, prices, stocks must have the same length',
#                      'lengths': {'names': len(names), 'prices': len(prices), 'stocks': len(stocks)}},
#                     status=400)
#
#     start = timer()
#
#     # Phase 0: create the item index. Must fully complete before any
#     # item.insert: insert fans out to itemidx.add_item, which appends to
#     # __state__['items'] and would KeyError if the index does not exist yet.
#     idx_future = await styx_client.send_event(
#         operator=itemidx_operator, key=idx_id,
#         function='insert', params=(idx_id,)
#     )
#     await idx_future.get()
#
#     # Phase 1: insert all items. item.insert already registers each item in
#     # the "items" index, so no separate add_item pass is needed (a Phase 3
#     # add_item loop would double-register every item).
#     futures = []
#     for name, price in zip(names, prices):
#         f = await styx_client.send_event(
#             operator=item_operator, key=name,
#             function='insert', params=(name, price)
#         )
#         futures.append(f)
#     for f in futures:
#         await f.get()
#
#     # Phase 2: set stock for items with stock > 0
#     futures = []
#     for name, stock in zip(names, stocks):
#         if stock > 0:
#             f = await styx_client.send_event(
#                 operator=item_operator, key=name,
#                 function='update_stock', params=(stock,)
#             )
#             futures.append(f)
#     for f in futures:
#         await f.get()
#
#     end = timer()
#     return json({
#         'count': len(names),
#         'idx_id': idx_id,
#         'items': [{'name': n, 'price': p, 'stock': s} for n, p, s in zip(names, prices, stocks)],
#         'total_time_ms': round((end - start) * 1000, 1),
#     })


@app.get("/demo/<user_id>")
async def demo(request, user_id: str):
    try:
        future = await styx_client.send_event(operator=user_operator, key=user_id, function="demo", params=())
        result: StyxResponse = await future.get()
        return json({"demo": result.response})
    except Exception as e:
        return json({"error": str(e)}, status=400)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8002, debug=True)
