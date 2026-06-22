#!/usr/bin/env python3
"""
ws_mirror.py
============

Asynchronous WebSocket streaming layer that mirrors the live order books for
our matched 15-minute BTC tokens on Polymarket and Kalshi, eliminating REST
polling.

Scope / isolation
-----------------
This module's ONLY job is to maintain a near-zero-latency, in-memory reflection
of the raw top-of-book. It contains NO order-placement, NO signing of trades,
and NO money movement. It reads public market data (Polymarket) and an
authenticated *read* stream (Kalshi). Keep it that way.

Design
------
  * ``LiveBookState`` holds derived top-of-book (best_bid/best_ask, 0..1) for
    both venues, plus update counters so the printer can highlight the feed
    that just moved.
  * Two long-running tasks — ``stream_polymarket`` and ``stream_kalshi`` — each
    maintain a *local* copy of the book (price->size maps) from snapshot + delta
    frames and recompute top-of-book on every message.
  * The ``backoff`` library drives reconnects: a dropped socket backs off
    exponentially (with jitter), reconnects, RE-AUTHENTICATES (Kalshi headers
    are regenerated per connect), and re-subscribes — without crashing.
  * A printer task renders a unified matrix once per second.

Dependencies:
    pip install websockets backoff
    # Kalshi auth also needs:  pip install cryptography   (see auth_manager.py)
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

# --- optional deps with friendly errors -------------------------------------
try:
    import websockets
    from websockets.exceptions import (
        ConnectionClosed,
        InvalidStatus,
        WebSocketException,
    )
    _HAS_WEBSOCKETS = True
    _WS_IMPORT_ERR = ""
except ImportError as _exc:  # pragma: no cover
    _HAS_WEBSOCKETS = False
    _WS_IMPORT_ERR = str(_exc)
    WebSocketException = Exception  # type: ignore
    ConnectionClosed = Exception  # type: ignore
    InvalidStatus = Exception  # type: ignore

try:
    import backoff
    _HAS_BACKOFF = True
    _BACKOFF_IMPORT_ERR = ""
except ImportError as _exc:  # pragma: no cover
    _HAS_BACKOFF = False
    _BACKOFF_IMPORT_ERR = str(_exc)

# auth_manager is only needed for the Kalshi stream. Import softly so the
# Polymarket-only path still works if creds/crypto aren't set up yet.
try:
    import auth_manager
    _HAS_AUTH = True
    _AUTH_IMPORT_ERR = ""
except Exception as _exc:  # pragma: no cover
    _HAS_AUTH = False
    _AUTH_IMPORT_ERR = str(_exc)


# ---------------------------------------------------------------------------
# Endpoints / tuning
# ---------------------------------------------------------------------------

# NOTE: the user-facing CLOB host is clob.polymarket.com, but the live market
# *streaming* channel is served from the ws-subscriptions host. Both are listed;
# flip POLY_WS_URL if Polymarket changes routing.
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLY_WS_URL_ALT = "wss://clob.polymarket.com/ws"

# Kalshi v2 streaming endpoint. The signed path used for auth is the WS path.
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

PRINT_INTERVAL_SEC = 1.0
WS_PING_INTERVAL = 10
WS_PING_TIMEOUT = 10
BACKOFF_MAX_SEC = 30  # cap exponential backoff so reconnects stay responsive


# ---------------------------------------------------------------------------
# Local book maintenance
# ---------------------------------------------------------------------------

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


@dataclass
class _LocalBook:
    """A single outcome's resting book as price->size maps (raw units)."""
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()

    @staticmethod
    def _apply(side_map: Dict[float, float], price: Optional[float],
               size: Optional[float]) -> None:
        if price is None or size is None:
            return
        if size <= 0:
            side_map.pop(price, None)
        else:
            side_map[price] = size

    def set_bid(self, price, size) -> None:
        self._apply(self.bids, _to_float(price), _to_float(size))

    def set_ask(self, price, size) -> None:
        self._apply(self.asks, _to_float(price), _to_float(size))

    def best_bid(self) -> Optional[float]:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return min(self.asks) if self.asks else None


@dataclass
class Quote:
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None


@dataclass
class LiveBookState:
    """Unified, in-memory reflection of both venues' top-of-book.

    Prices are normalized to 0.00..1.00 probabilities. UP == YES, DOWN == NO.
    """
    # active instruments
    poly_yes_token: str = ""
    poly_no_token: str = ""
    kalshi_ticker: str = ""

    # derived top-of-book (what consumers read)
    poly_up: Quote = field(default_factory=Quote)
    poly_down: Quote = field(default_factory=Quote)
    kalshi_up: Quote = field(default_factory=Quote)
    kalshi_down: Quote = field(default_factory=Quote)

    # update bookkeeping (printer reads these to highlight the live feed)
    poly_updates: int = 0
    kalshi_updates: int = 0
    poly_last_ts: float = 0.0
    kalshi_last_ts: float = 0.0
    poly_connected: bool = False
    kalshi_connected: bool = False

    # PER-VENUE liveness tracking for the REST-fallback watchdog. Each stamp is
    # a monotonic time refreshed on EVERY raw WS frame from THAT venue
    # (heartbeats included), independent of whether the book changed. Tracking
    # per venue is essential: one venue can be dark (e.g. Kalshi WS needs auth)
    # while the other streams happily — a global stamp would mask that and the
    # watchdog would never fall the dark venue back to REST.
    poly_last_frame: float = 0.0
    kalshi_last_frame: float = 0.0
    poly_ws_frames: int = 0
    kalshi_ws_frames: int = 0
    poly_rest_fallback: bool = False    # this venue is currently on REST polling
    kalshi_rest_fallback: bool = False

    # Optional event hook: invoked with the feed name ("poly"/"kalshi") on every
    # book-changing frame, so consumers (e.g. the math engine) can react in the
    # same instant the top-of-book moves. Kept out of __repr__ / book logic.
    on_update: Optional[Callable[[str], None]] = None
    # Fired with the feed name on every raw WS frame arrival — used by the
    # watchdog to stand down that venue's REST fallback the instant its WS
    # connectivity is restored.
    on_ws_frame: Optional[Callable[[str], None]] = None

    def mark_ws_frame(self, feed: str) -> None:
        """Record that a real WS frame just arrived for ``feed`` (liveness)."""
        if feed == "poly":
            self.poly_ws_frames += 1
            self.poly_last_frame = time.monotonic()
        elif feed == "kalshi":
            self.kalshi_ws_frames += 1
            self.kalshi_last_frame = time.monotonic()
        if self.on_ws_frame is not None:
            try:
                self.on_ws_frame(feed)
            except Exception as exc:  # noqa: BLE001 - defensive isolation
                print(f"[ws] on_ws_frame callback error: {exc}")

    def _fire(self, feed: str) -> None:
        if self.on_update is not None:
            # Never let a consumer callback kill the stream task.
            try:
                self.on_update(feed)
            except Exception as exc:  # noqa: BLE001 - defensive isolation
                print(f"[{feed}] on_update callback error: {exc}")

    def note_poly_update(self) -> None:
        self.poly_updates += 1
        self.poly_last_ts = time.monotonic()
        self._fire("poly")

    def note_kalshi_update(self) -> None:
        self.kalshi_updates += 1
        self.kalshi_last_ts = time.monotonic()
        self._fire("kalshi")


# ---------------------------------------------------------------------------
# Polymarket stream
# ---------------------------------------------------------------------------

def _normalize_frames(payload) -> List[dict]:
    """A WS text frame may carry a single event or a list of events."""
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _poly_apply_message(
    raw: str,
    books: Dict[str, _LocalBook],
    state: LiveBookState,
) -> bool:
    """Apply a Polymarket market-channel frame to local books.

    Returns True if any tracked asset's top-of-book may have changed.
    Handles ``book`` (snapshot) and ``price_change`` (delta) event types.
    """
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return False

    changed = False
    for ev in _normalize_frames(payload):
        etype = ev.get("event_type") or ev.get("type")
        asset = str(ev.get("asset_id") or ev.get("asset") or "")
        if asset not in books:
            continue
        book = books[asset]

        if etype == "book":
            # Full snapshot: rebuild the side maps.
            book.reset()
            for lvl in ev.get("bids") or ev.get("buys") or []:
                book.set_bid(lvl.get("price"), lvl.get("size"))
            for lvl in ev.get("asks") or ev.get("sells") or []:
                book.set_ask(lvl.get("price"), lvl.get("size"))
            changed = True

        elif etype == "price_change":
            # Incremental deltas: each change updates one (side, price) level.
            for ch in ev.get("changes") or []:
                side = str(ch.get("side", "")).upper()
                price, size = ch.get("price"), ch.get("size")
                if side in ("BUY", "BID"):
                    book.set_bid(price, size)
                elif side in ("SELL", "ASK"):
                    book.set_ask(price, size)
            changed = True

    if changed:
        _poly_refresh(books, state)
    return changed


def _poly_refresh(books: Dict[str, _LocalBook], state: LiveBookState) -> None:
    up = books.get(state.poly_yes_token)
    down = books.get(state.poly_no_token)
    if up is not None:
        state.poly_up = Quote(up.best_bid(), up.best_ask())
    if down is not None:
        state.poly_down = Quote(down.best_bid(), down.best_ask())
    state.note_poly_update()


def _poly_subscribe_frame(token_ids: List[str]) -> str:
    # Polymarket market channel subscription.
    return json.dumps({"type": "market", "assets_ids": token_ids})


async def stream_polymarket(state: LiveBookState, stop: asyncio.Event) -> None:
    """Long-running Polymarket book mirror with exponential-backoff reconnect."""
    token_ids = [t for t in (state.poly_yes_token, state.poly_no_token) if t]
    if not token_ids:
        print("[poly] no token ids configured; skipping stream.")
        return

    @_reconnecting("poly", stop)
    async def _run() -> None:
        books = {tid: _LocalBook() for tid in token_ids}
        async with _ws_connect(POLY_WS_URL) as ws:
            state.poly_connected = True
            await ws.send(_poly_subscribe_frame(token_ids))
            print(f"[poly] connected & subscribed to {len(token_ids)} tokens")
            async for raw in ws:
                if stop.is_set():
                    return
                state.mark_ws_frame("poly")  # liveness: a real frame arrived
                _poly_apply_message(raw, books, state)

    try:
        await _run()
    finally:
        state.poly_connected = False


# ---------------------------------------------------------------------------
# Kalshi stream (authenticated)
# ---------------------------------------------------------------------------

def _kalshi_price(p) -> Optional[float]:
    """Kalshi WS prices are dollar strings already in 0..1 (e.g. '0.8700').

    Defensively divide by 100 if a value somehow arrives as cents (>1).
    """
    f = _to_float(p)
    if f is None:
        return None
    if f > 1.0:           # legacy cents -> dollars
        f = f / 100.0
    return f


def _kalshi_apply_message(
    raw: str,
    yes_book: _LocalBook,
    no_book: _LocalBook,
    state: LiveBookState,
) -> bool:
    """Apply a Kalshi WS frame (orderbook_snapshot / orderbook_delta).

    Live Kalshi WS format (verified):
      * snapshot msg has ``yes_dollars_fp`` / ``no_dollars_fp``: lists of
        [price_str, size_str] where price is ALREADY in dollars (0..1).
      * delta msg has ``price_dollars``, ``delta_fp`` (signed size change),
        and ``side`` ("yes"/"no").
    Each side lists resting *bids*; asks are the complement (1 - opposite bid).
    Legacy key names (yes/no, price/delta, *_dollars) are accepted as fallback.
    """
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return False

    mtype = payload.get("type")
    msg = payload.get("msg") or {}
    changed = False

    if mtype == "orderbook_snapshot":
        yes_book.reset()
        no_book.reset()
        yes_levels = (msg.get("yes_dollars_fp") or msg.get("yes_dollars")
                      or msg.get("yes") or [])
        no_levels = (msg.get("no_dollars_fp") or msg.get("no_dollars")
                     or msg.get("no") or [])
        for lvl in yes_levels:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                yes_book.set_bid(_kalshi_price(lvl[0]), _to_float(lvl[1]))
        for lvl in no_levels:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                no_book.set_bid(_kalshi_price(lvl[0]), _to_float(lvl[1]))
        changed = True

    elif mtype == "orderbook_delta":
        side = str(msg.get("side", "")).lower()
        price = _kalshi_price(msg.get("price_dollars", msg.get("price")))
        # Signed CHANGE in resting size at that price level.
        delta = _to_float(msg.get("delta_fp", msg.get("delta")))
        target = yes_book if side == "yes" else no_book if side == "no" else None
        if target is not None and price is not None and delta is not None:
            new_size = target.bids.get(price, 0.0) + delta
            target.set_bid(price, new_size if new_size > 0 else 0)
            changed = True

    if changed:
        _kalshi_refresh(yes_book, no_book, state)
    return changed


def _kalshi_refresh(
    yes_book: _LocalBook, no_book: _LocalBook, state: LiveBookState
) -> None:
    # Best resting bid on each side (prices already 0..1). Asks are the
    # complement of the opposite side's best bid.
    yes_bid = yes_book.best_bid()
    no_bid = no_book.best_bid()
    yes_ask = round(1.0 - no_bid, 4) if no_bid is not None else None
    no_ask = round(1.0 - yes_bid, 4) if yes_bid is not None else None
    state.kalshi_up = Quote(yes_bid, yes_ask)
    state.kalshi_down = Quote(no_bid, no_ask)
    state.note_kalshi_update()


def _kalshi_subscribe_frame(ticker: str) -> str:
    return json.dumps(
        {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": [ticker],
            },
        }
    )


def _kalshi_auth_headers() -> Dict[str, str]:
    """Generate fresh signed Kalshi WS headers via auth_manager.

    Regenerated on every (re)connect so the timestamp/signature are current.
    Raises AuthError if creds/crypto are unavailable.
    """
    if not _HAS_AUTH:
        raise RuntimeError(
            f"auth_manager unavailable for Kalshi WS auth: {_AUTH_IMPORT_ERR}"
        )
    mgr = auth_manager.AuthManager(require_kalshi=True, require_polymarket=False)
    # Kalshi WS auth signs the GET on the WS path, same RSA-PSS scheme as REST.
    return mgr.kalshi.sign("GET", KALSHI_WS_PATH)


async def stream_kalshi(state: LiveBookState, stop: asyncio.Event) -> None:
    """Long-running authenticated Kalshi book mirror with backoff reconnect."""
    if not state.kalshi_ticker:
        print("[kalshi] no ticker configured; skipping stream.")
        return
    # Fail fast & clearly if we can't auth — but DON'T crash the program.
    try:
        _kalshi_auth_headers()
    except Exception as exc:
        print(f"[kalshi] auth unavailable, stream disabled: {exc}")
        return

    @_reconnecting("kalshi", stop)
    async def _run() -> None:
        yes_book, no_book = _LocalBook(), _LocalBook()
        headers = _kalshi_auth_headers()  # re-auth fresh on every connect
        async with _ws_connect(KALSHI_WS_URL, headers=headers) as ws:
            state.kalshi_connected = True
            await ws.send(_kalshi_subscribe_frame(state.kalshi_ticker))
            print(f"[kalshi] connected, authed & subscribed to "
                  f"{state.kalshi_ticker}")
            async for raw in ws:
                if stop.is_set():
                    return
                state.mark_ws_frame("kalshi")  # liveness: a real frame arrived
                _kalshi_apply_message(raw, yes_book, no_book, state)

    try:
        await _run()
    finally:
        state.kalshi_connected = False


# ---------------------------------------------------------------------------
# Connection plumbing: ws connect helper + backoff reconnect decorator
# ---------------------------------------------------------------------------

def _ws_connect(url: str, headers: Optional[Dict[str, str]] = None):
    """Return a websockets connect context manager, tolerant of lib versions.

    websockets >=13 uses ``additional_headers``; older uses ``extra_headers``.
    """
    if not _HAS_WEBSOCKETS:
        raise RuntimeError(
            f"The 'websockets' package is required: {_WS_IMPORT_ERR}"
        )
    common = dict(ping_interval=WS_PING_INTERVAL, ping_timeout=WS_PING_TIMEOUT,
                  close_timeout=5)
    if headers:
        try:
            return websockets.connect(url, additional_headers=headers, **common)
        except TypeError:
            return websockets.connect(url, extra_headers=headers, **common)
    return websockets.connect(url, **common)


def _reconnecting(name: str, stop: asyncio.Event):
    """Decorator: keep a streaming coroutine alive across drops via backoff.

    Exponential backoff (capped, jittered, unlimited retries) on network/socket
    errors. ``stop`` set -> stop retrying and return cleanly. Falls back to a
    hand-rolled loop if the ``backoff`` library isn't installed.
    """
    retryable = (
        ConnectionClosed, WebSocketException, InvalidStatus,
        OSError, asyncio.TimeoutError,
    )

    def _giveup(_exc) -> bool:
        return stop.is_set()

    def _log(details) -> None:
        wait = details.get("wait", 0.0)
        tries = details.get("tries", 0)
        print(f"[{name}] disconnected — reconnecting in {wait:0.1f}s "
              f"(attempt #{tries})")

    def decorator(coro):
        if _HAS_BACKOFF:
            wrapped = backoff.on_exception(
                backoff.expo,
                retryable,
                max_value=BACKOFF_MAX_SEC,
                jitter=backoff.full_jitter,
                giveup=_giveup,
                on_backoff=_log,
                logger=None,
            )(coro)

            async def runner() -> None:
                while not stop.is_set():
                    try:
                        await wrapped()
                        # Clean return (e.g. server closed politely): loop and
                        # reconnect immediately with a fresh backoff sequence.
                        if stop.is_set():
                            return
                    except retryable as exc:
                        # backoff gave up only because stop was set.
                        if stop.is_set():
                            return
                        print(f"[{name}] gave up after error: {exc}")
                        return
            return runner

        # --- manual fallback if backoff isn't installed ---
        async def manual_runner() -> None:
            delay = 1.0
            while not stop.is_set():
                try:
                    await coro()
                    delay = 1.0
                except retryable as exc:
                    if stop.is_set():
                        return
                    print(f"[{name}] disconnected ({exc}); retry in {delay:0.1f}s")
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, BACKOFF_MAX_SEC)
        return manual_runner

    return decorator


# ---------------------------------------------------------------------------
# Terminal matrix printer
# ---------------------------------------------------------------------------

def _fmt(v: Optional[float]) -> str:
    return f"{v:0.3f}" if isinstance(v, (int, float)) else "  -  "


async def print_matrix(state: LiveBookState, stop: asyncio.Event) -> None:
    """Render the unified book once per second, flagging the live feed."""
    # Seed from current counters so the FIRST frame doesn't falsely flag both
    # feeds as just-updated.
    prev_poly, prev_kalshi = state.poly_updates, state.kalshi_updates
    while not stop.is_set():
        poly_hot = state.poly_updates != prev_poly
        kalshi_hot = state.kalshi_updates != prev_kalshi
        prev_poly, prev_kalshi = state.poly_updates, state.kalshi_updates

        def tag(connected: bool, hot: bool) -> str:
            if not connected:
                return "○ down"
            return "◀ LIVE" if hot else "● ok  "

        ts = time.strftime("%H:%M:%S")
        print(f"\n┌─ [{ts}] WS MIRROR ── {state.kalshi_ticker or 'n/a'} "
              f"── poll-free ─")
        print(f"│ {'FEED':<12}{'OUTCOME':<8}{'bid':>9}{'ask':>9}   status")
        print(f"│ {'Polymarket':<12}{'UP/YES':<8}"
              f"{_fmt(state.poly_up.best_bid):>9}{_fmt(state.poly_up.best_ask):>9}"
              f"   {tag(state.poly_connected, poly_hot)}")
        print(f"│ {'':<12}{'DOWN/NO':<8}"
              f"{_fmt(state.poly_down.best_bid):>9}{_fmt(state.poly_down.best_ask):>9}")
        print(f"│ {'Kalshi':<12}{'UP/YES':<8}"
              f"{_fmt(state.kalshi_up.best_bid):>9}{_fmt(state.kalshi_up.best_ask):>9}"
              f"   {tag(state.kalshi_connected, kalshi_hot)}")
        print(f"│ {'':<12}{'DOWN/NO':<8}"
              f"{_fmt(state.kalshi_down.best_bid):>9}{_fmt(state.kalshi_down.best_ask):>9}")
        print(f"└ updates  poly={state.poly_updates}  kalshi={state.kalshi_updates}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=PRINT_INTERVAL_SEC)
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run_mirror(
    poly_yes_token: str,
    poly_no_token: str,
    kalshi_ticker: str,
) -> None:
    """Spin up both streams + the printer and run until interrupted."""
    state = LiveBookState(
        poly_yes_token=poly_yes_token,
        poly_no_token=poly_no_token,
        kalshi_ticker=kalshi_ticker,
    )
    stop = asyncio.Event()

    tasks = [
        asyncio.create_task(stream_polymarket(state, stop), name="poly"),
        asyncio.create_task(stream_kalshi(state, stop), name="kalshi"),
        asyncio.create_task(print_matrix(state, stop), name="printer"),
    ]
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _discover_and_run() -> None:
    """Convenience entry: reuse the sim's discovery to get the active pair."""
    try:
        import httpx
        import fast_crypto_arb as f
    except Exception as exc:
        raise SystemExit(
            f"Auto-discovery needs fast_crypto_arb + httpx: {exc}"
        )
    async with httpx.AsyncClient(follow_redirects=True,
                                 headers={"User-Agent": "ws-mirror/1.0"}) as c:
        pair = await f.discover(c)
    if pair is None:
        raise SystemExit("Could not discover an active 15m BTC pair.")
    print(f"Mirroring: Poly[{pair.poly_question}]  Kalshi[{pair.kalshi_ticker}]")
    await run_mirror(pair.poly_yes_token, pair.poly_no_token, pair.kalshi_ticker)


def _preflight() -> None:
    print("=" * 60)
    print(" ws_mirror — live book streaming (read-only, no order logic)")
    print("=" * 60)
    print(f"  websockets installed : {_HAS_WEBSOCKETS}")
    print(f"  backoff installed    : {_HAS_BACKOFF} "
          f"{'' if _HAS_BACKOFF else '(falling back to manual reconnect)'}")
    print(f"  auth_manager loaded  : {_HAS_AUTH}")
    if not _HAS_WEBSOCKETS:
        raise SystemExit(f"Install websockets first: {_WS_IMPORT_ERR}")


if __name__ == "__main__":
    _preflight()
    try:
        asyncio.run(_discover_and_run())
    except KeyboardInterrupt:
        print("\n👋 Stopped. (Mirror only — no positions were ever touched.)")
