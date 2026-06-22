#!/usr/bin/env python3
"""
binance_bybit_raw_probe.py
==========================

Minimal, standalone WebSocket diagnostic for the perpetual-arb endpoints.
Connects to Binance USDⓈ-M Futures and Bybit V5 linear, prints the first few
RAW frames from each (pretty-printed), and exits. Read-only.

It exists ONLY to confirm the upstream sockets are alive and emitting the data
shapes our parser expects. It imports nothing from the core engine and places
no orders.

Run:
    pip install websockets
    python binance_bybit_raw_probe.py
"""

import asyncio
import json
import sys

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    raise SystemExit(f"this probe needs websockets:  pip install websockets ({exc})")

# Force UTF-8 so box-drawing/glyphs render on Windows consoles.
try:  # pragma: no cover - platform dependent
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BINANCE_WS_URL = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_SUBSCRIBE = {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}

FRAMES = 3            # raw payloads to print per venue
RECV_TIMEOUT = 20.0   # seconds to wait for each frame before giving up


def _show(raw) -> None:
    """Pretty-print one raw frame; fall back to repr if it isn't JSON."""
    try:
        print(json.dumps(json.loads(raw), indent=2))
    except (ValueError, TypeError):
        print(repr(raw))


async def probe_binance() -> None:
    print("=" * 60)
    print(" BINANCE USDⓈ-M Futures  —  btcusdt@bookTicker")
    print(f" {BINANCE_WS_URL}")
    print("=" * 60)
    try:
        # No subscribe frame needed: the stream is encoded in the URL path.
        async with websockets.connect(BINANCE_WS_URL, open_timeout=10) as ws:
            print("connected. first %d raw frames:" % FRAMES)
            for n in range(1, FRAMES + 1):
                raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                print(f"\n--- frame {n} ---")
                _show(raw)
    except asyncio.TimeoutError:
        print(f"(no frame within {RECV_TIMEOUT:.0f}s — endpoint silent?)")
    except Exception as exc:
        print(f"Binance probe error: {type(exc).__name__}: {exc}")


async def probe_bybit() -> None:
    print("\n" + "=" * 60)
    print(" BYBIT V5 Linear Futures  —  orderbook.1.BTCUSDT")
    print(f" {BYBIT_WS_URL}")
    print("=" * 60)
    try:
        async with websockets.connect(BYBIT_WS_URL, open_timeout=10) as ws:
            # Bybit requires an explicit subscribe frame after connecting.
            await ws.send(json.dumps(BYBIT_SUBSCRIBE))
            print(f"connected. sent subscribe: {json.dumps(BYBIT_SUBSCRIBE)}")

            printed = 0
            while printed < FRAMES:
                raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                try:
                    obj = json.loads(raw)
                except (ValueError, TypeError):
                    print("\n--- non-JSON frame ---")
                    print(repr(raw))
                    continue

                # Catch (and surface) the subscription acknowledgment, but don't
                # count it as one of the order-book frames.
                if obj.get("op") == "subscribe" or "success" in obj:
                    print("\n--- subscription ack ---")
                    print(json.dumps(obj, indent=2))
                    continue

                printed += 1
                print(f"\n--- order book update {printed} ---")
                print(json.dumps(obj, indent=2))
    except asyncio.TimeoutError:
        print(f"(no frame within {RECV_TIMEOUT:.0f}s — endpoint silent?)")
    except Exception as exc:
        print(f"Bybit probe error: {type(exc).__name__}: {exc}")


async def main() -> None:
    await probe_binance()
    await probe_bybit()
    print("\n" + "=" * 60)
    print(" Done. Both endpoints probed.")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped.")
