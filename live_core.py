#!/usr/bin/env python3
"""
live_core.py
============

Event-driven cross-venue PERPETUAL arbitrage mirror:
Binance USDⓈ-M Futures  ×  Bybit V5 Linear Futures, on the BTCUSDT contract.

Both venues' best bid/ask are streamed over WebSocket and folded — frame by
frame — into a fee-aware arbitrage gate. When one venue's ask sits below the
other venue's bid by more than the round-trip taker fee, the gate opens a
single simulated position and locks until the spread converges.

Pipeline
--------
    connect Binance @bookTicker  ─┐
    connect Bybit orderbook.1 ────┤→ PerpBookState.on_update (per frame)
                                   │      → _ArbGate re-checks the cross-spread
                                   │        INSTANTLY (no polling)
                                   │      → in-memory ledger opens / closes the
                                   │        paper position on the live book
    staleness watchdog ───────────┘

Preserved from the prior architecture (unchanged in spirit)
-----------------------------------------------------------
  * Fully asynchronous: one task per stream + printer + watchdog, coordinated
    by a single ``asyncio.Event`` stop signal, torn down via ``asyncio.gather``.
  * EXECUTION STATE LOCK (``is_in_flight``): one entry + one exit per spread,
    immune to high-frequency frame storms.
  * MONOTONIC time counters: the post-exit cooldown uses ``time.monotonic()``
    exclusively, so it is immune to wall-clock jumps.
  * The once-per-second performance matrix logger.

Read-only market data: the BTCUSDT book streams are PUBLIC and need no auth.
Exchange API credentials, if present in a local ``.env``, are loaded securely
(never hardcoded, never logged) and reserved for the optional authenticated
reconciliation layer — they are NOT used by the public read-only streams.

Run:
    pip install websockets python-dotenv
    python live_core.py
"""

import argparse
import asyncio
import csv
import json
import os
import queue
import random
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

# Windows consoles default to cp1252 and crash on the emoji/box-drawing glyphs
# below. Force UTF-8 so the matrix logger renders everywhere.
try:  # pragma: no cover - platform dependent
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Optional: python-dotenv loads a local .env into os.environ. If it isn't
# installed we degrade gracefully and read straight from the real environment.
try:
    from dotenv import load_dotenv
    _HAS_DOTENV = True
except ImportError:
    _HAS_DOTENV = False

    def load_dotenv(*_a, **_k):  # no-op shim
        return False

try:
    import websockets
    _HAS_WEBSOCKETS = True
    _WS_IMPORT_ERR = None
except ImportError as exc:  # pragma: no cover
    websockets = None
    _HAS_WEBSOCKETS = False
    _WS_IMPORT_ERR = exc


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# --- Target asset universe -------------------------------------------------
# Each symbol maps a canonical name to the per-exchange ticker formats. High
# volatility + frequent multi-venue dislocations make these good arb targets.
# Add/remove entries here to reshape the tracked universe; everything else
# (streams, books, gates, dashboard) scales off this list automatically.


@dataclass(frozen=True)
class SymbolSpec:
    name: str        # canonical display name, e.g. "SOL"
    binance: str     # Binance USDⓈ-M stream symbol, e.g. "solusdt"
    bybit: str       # Bybit V5 linear symbol, e.g. "SOLUSDT"
    okx: str         # OKX swap instId, e.g. "SOL-USDT-SWAP"


TARGET_SYMBOLS = [
    SymbolSpec("SOL",  "solusdt",  "SOLUSDT",  "SOL-USDT-SWAP"),
    SymbolSpec("DOGE", "dogeusdt", "DOGEUSDT", "DOGE-USDT-SWAP"),
    # AVAX replaces PEPE: PEPE only lists as the 1000x-scaled "1000PEPEUSDT" on
    # Binance/Bybit, which would not price-match OKX's unscaled PEPE-USDT-SWAP.
    # AVAX is high-vol and lists 1:1 with identical naming on all three venues.
    SymbolSpec("AVAX", "avaxusdt", "AVAXUSDT", "AVAX-USDT-SWAP"),
    SymbolSpec("WIF",  "wifusdt",  "WIFUSDT",  "WIF-USDT-SWAP"),
    SymbolSpec("APT",  "aptusdt",  "APTUSDT",  "APT-USDT-SWAP"),
]

# Master catalog keyed by canonical name. config.json's ``target_markets`` is
# resolved against this, so the file selects a subset of known, price-matched
# contracts rather than inventing arbitrary tickers the streams couldn't follow.
SYMBOL_CATALOG = {s.name: s for s in TARGET_SYMBOLS}

# Binance USDⓈ-M combined stream: one socket multiplexes every symbol's
# bookTicker. Frames arrive wrapped as {"stream": "<sym>@bookTicker", "data": …}.
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="

# Bybit V5 + OKX V5 public hosts: one socket each, subscribed to ALL symbols.
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


def binance_combined_url(specs) -> str:
    """One combined-stream URL covering every symbol's @bookTicker."""
    return BINANCE_WS_BASE + "/".join(f"{s.binance}@bookTicker" for s in specs)


def bybit_subscribe_frames(specs):
    """ONE subscribe frame PER symbol. Bybit rejects an entire batched subscribe
    if any single topic is invalid (e.g. a delisted/misnamed ticker), so each
    symbol is isolated in its own frame — one bad ticker can't silence the rest."""
    return [{"op": "subscribe", "args": [f"orderbook.1.{s.bybit}"]} for s in specs]


def okx_subscribe(specs) -> dict:
    """A single OKX subscribe frame listing every symbol's bbo-tbt channel."""
    return {"op": "subscribe",
            "args": [{"channel": "bbo-tbt", "instId": s.okx} for s in specs]}


# --- Taker fees ------------------------------------------------------------
# Standard exchange taker rate charged on EVERY fill (0.10%). A complete
# arbitrage round-trip fills each leg TWICE — once to OPEN (buy venue-A /
# sell venue-B) and once to CLOSE (sell venue-A / buy venue-B on convergence) —
# so the taker cost the edge math must charge PER LEG is entry + exit = 2× the
# per-fill rate. ``TAKER_FEE`` therefore stays the single per-leg constant used
# structurally in the edge formula, now valued at the full round-trip cost.
#
# NOTE: this is a *simulation* fee assumption only. Nothing here debits a real
# balance — all fills are paper (see SimulatedBroker / dry_run). Live venue
# fee tiers / native discounts (e.g. BNB on Binance) would be reconciled by the
# authenticated layer, which is a deliberately-unimplemented stub.
TAKER_FEE_PER_FILL = 0.001                 # 0.10% taker, charged on every fill
TAKER_FEE = 2.0 * TAKER_FEE_PER_FILL       # entry + exit ⇒ round-trip per leg

# --- Hard order-size ceiling (defence-in-depth) ----------------------------
# A strict physical per-clip ceiling (USD). Every intended stake is clamped to
# this BEFORE it can touch the simulated ledger, so no arithmetic blow-up can
# size a clip past the ceiling — even though dry-run never sends a real order.
MAX_ORDER_USD = 10.50                       # hard cap: no clip may exceed this
ORDER_WARN_USD = 11.00                      # above this → a LOUD clamp warning

# --- Circuit-breaker thresholds --------------------------------------------
# Strict trading circuit breaker. Tripping LATCHES a portfolio-wide halt (no
# new entries) and pauses the trading loop WITHOUT killing the process, so the
# streams + dashboard stay live for inspection. Tuned generously so normal
# operation never trips, but a runaway loop or a losing streak does.
CB_WINDOW_SEC = 60.0                        # rolling window for the rate limit
CB_MAX_TRADES_PER_MIN = 30                  # max entries opened per window
CB_MAX_CONSECUTIVE_LOSSES = 3               # consecutive losing round-trips → halt
# Execution-health monitors (added alongside loss/rate):
#  * MISSED-FILL STREAK — the gate keeps finding tradeable edges but the fills
#    are rejected (quote pulled / friction erased the edge). A sustained run is
#    an execution-degradation signature. Throttled so per-frame rejection storms
#    accrue at a human timescale, and reset by any successful fill.
#  * EDGE DEGRADATION — the rolling-average REALISED edge of the trades we do
#    capture has compressed below a floor (friction is eating the edge).
CB_MAX_MISSED_FILL_STREAK = 8               # consecutive missed fills → halt
CB_MISSED_FILL_MIN_GAP_SEC = 1.0            # count a miss at most this often
CB_EDGE_WINDOW = 5                          # rolling sample of realised edges
CB_MIN_AVG_EDGE_BPS = 0.5                   # avg realised edge below this → halt

# Notional staked per simulated arbitrage (USD).
PAPER_STAKE_USD = 100.0

# After an exit, ignore new entries for this many seconds so a lingering spread
# can't be farmed millisecond after millisecond. (Monotonic clock.)
TRADE_COOLDOWN_SEC = 2.0

# Persistent trade journal: every executed (here: simulated) round-trip is
# appended here as one CSV row, flushed instantly for crash durability.
TRADE_LOG_PATH = "trades.csv"

# Operator-tunable knobs (size, spread hurdle, market list, dry-run) are read
# from this JSON file at startup so thresholds change without code edits.
CONFIG_PATH = "config.json"

# Loop / liveness cadences.
PRINT_INTERVAL_SEC = 1.0
WATCHDOG_TICK_SEC = 2.0
RECV_TIMEOUT_SEC = 20.0           # WS read timeout before we ping / reconnect
DEFAULT_STALE_THRESHOLD = 15.0    # seconds of silence before a venue is "stale"

# Reconnect backoff: deterministic exponential 2s, 4s, 8s, 16s ... capped.
_BACKOFF_BASE = 2.0
_BACKOFF_CAP = 60.0

# ANSI colors for the gate / logger.
RESET = "\033[0m"
GRAY = "\033[90m"
GREEN = "\033[92m"
PURPLE = "\033[95m"
YELLOW = "\033[93m"
RED = "\033[91m"


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _ts_ms() -> str:
    """UTC ISO-8601 timestamp with MILLISECOND precision (diagnostic logs)."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _fmt(x) -> str:
    return f"{x:,.2f}" if x else "  --  "


def _fmt_px(x) -> str:
    """Adaptive price formatter: altcoins span 6 orders of magnitude (PEPE at
    ~0.00001 to SOL at ~150), so scale the precision to the price."""
    if not x:
        return "    --    "
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}"
    return f"{x:.8f}"


# ---------------------------------------------------------------------------
# Feature 1 — secure credential handling (os + dotenv, never hardcoded/logged)
# ---------------------------------------------------------------------------

@dataclass
class ExchangeCredentials:
    """API credentials pulled from the environment. Secrets are never logged.

    The public BTCUSDT book streams need no auth, so these stay unused unless an
    authenticated layer (e.g. balance reconciliation) is explicitly enabled.
    """
    binance_key: "str | None" = None
    binance_secret: "str | None" = None
    bybit_key: "str | None" = None
    bybit_secret: "str | None" = None
    okx_key: "str | None" = None
    okx_secret: "str | None" = None
    okx_passphrase: "str | None" = None   # OKX private API also needs a passphrase

    @property
    def has_binance(self) -> bool:
        return bool(self.binance_key and self.binance_secret)

    @property
    def has_bybit(self) -> bool:
        return bool(self.bybit_key and self.bybit_secret)

    @property
    def has_okx(self) -> bool:
        return bool(self.okx_key and self.okx_secret and self.okx_passphrase)

    @property
    def has_all(self) -> bool:
        return self.has_binance and self.has_bybit and self.has_okx


def load_credentials(env_path: str = ".env") -> "ExchangeCredentials":
    """Load exchange credentials from a local ``.env`` (if present) + the real
    environment. No hardcoded placeholders; missing keys simply stay ``None``.

    ``load_dotenv`` is a no-op when python-dotenv isn't installed or the file is
    absent, so this never raises and never blocks startup.
    """
    load_dotenv(env_path)
    return ExchangeCredentials(
        binance_key=os.environ.get("BINANCE_API_KEY"),
        binance_secret=os.environ.get("BINANCE_SECRET_KEY"),
        bybit_key=os.environ.get("BYBIT_API_KEY"),
        bybit_secret=os.environ.get("BYBIT_SECRET_KEY"),
        okx_key=os.environ.get("OKX_API_KEY"),
        okx_secret=os.environ.get("OKX_SECRET_KEY"),
        okx_passphrase=os.environ.get("OKX_PASSPHRASE"),
    )


# ---------------------------------------------------------------------------
# Feature 2 — dynamic configuration loader (externalized thresholds)
#
# Every execution knob the operator might tune — trade size, the spread hurdle,
# the tracked market list, and the dry-run safety switch — lives in config.json
# instead of being hardcoded through the pipeline. The init phase parses the
# file into one validated ``BotConfig`` object; the engine reads ONLY from that
# object, so a threshold change is a one-line JSON edit, never a code change.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RealismParams:
    """Execution-realism knobs (config.json ``realism`` block).

    With ``enabled`` False (default) the simulator fills perfectly at top-of-book
    — the original idealised behaviour. With it True, simulated fills suffer the
    frictions a real cross-venue taker would: slippage + market impact, latency-
    driven adverse selection, occasional missed / partial fills, the odd failed
    leg (→ unhedged exposure), and perp funding accrual over the hold.
    """
    enabled: bool = False             # master switch (OFF ⇒ idealised fills)
    # Per-effect ON/OFF switches. Independent of the magnitudes below, so any
    # single friction can be silenced WITHOUT losing its tuned value. All ON by
    # default; an effect bites only when BOTH ``enabled`` and its switch are on.
    slippage: bool = True              # gates slippage_bps + impact_bps_per_10k
    latency: bool = True               # gates latency adverse selection
    missed_fills: bool = True          # gates fill_probability (no-fill draws)
    partial_fills: bool = True         # gates partial_fill_prob
    leg_failure: bool = True           # gates leg_failure_prob (unhedged risk)
    funding: bool = True               # gates funding_rate_8h_bps
    # Magnitudes.
    slippage_bps: float = 1.0          # taker slip past top-of-book, per leg
    slippage_mult: float = 1.0         # adjustable multiplier on slippage_bps
    impact_bps_per_10k: float = 2.5    # extra bps of impact per $10k of size
    latency_ms: float = 230.0            # signal→fill wire time
    latency_adverse_bps: float = 0.5   # worst-case adverse drift per 100ms
    fill_probability: float = 0.88      # P(the resting quote is still there)
    partial_fill_prob: float = 0.05     # P(we only get part of the size)
    partial_fill_min_ratio: float = 0.40  # floor on a partial fill ratio
    leg_failure_prob: float = 0.01     # P(one leg fails → unhedged exposure)
    funding_rate_8h_bps: float = 0.05   # perp funding drag per 8h (may be < 0)
    seed: int = 1337                   # RNG seed → fully reproducible runs


@dataclass(frozen=True)
class BotConfig:
    """Parsed + validated runtime configuration (mirrors config.json).

    * ``dry_run``        — when True (default), every fill is SIMULATED and
                           journaled to trades.csv, but NO exchange order-
                           submission endpoint is ever called (SimulatedBroker
                           only). Flipping it off routes to the LiveBroker
                           adapters, which are deliberately unimplemented stubs.
    * ``order_size_usd`` — notional staked per arbitrage round-trip.
    * ``min_spread_bps`` — extra net-of-fee edge (basis points) an opportunity
                           must clear, ON TOP of the round-trip taker buffer,
                           before the gate opens a position. 0 = original
                           behaviour (any positive net edge trades).
    * ``target_markets`` — canonical symbols to track, resolved against
                           SYMBOL_CATALOG (unknown names dropped with a warning).
    """
    dry_run: bool = True
    order_size_usd: float = PAPER_STAKE_USD
    min_spread_bps: float = 0.0
    target_markets: tuple = field(
        default_factory=lambda: tuple(s.name for s in TARGET_SYMBOLS))
    realism: "RealismParams" = field(default_factory=RealismParams)
    source: str = "built-in defaults"     # provenance, for the startup banner
    warnings: tuple = ()                  # non-fatal parse notes (e.g. unknowns)

    @property
    def specs(self):
        """Resolve ``target_markets`` → SymbolSpec objects, in configured order."""
        return [SYMBOL_CATALOG[name] for name in self.target_markets]


def _cfg_number(raw: dict, key: str, default: float, *,
                minimum: "float | None" = None, allow_zero: bool = True) -> float:
    """Validate one numeric config field. Rejects bools (a JSON ``true`` is NOT
    a number here) and out-of-range values with a clear, fail-fast SystemExit."""
    if key not in raw:
        return float(default)
    val = raw[key]
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise SystemExit(f"config: '{key}' must be a number, got "
                         f"{type(val).__name__}.")
    val = float(val)
    if minimum is not None and val < minimum:
        raise SystemExit(f"config: '{key}' must be >= {minimum}, got {val}.")
    if not allow_zero and val == 0.0:
        raise SystemExit(f"config: '{key}' must be greater than 0, got {val}.")
    return val


def _resolve_markets(raw: dict):
    """Map requested ``target_markets`` names onto known catalog symbols.

    Returns ``(resolved_names_tuple, warnings_list)``. Unknown names are dropped
    with a warning (not fatal); an empty / all-unknown list falls back to the
    full catalog so the bot always has something live to track.
    """
    warnings = []
    if "target_markets" not in raw:
        return tuple(SYMBOL_CATALOG), warnings
    requested = raw["target_markets"]
    if (not isinstance(requested, list)
            or not all(isinstance(x, str) for x in requested)):
        raise SystemExit("config: 'target_markets' must be a list of strings.")
    resolved, unknown = [], []
    for name in requested:
        key = name.strip().upper()
        if key in SYMBOL_CATALOG:
            if key not in resolved:          # dedupe, keep first-seen order
                resolved.append(key)
        else:
            unknown.append(name)
    if unknown:
        warnings.append(f"unknown target_markets ignored {unknown} "
                        f"(known: {sorted(SYMBOL_CATALOG)})")
    if not resolved:
        warnings.append("no valid target_markets — falling back to the full "
                        f"catalog {sorted(SYMBOL_CATALOG)}")
        resolved = list(SYMBOL_CATALOG)
    return tuple(resolved), warnings


def _parse_realism(raw: dict) -> "RealismParams":
    """Validate the optional ``realism`` block. Absent → disabled defaults."""
    sub = raw.get("realism")
    if sub is None:
        return RealismParams()
    if not isinstance(sub, dict):
        raise SystemExit("config: 'realism' must be a JSON object.")
    enabled = sub.get("enabled", False)
    if not isinstance(enabled, bool):
        raise SystemExit("config: 'realism.enabled' must be a boolean.")

    def flag(key, default=True):
        if key not in sub:
            return default
        v = sub[key]
        if not isinstance(v, bool):
            raise SystemExit(f"config: 'realism.{key}' must be a boolean.")
        return v

    def num(key, default, *, minimum=0.0, maximum=None):
        if key not in sub:
            return float(default)
        v = sub[key]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise SystemExit(f"config: 'realism.{key}' must be a number.")
        v = float(v)
        if minimum is not None and v < minimum:
            raise SystemExit(f"config: 'realism.{key}' must be >= {minimum}.")
        if maximum is not None and v > maximum:
            raise SystemExit(f"config: 'realism.{key}' must be <= {maximum}.")
        return v

    seed = sub.get("seed", 1337)
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise SystemExit("config: 'realism.seed' must be an integer.")

    return RealismParams(
        enabled=enabled,
        slippage=flag("slippage"),
        latency=flag("latency"),
        missed_fills=flag("missed_fills"),
        partial_fills=flag("partial_fills"),
        leg_failure=flag("leg_failure"),
        funding=flag("funding"),
        slippage_bps=num("slippage_bps", 0.0),
        slippage_mult=num("slippage_mult", 1.0),
        impact_bps_per_10k=num("impact_bps_per_10k", 0.0),
        latency_ms=num("latency_ms", 0.0),
        latency_adverse_bps=num("latency_adverse_bps", 0.0),
        fill_probability=num("fill_probability", 1.0, maximum=1.0),
        partial_fill_prob=num("partial_fill_prob", 0.0, maximum=1.0),
        partial_fill_min_ratio=num("partial_fill_min_ratio", 1.0, maximum=1.0),
        leg_failure_prob=num("leg_failure_prob", 0.0, maximum=1.0),
        # Funding can be a carry (negative) as well as a drag (positive).
        funding_rate_8h_bps=num("funding_rate_8h_bps", 0.0, minimum=None),
        seed=seed,
    )


def _config_from_dict(raw: dict, source: str) -> "BotConfig":
    """Validate a raw config dict → ``BotConfig`` (used by both the file loader
    and the editor, so an in-memory edit is checked with identical rules before
    it is ever written to disk)."""
    if not isinstance(raw, dict):
        raise SystemExit(f"config: must be a JSON object (got "
                         f"{type(raw).__name__}).")

    dry_run = raw.get("dry_run", True)
    if not isinstance(dry_run, bool):
        raise SystemExit("config: 'dry_run' must be a boolean (true/false).")

    order_size = _cfg_number(raw, "order_size_usd", PAPER_STAKE_USD,
                             minimum=0.0, allow_zero=False)
    min_spread = _cfg_number(raw, "min_spread_bps", 0.0, minimum=0.0)
    markets, warnings = _resolve_markets(raw)
    realism = _parse_realism(raw)

    return BotConfig(
        dry_run=dry_run,
        order_size_usd=order_size,
        min_spread_bps=min_spread,
        target_markets=markets,
        realism=realism,
        source=source,
        warnings=tuple(warnings),
    )


def load_config(path: str = CONFIG_PATH) -> "BotConfig":
    """Read + validate config.json into a ``BotConfig``. Robust by design:

    * Missing file        → built-in defaults (startup is never blocked).
    * Malformed JSON / wrong type / out-of-range value → a clear SystemExit, so
      a typo can never silently arm the wrong size, hurdle, or live mode.
    * Unknown ``target_markets`` → dropped with a warning (non-fatal).
    """
    if not os.path.exists(path):
        return BotConfig(source=f"built-in defaults (no {path} found)")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (ValueError, OSError) as exc:
        raise SystemExit(f"config: could not read/parse {path}: {exc}")
    if not isinstance(raw, dict):
        raise SystemExit(f"config: {path} must contain a JSON object "
                         f"(got {type(raw).__name__}).")
    return _config_from_dict(raw, source=path)


# ---------------------------------------------------------------------------
# Config EDITOR — `python live_core.py config ...` (view / set / toggle effects)
# ---------------------------------------------------------------------------

# The six toggleable realism effects (each maps to a RealismParams bool switch).
REALISM_EFFECTS = ("slippage", "latency", "missed_fills", "partial_fills",
                   "leg_failure", "funding")

_TOP_KEYS = {"dry_run", "order_size_usd", "min_spread_bps", "target_markets"}
_REALISM_KEYS = ({"enabled", "seed", "slippage_bps", "slippage_mult",
                  "impact_bps_per_10k",
                  "latency_ms", "latency_adverse_bps", "fill_probability",
                  "partial_fill_prob", "partial_fill_min_ratio",
                  "leg_failure_prob", "funding_rate_8h_bps"}
                 | set(REALISM_EFFECTS))


def _default_config_dict() -> dict:
    """The template written by ``config --init`` (mirrors the shipped config)."""
    return {
        "dry_run": True,
        "order_size_usd": PAPER_STAKE_USD,
        "min_spread_bps": 1.0,
        "target_markets": [s.name for s in TARGET_SYMBOLS],
        "realism": {
            "enabled": True,
            "slippage": True, "latency": True, "missed_fills": True,
            "partial_fills": True, "leg_failure": True, "funding": True,
            "slippage_bps": 1.0, "slippage_mult": 5.0, "impact_bps_per_10k": 0.8,
            "latency_ms": 250.0, "latency_adverse_bps": 0.6,
            "fill_probability": 0.97, "partial_fill_prob": 0.15,
            "partial_fill_min_ratio": 0.5, "leg_failure_prob": 0.02,
            "funding_rate_8h_bps": 1.0, "seed": 1337,
        },
    }


def _parse_set_value(text: str):
    """Parse a ``--set`` RHS as JSON (so true/2.5/["SOL"] work); fall back to a
    bare string for unquoted words like an unquoted symbol name."""
    try:
        return json.loads(text)
    except ValueError:
        return text


def _apply_set(raw: dict, key: str, value) -> None:
    """Apply one ``KEY=VALUE`` edit to the raw dict (dotted ``realism.*`` keys)."""
    if key.startswith("realism."):
        sub = key.split(".", 1)[1]
        if sub not in _REALISM_KEYS:
            raise SystemExit(f"config: unknown field 'realism.{sub}'. "
                             f"Known: {sorted(_REALISM_KEYS)}")
        raw.setdefault("realism", {})[sub] = value
    elif key in _TOP_KEYS:
        raw[key] = value
    elif key == "realism":
        raise SystemExit("config: edit a specific field, e.g. "
                         "realism.slippage_bps, not 'realism' wholesale.")
    else:
        raise SystemExit(f"config: unknown key '{key}'. Known: "
                         f"{sorted(_TOP_KEYS)} or realism.<field>.")


def _toggle_effect(raw: dict, eff: str, on: bool) -> None:
    """Turn a realism effect (or the master switch / all) on or off."""
    name = eff.strip().lower()
    rl = raw.setdefault("realism", {})
    if name in ("all", "everything", "*"):
        rl["enabled"] = on
        for e in REALISM_EFFECTS:
            rl[e] = on
    elif name in ("realism", "master", "enabled"):
        rl["enabled"] = on
    elif name in REALISM_EFFECTS:
        rl[name] = on
    else:
        raise SystemExit(f"config: unknown effect '{eff}'. Choose from: "
                         f"{', '.join(REALISM_EFFECTS)}, all, realism.")


def _print_config(cfg: "BotConfig") -> None:
    """Human-readable dump of a resolved BotConfig (effects shown on/off)."""
    rp = cfg.realism
    on = lambda b: (GREEN + "on " + RESET) if b else (GRAY + "off" + RESET)
    print("─" * 64)
    print(f" source          : {cfg.source}")
    print(f" dry_run         : {cfg.dry_run}   "
          f"({'SIMULATION' if cfg.dry_run else 'LIVE'})")
    capped_note = (f"  → CLAMPED to {MAX_ORDER_USD:.2f} (hard ceiling)"
                   if cfg.order_size_usd > MAX_ORDER_USD else "")
    print(f" order_size_usd  : {cfg.order_size_usd:,.2f}{capped_note}")
    print(f" min_spread_bps  : {cfg.min_spread_bps:.2f}")
    print(f" target_markets  : {', '.join(cfg.target_markets)}")
    print(f" realism.enabled : {on(rp.enabled)} (master)")
    print(f"   {'slippage':<14}{on(rp.slippage)}  "
          f"{rp.slippage_bps:.2f}×{rp.slippage_mult:.2f}"
          f"={rp.slippage_bps*rp.slippage_mult:.2f}bps "
          f"+ {rp.impact_bps_per_10k:.2f}bps/$10k")
    print(f"   {'latency':<14}{on(rp.latency)}  "
          f"{rp.latency_ms:.0f}ms ±{rp.latency_adverse_bps:.2f}bps/100ms")
    print(f"   {'missed_fills':<14}{on(rp.missed_fills)}  "
          f"fill_probability={rp.fill_probability:.3f}")
    print(f"   {'partial_fills':<14}{on(rp.partial_fills)}  "
          f"prob={rp.partial_fill_prob:.3f} min_ratio={rp.partial_fill_min_ratio:.2f}")
    print(f"   {'leg_failure':<14}{on(rp.leg_failure)}  "
          f"prob={rp.leg_failure_prob:.3f}")
    print(f"   {'funding':<14}{on(rp.funding)}  "
          f"{rp.funding_rate_8h_bps:+.2f}bps/8h")
    print(f"   {'seed':<14}{rp.seed}")
    if not rp.enabled and any(getattr(rp, e) for e in REALISM_EFFECTS):
        print(YELLOW + "   note: realism master is OFF — per-effect switches "
              "won't apply until you enable it (config --enable realism)." + RESET)
    for w in cfg.warnings:
        print(YELLOW + f" ⚠ {w}" + RESET)
    print("─" * 64)


def _config_cli(argv) -> int:
    """`python live_core.py config ...` — view, edit, and toggle config.json."""
    p = argparse.ArgumentParser(
        prog="live_core config",
        description="View / edit config.json: simulation parameters and "
                    "per-effect realism toggles (no hand-editing JSON).",
        epilog="examples:\n"
               "  config --show\n"
               "  config --set realism.slippage_bps=2.5 --set order_size_usd=250\n"
               "  config --disable funding --disable partial_fills\n"
               "  config --enable realism   # master switch on\n"
               "  config --init             # write a default config.json",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=CONFIG_PATH, metavar="PATH",
                   help=f"config file to view/edit (default: {CONFIG_PATH}).")
    p.add_argument("--show", action="store_true",
                   help="print the resolved config (default action).")
    p.add_argument("--init", action="store_true",
                   help="write a default config.json if one doesn't exist.")
    p.add_argument("--set", dest="sets", action="append", default=[],
                   metavar="KEY=VALUE",
                   help="set a value; JSON-typed; dotted realism.<field>. "
                        "Repeatable.")
    p.add_argument("--enable", action="append", default=[], metavar="EFFECT",
                   help=f"turn an effect ON: {', '.join(REALISM_EFFECTS)}, "
                        "all, or realism (master). Repeatable.")
    p.add_argument("--disable", action="append", default=[], metavar="EFFECT",
                   help="turn an effect OFF (same names as --enable). Repeatable.")
    a = p.parse_args(argv)
    path = a.config

    editing = bool(a.sets or a.enable or a.disable or a.init)

    # Start from the existing file, or a default template when editing a missing
    # one. A pure --show of a missing file just renders the built-in defaults.
    raw = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (ValueError, OSError) as exc:
            raise SystemExit(f"config: could not read/parse {path}: {exc}")
        if not isinstance(raw, dict):
            raise SystemExit(f"config: {path} must contain a JSON object.")
    elif editing:
        raw = _default_config_dict()
        print(f"[config] {path} not found — starting from defaults.")

    for item in a.sets:
        if "=" not in item:
            raise SystemExit(f"config: --set expects KEY=VALUE, got '{item}'.")
        key, val = item.split("=", 1)
        _apply_set(raw, key.strip(), _parse_set_value(val.strip()))
    for eff in a.enable:
        _toggle_effect(raw, eff, True)
    for eff in a.disable:
        _toggle_effect(raw, eff, False)

    # Validate BEFORE writing — a bad edit raises and never corrupts the file.
    cfg = (_config_from_dict(raw, source=path) if raw
           else BotConfig(source=f"built-in defaults (no {path} found)"))

    if editing:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(raw, fh, indent=2)
            fh.write("\n")
        print(f"[config] wrote {path}")

    _print_config(cfg)
    return 0


# ---------------------------------------------------------------------------
# Live book state — a near-zero-latency reflection of both venues' top of book
# ---------------------------------------------------------------------------

@dataclass
class VenueBook:
    """Top-of-book for one venue plus its liveness bookkeeping."""
    name: str
    bid: float = 0.0
    ask: float = 0.0
    updates: int = 0
    last_frame: float = 0.0   # time.monotonic() of the last applied frame
    connected: bool = False
    stale: bool = False

    @property
    def healthy(self) -> bool:
        # A venue is tradeable ONLY when its socket is up (``connected``), it is
        # NOT flagged ``stale`` by the watchdog, AND it carries a live two-sided
        # book. Anything else means we'd be pricing against cached/phantom data.
        return self.connected and not self.stale and self.bid > 0 and self.ask > 0

    def apply(self, bid: float, ask: float, now: float) -> None:
        self.bid = bid
        self.ask = ask
        self.updates += 1
        self.last_frame = now
        # A freshly applied frame is proof the venue is live + synchronized at
        # this instant: mark it connected and clear any stale flag. (A drop or
        # silence later flips these back via mark_down / the watchdog.)
        self.connected = True
        self.stale = False


class PerpBookState:
    """Holds both venues' books and fans each update out to ``on_update``.

    Kept deliberately transport-agnostic: the stream tasks call ``update_*``
    and this object never knows or cares about order placement.
    """

    def __init__(self, spec: "SymbolSpec | None" = None) -> None:
        self.spec = spec                       # which asset this book tracks
        self.binance = VenueBook("Binance")
        self.bybit = VenueBook("Bybit")
        self.okx = VenueBook("OKX")
        # Keyed lookup so adding venues doesn't touch every method.
        self._books = {"binance": self.binance,
                       "bybit": self.bybit,
                       "okx": self.okx}
        # Set by the orchestrator to the gate's per-frame callback.
        self.on_update = None        # fn(feed: str) -> None
        self.on_ws_frame = None      # fn(feed: str) -> None  (liveness hook)

    def _book(self, feed: str) -> VenueBook:
        return self._books[feed]

    def books(self):
        """All venue books in a stable display order."""
        return (self.binance, self.bybit, self.okx)

    def ready_books(self):
        """Only venues with a populated two-sided book."""
        return [b for b in self.books() if b.bid > 0 and b.ask > 0]

    def healthy_books(self):
        """Only FULLY healthy venues (connected, not stale, two-sided book) —
        the sole venues safe to price an arbitrage against."""
        return [b for b in self.books() if b.healthy]

    def mark_connected(self, feed: str, connected: bool) -> None:
        self._book(feed).connected = connected

    def update(self, feed: str, bid: float, ask: float) -> None:
        book = self._book(feed)
        book.apply(bid, ask, time.monotonic())
        if self.on_ws_frame:
            self.on_ws_frame(feed)
        if self.on_update:
            self.on_update(feed)

    def both_ready(self) -> bool:
        # Retained for back-compat: true once at least two venues have books.
        return len(self.ready_books()) >= 2


# ---------------------------------------------------------------------------
# Fee-aware cross-venue edge math
# ---------------------------------------------------------------------------

@dataclass
class Edge:
    """One directional opportunity: BUY on ``buy_venue``, SELL on ``sell_venue``."""
    direction: str        # short label, e.g. "Binance→Bybit"
    buy_venue: str
    sell_venue: str
    buy_ask: float        # price paid (taker, lifts the ask)
    sell_bid: float       # price received (taker, hits the bid)
    per_unit: float       # net profit per BTC after round-trip taker fees

    @property
    def is_open(self) -> bool:
        # True iff buy_ask·(1+fee) < sell_bid·(1−fee)  ⇒  per_unit > 0.
        return self.per_unit > 0.0

    @property
    def bps(self) -> float:
        ref = self.buy_ask or 1.0
        return (self.per_unit / ref) * 1e4


def _edge(direction, buy_venue, sell_venue, buy_ask, sell_bid) -> Edge:
    # Net per-unit edge buying at ``buy_ask`` and selling at ``sell_bid`` with a
    # taker fee charged on BOTH legs.
    per_unit = sell_bid * (1.0 - TAKER_FEE) - buy_ask * (1.0 + TAKER_FEE)
    return Edge(direction, buy_venue, sell_venue, buy_ask, sell_bid, per_unit)


def evaluate_edges(state: "PerpBookState"):
    """Every directional edge across all HEALTHY venue pairs, best (highest) first.

    With two venues live this yields the original two Binance↔Bybit directions
    (unchanged); with OKX also streaming it additionally yields the Binance↔OKX
    and Bybit↔OKX directions, so the gate routes to the best edge anywhere.
    """
    # PHANTOM-SPREAD GUARD: price ONLY across fully-healthy venues. A stale or
    # disconnected venue keeps its last cached bid/ask, which would otherwise
    # manufacture an artificial edge against dead data — strictly exclude it
    # from every pair, so no spread is computed (and no entry/in-flight flagged)
    # touching an unhealthy venue.
    venues = state.healthy_books()
    edges = []
    for buy in venues:
        for sell in venues:
            if buy is sell:
                continue
            # BUY at buy.ask, SELL at sell.bid.
            edges.append(_edge(f"{buy.name}→{sell.name}",
                               buy.name, sell.name, buy.ask, sell.bid))
    return sorted(edges, key=lambda e: e.per_unit, reverse=True)


# Canonical unordered venue pairs for the basis readout (display order).
VENUE_PAIRS = (("Binance", "Bybit"), ("Binance", "OKX"), ("Bybit", "OKX"))


def best_edge_between(state: "PerpBookState", name_a: str, name_b: str):
    """Best (highest net) directional edge between two named venues, or None
    if either side isn't streaming a book yet."""
    cand = [e for e in evaluate_edges(state)
            if {e.buy_venue, e.sell_venue} == {name_a, name_b}]
    return max(cand, key=lambda e: e.per_unit) if cand else None


def evaluate_all_symbols(portfolio: "Portfolio"):
    """Run the per-symbol edge math across EVERY tracked asset at once.

    Returns ``{symbol_name: [edges...]}`` — each symbol evaluated independently
    on its own three-venue book, so concurrent assets never cross-contaminate.
    """
    return {name: evaluate_edges(eng.state)
            for name, eng in portfolio.engines.items()}


# ---------------------------------------------------------------------------
# In-memory paper ledger
# ---------------------------------------------------------------------------

@dataclass
class Position:
    direction: str
    buy_venue: str
    sell_venue: str
    entry_buy: float
    entry_sell: float
    contracts: float          # BTC notional = stake / entry_buy
    locked_per_unit: float    # net edge captured per BTC at entry
    opened_at: float          # time.monotonic()
    # Hedge state — both legs fill atomically in simulation, so a paper trade is
    # always hedged. A LIVE executor would flip ``hedged`` False and stamp
    # ``unhedged_since`` the instant one leg fills but its partner does not; the
    # reconciliation guard panics if that exposure persists past the grace window.
    hedged: bool = True
    unhedged_since: "float | None" = None
    # Perp funding accrued per second on the position notional (realism layer).
    # 0.0 → no funding (idealised fills), so existing math is unchanged.
    funding_rate_per_sec: float = 0.0

    @property
    def locked_pnl(self) -> float:
        return self.locked_per_unit * self.contracts

    @property
    def notional(self) -> float:
        """Position notional in USD (size × entry buy price)."""
        return self.contracts * self.entry_buy


@dataclass
class Ledger:
    realized_pnl: float = 0.0
    trade_count: int = 0
    open_trade: "Position | None" = None

    def open_position(self, edge: "Edge", stake: float, *,
                      fill: "Fill | None" = None) -> None:
        """Open a paper position. With ``fill`` None this books the idealised
        edge (full size at top-of-book). With a ``fill`` from the execution-
        realism layer it books the ACTUAL realised prices, size, hedge state and
        funding rate instead — so slippage / partials / leg-failures persist."""
        if fill is None:
            contracts = stake / edge.buy_ask if edge.buy_ask else 0.0
            self.open_trade = Position(
                direction=edge.direction,
                buy_venue=edge.buy_venue,
                sell_venue=edge.sell_venue,
                entry_buy=edge.buy_ask,
                entry_sell=edge.sell_bid,
                contracts=contracts,
                locked_per_unit=edge.per_unit,
                opened_at=time.monotonic(),
            )
        else:
            self.open_trade = Position(
                direction=edge.direction,
                buy_venue=edge.buy_venue,
                sell_venue=edge.sell_venue,
                entry_buy=fill.exec_buy,
                entry_sell=fill.exec_sell,
                contracts=fill.contracts,
                locked_per_unit=fill.per_unit,
                opened_at=time.monotonic(),
                hedged=fill.hedged,
                unhedged_since=fill.unhedged_since,
                funding_rate_per_sec=fill.funding_rate_per_sec,
            )

    def close_position(self) -> float:
        pos = self.open_trade
        if pos is None:
            return 0.0
        # Arb profit is locked at entry on both legs; the realism layer then
        # nets perp funding accrued over the hold (0 when funding is disabled).
        held = max(0.0, time.monotonic() - pos.opened_at)
        funding = pos.funding_rate_per_sec * pos.notional * held
        pnl = pos.locked_pnl - funding
        self.realized_pnl += pnl
        self.trade_count += 1
        self.open_trade = None
        return pnl


# ---------------------------------------------------------------------------
# Structured trade journal — persistent, append-only, off the hot path
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TradeRecord:
    """One executed (simulated) round-trip, exactly as persisted to ``trades.csv``."""
    timestamp_utc: str        # ISO-8601 UTC, second precision
    asset: str                # canonical symbol, e.g. "SOL"
    venue_buy: str            # leg A — venue we bought (took the ask) on
    venue_sell: str           # leg B — venue we sold (hit the bid) on
    exec_price_buy: float     # execution price on leg A
    exec_price_sell: float    # execution price on leg B
    expected_spread_bps: float  # net-of-fee edge locked at entry, in basis points
    realized_pnl: float       # realized P/L on the closed round-trip (USD)


class TradeLogger:
    """Append-only CSV journal for executed trades.

    Why a background thread: ``record()`` is called from inside the per-frame
    execution path (``_ArbGate.on_update``) which runs on the single asyncio
    event-loop thread. Touching the disk there — open/write/``flush`` — would
    stall every stream and the dashboard behind filesystem latency. Instead the
    hot path only does an O(1), lock-free ``queue.put_nowait``; a dedicated
    daemon writer thread drains the queue and does the actual ``writerow`` +
    ``flush`` (instant durability per row, so a hard kill never loses a trade).
    """

    HEADER = ["timestamp_utc", "asset", "venue_buy", "venue_sell",
              "exec_price_buy", "exec_price_sell",
              "expected_spread_bps", "realized_pnl"]
    _SENTINEL = object()      # poison pill that tells the writer thread to stop

    def __init__(self, path: str = TRADE_LOG_PATH) -> None:
        self.path = path
        self.count = 0                       # rows accepted (producer side)
        self._q: "queue.Queue" = queue.Queue()
        # Write a header only for a brand-new / empty file, so re-runs append
        # cleanly onto the same journal instead of clobbering history.
        write_header = (not os.path.exists(path)) or os.path.getsize(path) == 0
        # newline="" lets csv own the line terminator (no blank lines on Windows).
        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        if write_header:
            self._writer.writerow(self.HEADER)
            self._fh.flush()
        self._thread = threading.Thread(
            target=self._drain, name="trade-logger", daemon=True)
        self._thread.start()

    def record(self, rec: "TradeRecord") -> None:
        """Enqueue a trade for persistence. Hot-path safe: NO disk I/O here."""
        self.count += 1
        self._q.put_nowait(rec)

    def _drain(self) -> None:
        """Writer thread: serialize queued records to disk, flushing each row."""
        while True:
            item = self._q.get()
            if item is self._SENTINEL:
                break
            self._writer.writerow([
                item.timestamp_utc, item.asset, item.venue_buy, item.venue_sell,
                f"{item.exec_price_buy:.8f}", f"{item.exec_price_sell:.8f}",
                f"{item.expected_spread_bps:.4f}", f"{item.realized_pnl:.6f}",
            ])
            self._fh.flush()                 # instant durability (requirement #2)

    def close(self) -> None:
        """Drain any queued rows, stop the writer thread, and close the file."""
        self._q.put_nowait(self._SENTINEL)
        self._thread.join(timeout=5.0)
        try:
            self._fh.flush()
            self._fh.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Feature 4 — execution realism layer (slippage, latency, partials, funding)
#
# The idealised gate assumes both legs fill instantly at top-of-book for the
# full size with the edge exactly as quoted. Reality is messier. ExecutionModel
# maps an ideal ``Edge`` to a realistic ``Fill`` by layering, in order:
#
#   1. LATENCY        — during the signal→fill wire time the book drifts against
#                       us (adverse selection); magnitude scales with latency.
#   2. SLIPPAGE+IMPACT — a taker lifts/hits worse than top-of-book, and larger
#                       size pushes the fill further (market impact).
#   3. FILL PROBABILITY — the resting quote may be gone: no fill at all. If the
#                       edge has evaporated post-slippage we also skip (never
#                       knowingly cross into a loss).
#   4. PARTIAL FILL   — we may capture only part of the intended size.
#   5. LEG FAILURE    — occasionally one leg fills and its hedge does NOT, leaving
#                       unhedged exposure the ReconciliationGuard then catches.
#   6. FUNDING        — perp funding accrues over the hold (netted at close).
#
# Every draw comes from a single seeded RNG, so a whole session is byte-for-byte
# reproducible. Disabled by default → the gate keeps its idealised behaviour.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Fill:
    """The realised result of trying to execute an ``Edge`` (see ExecutionModel)."""
    exec_buy: float            # actual buy-leg fill price (≥ ideal ask)
    exec_sell: float           # actual sell-leg fill price (≤ ideal bid)
    contracts: float           # size actually filled (≤ intended, partials)
    per_unit: float            # realised net edge per unit after frictions
    hedged: bool               # False ⇒ a leg failed → unhedged exposure
    unhedged_since: "float | None"
    funding_rate_per_sec: float
    fill_ratio: float          # filled / intended size (diagnostics)
    slipped_bps: float         # total adverse bps applied per leg (diagnostics)


class ExecutionModel:
    """Turns an ideal ``Edge`` into a realistic ``Fill``. Deterministic per seed.

    One instance per symbol (each seeded distinctly) keeps assets independent
    yet fully reproducible. ``simulate_entry`` returns ``None`` to mean "no fill"
    (quote pulled, or the edge evaporated once slippage/latency were applied).
    """

    def __init__(self, params: "RealismParams", seed: int) -> None:
        self.p = params
        self.rng = random.Random(seed)

    def simulate_entry(self, edge: "Edge", stake: float) -> "Fill | None":
        p, r = self.p, self.rng

        # Per-effect toggles collapse a disabled effect to its no-op value while
        # the RNG is still drawn in a fixed order (so toggling one effect doesn't
        # disturb the others' reproducible stream).
        slip_bps = (p.slippage_bps * p.slippage_mult) if p.slippage else 0.0
        impact_per_10k = p.impact_bps_per_10k if p.slippage else 0.0
        lat_adverse = p.latency_adverse_bps if p.latency else 0.0
        fill_prob = p.fill_probability if p.missed_fills else 1.0
        partial_prob = p.partial_fill_prob if p.partial_fills else 0.0
        legfail_prob = p.leg_failure_prob if p.leg_failure else 0.0
        fund_8h = p.funding_rate_8h_bps if p.funding else 0.0

        # (1) LATENCY adverse selection + (2) SLIPPAGE + market IMPACT, as a
        # single adverse fraction applied to BOTH legs (buy higher, sell lower).
        # latency_adverse_bps is the worst-case drift per 100ms; a uniform draw
        # scales it, so the realised hit varies trade to trade.
        adverse_bps = lat_adverse * (p.latency_ms / 100.0) * r.random()
        impact_bps = impact_per_10k * (stake / 10_000.0)
        penalty = (adverse_bps + slip_bps + impact_bps) * 1e-4
        exec_buy = edge.buy_ask * (1.0 + penalty)
        exec_sell = edge.sell_bid * (1.0 - penalty)
        per_unit = (exec_sell * (1.0 - TAKER_FEE)
                    - exec_buy * (1.0 + TAKER_FEE))

        # (3) FILL PROBABILITY: the resting quote may simply be gone.
        if r.random() > fill_prob:
            return None
        # Edge evaporated once frictions are applied — don't knowingly lose.
        if per_unit <= 0.0 or exec_buy <= 0.0:
            return None

        # (4) PARTIAL FILL: sometimes we only get part of the size.
        fill_ratio = 1.0
        if r.random() < partial_prob:
            fill_ratio = r.uniform(p.partial_fill_min_ratio, 1.0)
        contracts = (stake / exec_buy) * fill_ratio

        # (5) LEG FAILURE: one leg fills, its hedge does not → unhedged exposure.
        hedged, unhedged_since = True, None
        if r.random() < legfail_prob:
            hedged, unhedged_since = False, time.monotonic()

        # (6) FUNDING accrual rate (per second) on the notional.
        funding_rate_per_sec = (fund_8h * 1e-4) / (8.0 * 3600.0)

        return Fill(
            exec_buy=exec_buy,
            exec_sell=exec_sell,
            contracts=contracts,
            per_unit=per_unit,
            hedged=hedged,
            unhedged_since=unhedged_since,
            funding_rate_per_sec=funding_rate_per_sec,
            fill_ratio=fill_ratio,
            slipped_bps=penalty * 1e4,
        )


# ---------------------------------------------------------------------------
# Production guardrails — hard order-size ceiling + trading circuit breaker
#
# These intercept a trade request BEFORE it can mutate the simulated ledger.
# Nothing here transmits a real order (the broker layer stays mocked); they are
# defence-in-depth so the paper engine already behaves like a risk-managed live
# one — the guardrails are validated in simulation before any live wiring.
# ---------------------------------------------------------------------------

def cap_order_size(requested_usd: float) -> "tuple[float, bool]":
    """Clamp a per-clip stake to the hard ``MAX_ORDER_USD`` ceiling.

    Returns ``(capped_usd, clamped)`` — ``clamped`` is True iff the request
    exceeded the ceiling and was scaled back. Pure and side-effect free, so it
    is trivially unit-testable and safe to call on the per-frame hot path.
    """
    if requested_usd > MAX_ORDER_USD:
        return MAX_ORDER_USD, True
    return requested_usd, False


class CircuitBreakerTripped(RuntimeError):
    """Raised the instant a circuit-breaker rule trips.

    Carries a structured ``diagnostic`` snapshot of the EXACT engine state at
    the moment of the trip, so the operator log can be highly detailed. It is
    raised from the per-frame execution path and CAUGHT by ``_ArbGate.on_update``
    — which logs it and latches a clean portfolio-wide halt. The process is
    never killed, so the streams + dashboard stay live for inspection.
    """

    def __init__(self, rule: str, detail: str, diagnostic: dict) -> None:
        super().__init__(f"[{rule}] {detail}")
        self.rule = rule
        self.detail = detail
        self.diagnostic = diagnostic


class CircuitBreaker:
    """Portfolio-wide trading circuit breaker (shared by every symbol gate).

    Four independent rules, any of which trips ONE global halt:

      * RATE LIMIT        — more than ``max_trades_per_min`` entries opened
                            within a rolling ``window_sec`` window (runaway loop).
      * CONSECUTIVE LOSS  — ``max_consecutive_losses`` losing round-trips in a row.
      * MISSED-FILL STREAK — ``max_missed_fill_streak`` consecutive missed fills
                            (since the last successful fill); the gate keeps
                            finding edges it can't actually capture.
      * EDGE DEGRADATION  — the rolling average REALISED edge over the last
                            ``edge_window`` trades falls below ``min_avg_edge_bps``.

    Disabled by default (``enabled=False``) so ad-hoc engines and unit tests are
    unaffected; ``run_live`` constructs it enabled. Every clock is monotonic, so
    it is immune to wall-clock jumps.
    """

    def __init__(self, portfolio, *, enabled: bool = False,
                 max_trades_per_min: int = CB_MAX_TRADES_PER_MIN,
                 max_consecutive_losses: int = CB_MAX_CONSECUTIVE_LOSSES,
                 window_sec: float = CB_WINDOW_SEC,
                 max_missed_fill_streak: int = CB_MAX_MISSED_FILL_STREAK,
                 miss_min_gap_sec: float = CB_MISSED_FILL_MIN_GAP_SEC,
                 edge_window: int = CB_EDGE_WINDOW,
                 min_avg_edge_bps: float = CB_MIN_AVG_EDGE_BPS) -> None:
        self.portfolio = portfolio
        self.enabled = bool(enabled)
        self.max_trades_per_min = int(max_trades_per_min)
        self.max_consecutive_losses = int(max_consecutive_losses)
        self.window_sec = float(window_sec)
        self.max_missed_fill_streak = int(max_missed_fill_streak)
        self.miss_min_gap_sec = float(miss_min_gap_sec)
        self.edge_window = int(edge_window)
        self.min_avg_edge_bps = float(min_avg_edge_bps)
        self.tripped = False
        self._trade_times = deque()     # monotonic stamps of recent entries
        self._loss_streak = 0
        self._miss_streak = 0           # consecutive missed fills since a fill
        self._last_miss_at = 0.0        # monotonic stamp of last counted miss
        self._edges = deque(maxlen=self.edge_window)   # recent realised edge bps

    # --- rate limit --------------------------------------------------------
    def _evict(self, now: float) -> None:
        w = self._trade_times
        while w and now - w[0] > self.window_sec:
            w.popleft()

    def check_rate(self, symbol: str) -> None:
        """Pre-entry gate: trip if the rolling window is already at the cap.

        Called BEFORE the ledger is touched, so a tripped rate limit refuses the
        entry outright (intercept before state mutation).
        """
        if not self.enabled or self.tripped:
            return
        now = time.monotonic()
        self._evict(now)
        if len(self._trade_times) >= self.max_trades_per_min:
            raise self._trip(
                "RATE_LIMIT",
                f"{len(self._trade_times)} entries within the last "
                f"{self.window_sec:.0f}s hit the cap of "
                f"{self.max_trades_per_min}/window — refusing new entries",
                symbol)

    def record_trade(self) -> None:
        """Count a position that actually opened toward the rate window.

        A successful fill also breaks any in-progress missed-fill streak.
        """
        if not self.enabled:
            return
        self._trade_times.append(time.monotonic())
        self._miss_streak = 0

    # --- consecutive losses ------------------------------------------------
    def record_result(self, pnl: float, symbol: str) -> None:
        """Post-close: update the loss streak and trip on a run of losers."""
        if not self.enabled or self.tripped:
            return
        if pnl < 0.0:
            self._loss_streak += 1
        else:
            self._loss_streak = 0
        if self._loss_streak >= self.max_consecutive_losses:
            raise self._trip(
                "CONSECUTIVE_LOSSES",
                f"{self._loss_streak} consecutive losing round-trips hit the "
                f"cap of {self.max_consecutive_losses}",
                symbol)

    # --- missed-fill streak ------------------------------------------------
    def record_missed_fill(self, symbol: str) -> None:
        """A tradeable edge was found but the fill was REJECTED (quote pulled or
        friction erased it). Trips on a sustained run with no successful fill.

        THROTTLED: missed fills closer together than ``miss_min_gap_sec`` count
        once, so a per-frame rejection storm accrues at a human timescale rather
        than tripping in a few milliseconds. Reset by any successful fill
        (``record_trade``).
        """
        if not self.enabled or self.tripped:
            return
        now = time.monotonic()
        if now - self._last_miss_at < self.miss_min_gap_sec:
            return                       # already counted a miss very recently
        self._last_miss_at = now
        self._miss_streak += 1
        if self._miss_streak >= self.max_missed_fill_streak:
            raise self._trip(
                "MISSED_FILL_STREAK",
                f"{self._miss_streak} consecutive missed fills (≥"
                f"{self.miss_min_gap_sec:.0f}s apart, no fill in between) hit "
                f"the cap of {self.max_missed_fill_streak} — the book keeps "
                f"slipping away",
                symbol)

    # --- edge degradation --------------------------------------------------
    def record_edge(self, edge_bps: float, symbol: str) -> None:
        """Feed the REALISED edge (bps) of a freshly-opened trade. Trips when the
        rolling average over the last ``edge_window`` trades degrades below
        ``min_avg_edge_bps`` — the edges we capture have compressed to marginal.
        """
        if not self.enabled or self.tripped:
            return
        self._edges.append(float(edge_bps))
        if len(self._edges) >= self.edge_window:
            avg = sum(self._edges) / len(self._edges)
            if avg < self.min_avg_edge_bps:
                raise self._trip(
                    "EDGE_DEGRADATION",
                    f"avg realised edge {avg:.2f} bps over the last "
                    f"{len(self._edges)} trades fell below the "
                    f"{self.min_avg_edge_bps:.2f} bps floor",
                    symbol)

    # --- trip + halt -------------------------------------------------------
    def _trip(self, rule: str, detail: str,
              symbol: str) -> "CircuitBreakerTripped":
        self.tripped = True
        return CircuitBreakerTripped(rule, detail,
                                     self._snapshot(rule, detail, symbol))

    def latch_halt(self) -> None:
        """Latch the portfolio-wide kill-switch (no new entries anywhere)."""
        self.portfolio.halt_all("circuit-breaker")

    def _snapshot(self, rule: str, detail: str, symbol: str) -> dict:
        """Capture the EXACT engine state at the trip (for the diagnostic log)."""
        per_symbol = {}
        for name, eng in getattr(self.portfolio, "engines", {}).items():
            per_symbol[name] = {
                "status": eng.status,
                "open": eng.ledger.open_trade is not None,
                "realized_pnl": round(eng.ledger.realized_pnl, 6),
                "trades": eng.ledger.trade_count,
            }
        return {
            "ts_utc_ms": _ts_ms(),
            "rule": rule,
            "detail": detail,
            "trigger_symbol": symbol,
            "loss_streak": self._loss_streak,
            "max_consecutive_losses": self.max_consecutive_losses,
            "trades_in_window": len(self._trade_times),
            "max_trades_per_min": self.max_trades_per_min,
            "window_sec": self.window_sec,
            "miss_streak": self._miss_streak,
            "max_missed_fill_streak": self.max_missed_fill_streak,
            "recent_edges_bps": [round(e, 2) for e in self._edges],
            "avg_edge_bps": (round(sum(self._edges) / len(self._edges), 2)
                             if self._edges else None),
            "min_avg_edge_bps": self.min_avg_edge_bps,
            "session_realized_pnl": round(
                getattr(self.portfolio, "realized_pnl", 0.0), 6),
            "session_trade_count": getattr(self.portfolio, "trade_count", 0),
            "per_symbol": per_symbol,
        }


# ---------------------------------------------------------------------------
# The fee-gated arbitrage engine (execution state lock + monotonic cooldown)
# ---------------------------------------------------------------------------

class _ArbGate:
    """Per-frame callback enforcing one clean entry + one clean exit per spread.

    EXECUTION STATE LOCK (``is_in_flight``)
    ---------------------------------------
    WebSocket frames arrive many times per second; without a lock a persistent
    spread would re-fire the alert on every frame. The lock guarantees:
      * The instant a position opens, ``is_in_flight`` is set True.
      * While True, ALL new entries are suppressed; only the exit is managed.
      * It resets to False the moment the spread converges and the position is
        closed out — starting the monotonic cooldown clock.

    MONOTONIC COOLDOWN
    ------------------
    After an exit, ``last_trade_time`` (a ``time.monotonic()`` stamp) gates new
    entries for ``TRADE_COOLDOWN_SEC`` so the same converging book can't be
    farmed millisecond after millisecond.
    """

    def __init__(self, state: "PerpBookState", ledger: "Ledger", stake: float,
                 logger: "TradeLogger | None" = None,
                 min_spread_bps: float = 0.0,
                 execution: "ExecutionModel | None" = None,
                 breaker: "CircuitBreaker | None" = None) -> None:
        self.state = state
        self.ledger = ledger
        self.stake = stake
        self.logger = logger             # optional persistent trade journal
        # Configurable net-of-fee entry hurdle (bps), ON TOP of the taker buffer.
        self.min_spread_bps = float(min_spread_bps)
        # Optional execution-realism model. None → idealised top-of-book fills.
        self.execution = execution
        # Optional portfolio-wide circuit breaker. None → no breaker (tests).
        self.breaker = breaker
        self.missed_fills = 0            # entries lost to slippage/latency/no-fill
        self.is_in_flight = False        # execution state lock
        self.last_trade_time = 0.0       # monotonic time of last exit (cooldown)
        self.halted = False              # hard kill-switch set by a panic close
        self._last_block_log = 0.0
        self._block_interval = 2.0       # seconds between repeat "blocked" lines
        self._last_size_warn = 0.0       # throttle for the size-cap warning

    def release(self, reason: str = "shutdown") -> None:
        """Clear the execution lock (e.g. on teardown)."""
        self.is_in_flight = False

    def halt(self, reason: str = "panic") -> None:
        """Latch the kill-switch: refuse ALL new entries until restarted."""
        self.halted = True
        self.is_in_flight = False

    def on_update(self, feed: str) -> None:
        """Per-frame entry point. A guardrail trip raises CircuitBreakerTripped
        from deep in the evaluation; we catch it HERE so the breaker cleanly
        pauses trading (a portfolio-wide halt) WITHOUT crashing the stream task
        that drives this callback."""
        try:
            self._evaluate_frame(feed)
        except CircuitBreakerTripped as exc:
            self._handle_breaker_trip(exc)

    def _evaluate_frame(self, feed: str) -> None:
        st, led = self.state, self.ledger
        sym = st.spec.name if st.spec else "?"

        # --- LOCKED: a position is in flight. Manage ONLY the exit; suppress
        # every new entry on subsequent high-frequency frames. ---
        if self.is_in_flight:
            pos = led.open_trade
            if pos is not None:
                # Recompute THIS position's directional edge on the live book.
                live = self._directional_edge(pos.direction)
                # Exit ONLY on a trustworthy converged edge. If a leg's venue
                # went stale/down, ``live`` is None (excluded by the health
                # guard) — HOLD rather than closing against phantom/cached data.
                if live is not None and not live.is_open:
                    pnl = led.close_position()
                    self.is_in_flight = False
                    self.last_trade_time = time.monotonic()
                    self._journal(pos, pnl)   # persist the closed round-trip
                    print(GREEN +
                          f"[{_ts()}] ✅ SPREAD CONVERGED — closed {pos.direction} "
                          f"| locked PnL ${pnl:+.4f} | session ${led.realized_pnl:+.4f}"
                          + RESET)
                    # Feed the realised PnL to the breaker AFTER the close is
                    # booked; a run of losers trips it (raises) right here.
                    if self.breaker is not None:
                        self.breaker.record_result(pnl, sym)
            else:
                # Defensive: position vanished without a close. Release the lock.
                self.is_in_flight = False
            return

        # --- HALTED: a panic close latched the kill-switch. No new entries. ---
        if self.halted:
            return

        # --- COOLDOWN: after an exit, ignore new entries for the cooldown
        # window (monotonic clock — immune to wall-clock jumps). ---
        if time.monotonic() - self.last_trade_time < TRADE_COOLDOWN_SEC:
            return

        # --- UNLOCKED: evaluate this frame for a NEW entry. ---
        edges = evaluate_edges(st)
        if not edges:
            return
        best = edges[0]
        # Entry requires a POSITIVE net-of-fee edge that ALSO clears the
        # configured spread hurdle (min_spread_bps). With the hurdle at 0 this
        # is exactly the original "any positive net edge trades" behaviour.
        if best.is_open and best.bps >= self.min_spread_bps:
            # GUARDRAIL INTERCEPTION (before ANY ledger mutation):
            #   1) rate-limit: refuse/halt if we have already traded too fast;
            #   2) hard size cap: clamp this clip's stake to the ceiling.
            if self.breaker is not None:
                self.breaker.check_rate(sym)         # raises → caught in on_update
            stake = self._guarded_stake(sym)
            # Route the intended entry through the execution-realism model (if
            # any). It can degrade or REJECT the fill (slippage/latency/no-fill).
            fill = None
            if self.execution is not None:
                fill = self.execution.simulate_entry(best, stake)
                if fill is None:
                    self.missed_fills += 1
                    now = time.monotonic()
                    if now - self._last_block_log >= self._block_interval:
                        self._last_block_log = now
                        print(GRAY +
                              f"[{_ts()}] 🛑 MISSED FILL [{sym}] {best.direction} "
                              f"{best.bps:+.2f} bps — quote pulled or slippage/"
                              f"latency erased the edge." + RESET)
                    # Feed the rejected attempt to the breaker (throttled); a
                    # sustained streak with no fill trips it (raises) here.
                    if self.breaker is not None:
                        self.breaker.record_missed_fill(sym)
                    return
            led.open_position(best, stake, fill=fill)
            if led.open_trade is not None:
                self.is_in_flight = True   # engage the lock immediately
                # Count the opened entry toward the breaker's rate window.
                if self.breaker is not None:
                    self.breaker.record_trade()
            # Build a realism annotation for the alert (slip / partial / unhedged).
            if fill is not None:
                shown_bps = (fill.per_unit / fill.exec_buy * 1e4
                             if fill.exec_buy else 0.0)
                extra = f" | slip {fill.slipped_bps:.2f}bps → net {shown_bps:+.2f} bps"
                if fill.fill_ratio < 1.0:
                    extra += f" | PARTIAL {fill.fill_ratio*100:.0f}%"
                if not fill.hedged:
                    extra += f" | {RED}⚠ UNHEDGED LEG{PURPLE}"
                buy_px, sell_px = fill.exec_buy, fill.exec_sell
            else:
                extra = (f" | net {best.bps:+.2f} bps "
                         f"(${best.per_unit:+.6f}/unit after "
                         f"{TAKER_FEE_PER_FILL*1e2:.2f}%/fill entry+exit taker)")
                buy_px, sell_px = best.buy_ask, best.sell_bid
            print(PURPLE +
                  f"[{_ts()}] 🚨 ARB SPREAD FOUND! [{sym}] {best.direction} "
                  f"| BUY {best.buy_venue} @ {_fmt_px(buy_px)} "
                  f"SELL {best.sell_venue} @ {_fmt_px(sell_px)}"
                  f"{extra}"
                  + RESET)
            # Feed the realised entry edge to the degradation monitor (raises
            # → caught in on_update if the rolling average has degraded).
            if self.breaker is not None and led.open_trade is not None:
                pos = led.open_trade
                edge_bps = (pos.locked_per_unit / pos.entry_buy * 1e4
                            if pos.entry_buy else 0.0)
                self.breaker.record_edge(edge_bps, sym)
        else:
            now = time.monotonic()
            if now - self._last_block_log >= self._block_interval:
                self._last_block_log = now
                # Distinguish "swallowed by fees" from "positive but under the
                # operator's hurdle" so the log explains WHY it didn't fire.
                reason = (f"below the {self.min_spread_bps:.2f} bps hurdle"
                          if best.is_open
                          else "inside the round-trip fee buffer")
                print(GRAY +
                      f"[{_ts()}] ℹ️  No edge: best {best.direction} "
                      f"{best.bps:+.2f} bps — {reason}."
                      + RESET)

    def _journal(self, pos: "Position", pnl: float) -> None:
        """Append the just-closed round-trip to the persistent trade journal.

        Off the critical path: this only enqueues (the logger's writer thread
        does the disk I/O). Silently no-ops when no logger is wired (tests/offline).
        """
        if self.logger is None:
            return
        sym = self.state.spec.name if self.state.spec else "?"
        # Expected spread in bps == net edge locked at entry / entry buy price,
        # matching Edge.bps (per_unit referenced to the buy-leg ask).
        expected_bps = (pos.locked_per_unit / pos.entry_buy * 1e4
                        if pos.entry_buy else 0.0)
        self.logger.record(TradeRecord(
            timestamp_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            asset=sym,
            venue_buy=pos.buy_venue,
            venue_sell=pos.sell_venue,
            exec_price_buy=pos.entry_buy,
            exec_price_sell=pos.entry_sell,
            expected_spread_bps=expected_bps,
            realized_pnl=pnl,
        ))

    def _directional_edge(self, direction: str):
        for e in evaluate_edges(self.state):
            if e.direction == direction:
                return e
        return None

    def _guarded_stake(self, sym: str) -> float:
        """Apply the hard order-size ceiling to this clip's stake.

        Intercepts BEFORE the ledger is touched. Warns (throttled, with a louder
        RED line past ``ORDER_WARN_USD``) whenever the configured size has to be
        scaled back to the ``MAX_ORDER_USD`` ceiling.
        """
        capped, clamped = cap_order_size(self.stake)
        if clamped:
            now = time.monotonic()
            if now - self._last_size_warn >= self._block_interval:
                self._last_size_warn = now
                sev = RED if self.stake > ORDER_WARN_USD else YELLOW
                print(sev +
                      f"[{_ts()}] ⚠️  ORDER-SIZE GUARD [{sym}] requested "
                      f"${self.stake:,.2f} exceeds the ${MAX_ORDER_USD:.2f} hard "
                      f"ceiling — clamped to ${capped:.2f} per clip." + RESET)
        return capped

    def _handle_breaker_trip(self, exc: "CircuitBreakerTripped") -> None:
        """A circuit-breaker rule tripped: emit a highly-detailed diagnostic,
        latch a portfolio-wide halt, and PAUSE — never crash the process."""
        d = exc.diagnostic
        print(RED + "⛔ " + "═" * 60 + RESET)
        print(RED + f"⛔ CIRCUIT BREAKER TRIPPED [{d['rule']}] at "
              f"{d['ts_utc_ms']}" + RESET)
        print(RED + f"⛔   {exc.detail}" + RESET)
        print(RED + f"⛔   trigger={d['trigger_symbol']} · loss_streak="
              f"{d['loss_streak']}/{d['max_consecutive_losses']} · window="
              f"{d['trades_in_window']}/{d['max_trades_per_min']} per "
              f"{d['window_sec']:.0f}s" + RESET)
        avg = d['avg_edge_bps']
        print(RED + f"⛔   miss_streak={d['miss_streak']}/"
              f"{d['max_missed_fill_streak']} · avg_edge="
              f"{('%.2f' % avg) if avg is not None else 'n/a'}/"
              f"{d['min_avg_edge_bps']:.2f} bps · recent_edges="
              f"{d['recent_edges_bps']}" + RESET)
        print(RED + f"⛔   session: {d['session_trade_count']} trade(s), "
              f"realized ${d['session_realized_pnl']:+.4f}" + RESET)
        for name, snap in d["per_symbol"].items():
            print(RED + f"⛔     {name:<6} {snap['status']:<14} "
                  f"open={str(snap['open']):<5} trades={snap['trades']:<3} "
                  f"pnl=${snap['realized_pnl']:+.4f}" + RESET)
        print(RED + "⛔   Trading PAUSED portfolio-wide (no new entries). "
              "Streams + dashboard stay live; restart after review." + RESET)
        print(RED + "⛔ " + "═" * 60 + RESET)
        if self.breaker is not None:
            self.breaker.latch_halt()


# ---------------------------------------------------------------------------
# Multi-symbol portfolio: one independent engine (book + ledger + gate) per
# asset, so locks, cooldowns and P&L never cross between coins.
# ---------------------------------------------------------------------------

@dataclass
class SymbolEngine:
    """Everything needed to track ONE asset, fully isolated from the others."""
    spec: "SymbolSpec"
    state: "PerpBookState"
    ledger: "Ledger"
    gate: "_ArbGate"

    @property
    def cooling_down(self) -> bool:
        return (not self.gate.is_in_flight
                and time.monotonic() - self.gate.last_trade_time
                < TRADE_COOLDOWN_SEC)

    @property
    def status(self) -> str:
        if self.gate.halted:
            return "halted"
        if self.gate.is_in_flight:
            return "🔒 in-flight"
        if self.cooling_down:
            return "cooldown"
        return "idle"


class Portfolio:
    """Holds one ``SymbolEngine`` per ``SymbolSpec`` plus the venue→symbol
    routing maps the stream tasks use to fan frames to the right book.

    Each engine carries its OWN ``is_in_flight`` lock and cooldown clock (they
    live on the per-symbol ``_ArbGate``), so a spike that locks PEPE leaves SOL
    and WIF free to trade their own independent edges concurrently.
    """

    def __init__(self, specs, stake: float,
                 logger: "TradeLogger | None" = None,
                 min_spread_bps: float = 0.0,
                 realism: "RealismParams | None" = None,
                 circuit_breaker_enabled: bool = False,
                 max_trades_per_min: int = CB_MAX_TRADES_PER_MIN,
                 max_consecutive_losses: int = CB_MAX_CONSECUTIVE_LOSSES) -> None:
        self.specs = list(specs)
        self.stake = stake
        self.logger = logger
        self.min_spread_bps = float(min_spread_bps)   # shared entry hurdle
        self.realism = realism
        # ONE portfolio-wide circuit breaker, shared by every symbol gate, so a
        # losing streak or runaway rate ANYWHERE trips a single global halt.
        # Disabled by default (tests/ad-hoc); ``run_live`` enables it.
        self.breaker = CircuitBreaker(
            self, enabled=circuit_breaker_enabled,
            max_trades_per_min=max_trades_per_min,
            max_consecutive_losses=max_consecutive_losses)
        self.engines = {}
        for i, spec in enumerate(self.specs):
            state = PerpBookState(spec)
            ledger = Ledger()
            # Each symbol gets its OWN seeded execution model (distinct seed per
            # symbol) so assets stay independent yet the run is reproducible.
            execution = None
            if realism is not None and realism.enabled:
                execution = ExecutionModel(realism, seed=realism.seed + i)
            gate = _ArbGate(state, ledger, stake, logger, min_spread_bps,
                            execution, breaker=self.breaker)
            state.on_update = gate.on_update     # per-symbol frame → per-symbol gate
            self.engines[spec.name] = SymbolEngine(spec, state, ledger, gate)
        # Reverse lookups from each venue's wire identifier to our canonical name.
        self.binance_map = {s.binance: s.name for s in self.specs}
        self.bybit_map = {s.bybit: s.name for s in self.specs}
        self.okx_map = {s.okx: s.name for s in self.specs}

    # --- aggregates -------------------------------------------------------
    @property
    def realized_pnl(self) -> float:
        return sum(e.ledger.realized_pnl for e in self.engines.values())

    @property
    def trade_count(self) -> int:
        return sum(e.ledger.trade_count for e in self.engines.values())

    @property
    def missed_fills(self) -> int:
        """Entries lost to slippage / latency / no-fill across all symbols."""
        return sum(e.gate.missed_fills for e in self.engines.values())

    def open_engines(self):
        return [e for e in self.engines.values() if e.ledger.open_trade is not None]

    # --- fan-out helpers --------------------------------------------------
    def mark_venue_connected(self, venue: str, connected: bool) -> None:
        for e in self.engines.values():
            e.state.mark_connected(venue, connected)

    def wire_ws_frame(self, callback) -> None:
        for e in self.engines.values():
            e.state.on_ws_frame = callback

    def halt_all(self, reason: str = "panic") -> None:
        for e in self.engines.values():
            e.gate.halt(reason)


# ---------------------------------------------------------------------------
# WebSocket streams (isolated transport — no execution logic lives here)
# ---------------------------------------------------------------------------

def parse_binance_book(raw):
    """Binance @bookTicker frame -> (bid, ask), or None if not a book frame.

    Pure: pulls best bid from ``msg['b']`` and best ask from ``msg['a']``.
    """
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    bid, ask = msg.get("b"), msg.get("a")
    if bid is None or ask is None:
        return None
    return float(bid), float(ask)


def parse_bybit_book(raw, prev_bid: float = 0.0, prev_ask: float = 0.0):
    """Bybit orderbook.1 frame -> (bid, ask), or None for non-book frames.

    Pure: pulls best bid from ``data['b'][0][0]`` and best ask from
    ``data['a'][0][0]``. A delta that omits one side keeps the prior level
    (``prev_bid`` / ``prev_ask``). Subscribe acks / pongs return None.
    """
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    data = msg.get("data")
    if not data or "orderbook" not in str(msg.get("topic", "")):
        return None
    bids, asks = data.get("b") or [], data.get("a") or []
    bid = float(bids[0][0]) if bids else prev_bid
    ask = float(asks[0][0]) if asks else prev_ask
    return bid, ask


def parse_okx_book(raw, prev_bid: float = 0.0, prev_ask: float = 0.0):
    """OKX bbo-tbt frame -> (bid, ask), or None for non-book frames.

    Pure: pulls best bid from ``data[0]['bids'][0][0]`` and best ask from
    ``data[0]['asks'][0][0]``. Subscribe/error events and the literal "pong"
    keepalive return None; a side-only update keeps the prior level.
    """
    try:
        msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    except (ValueError, TypeError):
        return None   # e.g. the literal "pong" keepalive
    if not isinstance(msg, dict) or "event" in msg:
        return None   # subscribe ack / error envelope
    data = msg.get("data")
    if not data:
        return None
    top = data[0]
    bids, asks = top.get("bids") or [], top.get("asks") or []
    bid = float(bids[0][0]) if bids else prev_bid
    ask = float(asks[0][0]) if asks else prev_ask
    return bid, ask


def _backoff_delay(attempt: int) -> float:
    """Deterministic exponential backoff, capped:  2s, 4s, 8s, 16s, ... ≤ cap."""
    return min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** attempt))


def _ws_close_code(exc: Exception):
    """Best-effort extraction of a WebSocket close code across websockets versions."""
    code = getattr(exc, "code", None)
    if code is None:
        rcvd = getattr(exc, "rcvd", None)
        code = getattr(rcvd, "code", None)
    return code


def _classify_ws_drop(exc: Exception) -> str:
    """Human-readable reason for a stream drop (special-cases 1013 + timeouts)."""
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "network timeout"
    code = _ws_close_code(exc)
    if code == 1013:
        return "rate limited by venue (close 1013 'try again later')"
    if code is not None:
        return f"{type(exc).__name__} (close {code})"
    return type(exc).__name__


async def _run_stream_with_reconnect(name, stop, connect_and_read, mark_down):
    """Shared resilient driver: (re)connect forever with exponential backoff.

    ``connect_and_read`` does ONE full session — open the socket and pump frames
    until it returns or raises. Any drop (network timeout, server close, or a
    1013 rate-limit) is logged, backed off exponentially (2s → 4s → 8s → …
    capped), and retried. ``mark_down`` flips the venue's connectivity flag.
    Cancellation propagates cleanly; nothing here can crash the event loop.
    """
    attempt = 0
    while not stop.is_set():
        try:
            await connect_and_read()
            attempt = 0  # clean session end (e.g. stop set) — reset backoff
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mark_down()
            if stop.is_set():
                break
            delay = _backoff_delay(attempt)
            attempt += 1
            print(YELLOW + f"[{_ts()}] ⚠️  {name} stream dropped: "
                  f"{_classify_ws_drop(exc)}. Reconnect attempt {attempt} "
                  f"in {delay:.0f}s." + RESET)
            await _sleep_or_stop(delay, stop)
    mark_down()


def _ws_connect(url: str):
    return websockets.connect(
        url,
        ping_interval=15,
        ping_timeout=10,
        close_timeout=5,
        max_queue=None,
    )


async def stream_binance(portfolio: "Portfolio", stop: asyncio.Event) -> None:
    """Binance USDⓈ-M COMBINED @bookTicker for every symbol on one socket.

    Combined frames arrive wrapped: {"stream": "<sym>@bookTicker", "data": …}.
    We map ``<sym>`` back to the canonical symbol and route to its book.
    """
    url = binance_combined_url(portfolio.specs)

    async def session() -> None:
        async with _ws_connect(url) as ws:
            portfolio.mark_venue_connected("binance", True)
            print(f"[{_ts()}] 🔌 Binance USDⓈ-M connected "
                  f"({len(portfolio.specs)} combined @bookTicker streams).")
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT_SEC)
                msg = json.loads(raw)
                stream, data = msg.get("stream"), msg.get("data")
                if not stream or not data:
                    continue
                sym = portfolio.binance_map.get(stream.split("@")[0])
                if sym is None:
                    continue
                book = parse_binance_book(data)   # data carries b / a
                if book is not None:
                    portfolio.engines[sym].state.update("binance", book[0], book[1])

    await _run_stream_with_reconnect(
        "Binance", stop, session,
        lambda: portfolio.mark_venue_connected("binance", False))


async def stream_bybit(portfolio: "Portfolio", stop: asyncio.Event) -> None:
    """Bybit V5 linear: one socket, ONE subscribe frame per symbol (isolated)."""
    frames = bybit_subscribe_frames(portfolio.specs)

    async def session() -> None:
        async with _ws_connect(BYBIT_WS_URL) as ws:
            for f in frames:
                await ws.send(json.dumps(f))
            portfolio.mark_venue_connected("bybit", True)
            print(f"[{_ts()}] 🔌 Bybit V5 linear connected "
                  f"(subscribed {len(frames)} isolated orderbook.1 topics).")
            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(),
                                                 timeout=RECV_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    await ws.send(json.dumps({"op": "ping"}))   # keepalive
                    continue
                msg = json.loads(raw)
                # Surface a rejected subscription loudly instead of going silent.
                if msg.get("op") == "subscribe" and msg.get("success") is False:
                    print(YELLOW + f"[{_ts()}] ⚠️  Bybit rejected a subscription: "
                          f"{msg.get('ret_msg')}" + RESET)
                    continue
                topic = str(msg.get("topic", ""))
                if "orderbook" not in topic:
                    continue
                sym = portfolio.bybit_map.get(topic.split(".")[-1])
                if sym is None:
                    continue
                st = portfolio.engines[sym].state
                # A side-only delta keeps THIS symbol's prior level.
                book = parse_bybit_book(msg, st.bybit.bid, st.bybit.ask)
                if book is not None and book[0] > 0 and book[1] > 0:
                    st.update("bybit", book[0], book[1])

    await _run_stream_with_reconnect(
        "Bybit", stop, session,
        lambda: portfolio.mark_venue_connected("bybit", False))


async def stream_okx(portfolio: "Portfolio", stop: asyncio.Event) -> None:
    """OKX V5 bbo-tbt: one socket subscribed to every symbol's swap."""
    sub = okx_subscribe(portfolio.specs)

    async def session() -> None:
        async with _ws_connect(OKX_WS_URL) as ws:
            await ws.send(json.dumps(sub))
            portfolio.mark_venue_connected("okx", True)
            print(f"[{_ts()}] 🔌 OKX V5 connected "
                  f"(subscribed {len(portfolio.specs)} bbo-tbt channels).")
            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(),
                                                 timeout=RECV_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    await ws.send("ping")   # OKX literal-text keepalive
                    continue
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue                # e.g. the literal "pong"
                if not isinstance(msg, dict) or "event" in msg:
                    continue                # subscribe ack / error
                inst = (msg.get("arg") or {}).get("instId")
                sym = portfolio.okx_map.get(inst) if inst else None
                if sym is None:
                    continue
                st = portfolio.engines[sym].state
                book = parse_okx_book(msg, st.okx.bid, st.okx.ask)
                if book is not None and book[0] > 0 and book[1] > 0:
                    st.update("okx", book[0], book[1])

    await _run_stream_with_reconnect(
        "OKX", stop, session,
        lambda: portfolio.mark_venue_connected("okx", False))


async def _sleep_or_stop(seconds: float, stop: asyncio.Event) -> None:
    """Sleep up to ``seconds`` but wake immediately if ``stop`` is set."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


# ---------------------------------------------------------------------------
# Liveness watchdog (flags a venue stale after prolonged WS silence)
# ---------------------------------------------------------------------------

class StalenessWatchdog:
    """Flags a VENUE stale after ``threshold`` seconds of silence and clears it
    the instant any symbol's frame arrives on that venue. Each venue is a single
    shared socket feeding every symbol, so liveness is tracked per-venue (across
    all books), not per-symbol. The stream tasks own reconnection; this only
    annotates liveness for the dashboard."""

    VENUES = ("binance", "bybit", "okx")

    def __init__(self, portfolio: "Portfolio", threshold: float) -> None:
        self.portfolio = portfolio
        self.threshold = float(threshold)
        self._stale = {v: False for v in self.VENUES}

    def _venue_books(self, venue: str):
        return [e.state._book(venue) for e in self.portfolio.engines.values()]

    def _set_stale(self, venue: str, stale: bool) -> None:
        self._stale[venue] = stale
        for b in self._venue_books(venue):
            b.stale = stale

    def is_stale(self, venue: str) -> bool:
        return self._stale.get(venue, False)

    def on_ws_frame(self, feed: str) -> None:
        if self._stale.get(feed):
            self._set_stale(feed, False)
            print(f"[{_ts()}] ⚡ {feed.capitalize()} WebSocket restored "
                  f"(all symbols).")

    async def watchdog_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _sleep_or_stop(WATCHDOG_TICK_SEC, stop)
            if stop.is_set():
                break
            now = time.monotonic()
            for venue in self.VENUES:
                books = self._venue_books(venue)
                last = max((b.last_frame for b in books), default=0.0)
                if last <= 0:
                    continue   # venue never delivered a frame yet
                silent_for = now - last
                if not self._stale[venue] and silent_for > self.threshold:
                    self._set_stale(venue, True)
                    print(YELLOW + f"[{_ts()}] ⚠️  {venue.capitalize()} WebSocket "
                          f"silent for {silent_for:0.0f}s (all symbols)." + RESET)


# ---------------------------------------------------------------------------
# Feature 3 — balance reconciliation, unhedged-leg watchdog, panic close
#
# SAFETY MODEL
# ------------
# The default path is SIMULATION: brokers are ``SimulatedBroker`` instances
# backed by the virtual ledger, and a "panic close" closes only the PAPER
# position. The authenticated path (``--live``) swaps in ``LiveBroker``, whose
# private-API methods are deliberately UNIMPLEMENTED stubs that raise rather
# than pretend to move real money — so enabling --live screams for a real
# signed-request implementation instead of silently doing the wrong thing.
# ---------------------------------------------------------------------------

# Virtual starting equity used as the reconciliation baseline (USD).
INITIAL_EQUITY_USD = PAPER_STAKE_USD
# Equity drift beyond this (live vs. internal ledger) triggers a panic.
DISCREPANCY_TOL_USD = 5.0
# An unhedged single leg exposed longer than this (seconds) triggers a panic.
UNHEDGED_GRACE_SEC = 5.0
# Full equity reconciliation cadence (seconds).
RECONCILE_INTERVAL_SEC = 60.0
# Fast tick for the unhedged-leg check (must be << UNHEDGED_GRACE_SEC).
RECON_TICK_SEC = 1.0


def send_alert(message: str) -> None:
    """Emit a loud operator alert. Hook point for webhook / email / SMS.

    Kept dependency-free: it writes a prominent line to the console. A
    production deployment would also POST to a pager/Slack webhook here (e.g.
    using an ``ALERT_WEBHOOK_URL`` env var) — left out so this module needs no
    network egress and no extra credentials.
    """
    print(RED + "🚨🚨🚨 ALERT " + "─" * 40 + RESET)
    print(RED + f"   [{_ts()}] {message}" + RESET)
    print(RED + "─" * 52 + RESET)


class SimulatedBroker:
    """A venue stand-in backed by the virtual ledger. No network, no orders."""

    def __init__(self, name: str, equity_fn) -> None:
        self.name = name
        self._equity_fn = equity_fn

    async def fetch_equity(self) -> float:
        return float(self._equity_fn())

    async def cancel_all_orders(self) -> int:
        print(GRAY + f"[{_ts()}] [sim:{self.name}] cancel_all_orders → "
              f"0 working orders (simulation)." + RESET)
        return 0

    async def close_all_positions(self) -> int:
        print(GRAY + f"[{_ts()}] [sim:{self.name}] close_all_positions → "
              f"flat (paper position handled by the ledger)." + RESET)
        return 0


class LiveBroker:
    """Authenticated venue adapter — DELIBERATELY UNIMPLEMENTED.

    Each method requires a signed private-API request. Rather than fabricate
    order-management code that could move real money incorrectly, these raise
    ``NotImplementedError`` so that ``--live`` cannot silently transact. Fill
    these in with real signed REST calls (and test against a venue testnet)
    before trusting --live with capital.
    """

    def __init__(self, name: str, api_key: str, api_secret: str,
                 api_passphrase: "str | None" = None) -> None:
        self.name = name
        # Stored for a real implementation; never logged. ``api_passphrase`` is
        # required by some venues (e.g. OKX) and left None for the others.
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase

    async def fetch_equity(self) -> float:
        raise NotImplementedError(
            f"{self.name}: live equity fetch needs a signed private-API "
            f"request — not implemented.")

    async def cancel_all_orders(self) -> int:
        raise NotImplementedError(
            f"{self.name}: live order cancellation needs a signed private-API "
            f"request — not implemented.")

    async def close_all_positions(self) -> int:
        raise NotImplementedError(
            f"{self.name}: live position close needs a signed private-API "
            f"request — not implemented.")


class ReconciliationGuard:
    """Background safety loop: equity reconciliation + unhedged-leg watchdog.

    * Every ``RECON_TICK_SEC`` it checks for an unhedged leg exposed past
      ``unhedged_grace`` seconds (an execution-failure signature).
    * Every ``interval`` seconds it reconciles total broker-reported equity
      against the internal virtual ledger; drift beyond ``discrepancy_tol``
      is treated as a hard fault.
    * On EITHER fault it fires a single global PANIC: latch the engine
      kill-switch, close the (paper) position, cancel/flatten on every broker,
      and raise an operator alert. In --live mode the broker calls hit the
      unimplemented stubs, so the alert still fires and the log demands manual
      intervention rather than pretending the close succeeded.
    """

    def __init__(self, portfolio: "Portfolio", brokers, *, live: bool,
                 interval: float = RECONCILE_INTERVAL_SEC,
                 unhedged_grace: float = UNHEDGED_GRACE_SEC,
                 discrepancy_tol: float = DISCREPANCY_TOL_USD,
                 initial_equity: float = INITIAL_EQUITY_USD) -> None:
        self.portfolio = portfolio
        self.brokers = list(brokers)
        self.live = live
        self.interval = float(interval)
        self.unhedged_grace = float(unhedged_grace)
        self.discrepancy_tol = float(discrepancy_tol)
        self.initial_equity = float(initial_equity)   # baseline = order size
        self._last_reconcile = 0.0
        self.panicked = False

    async def loop(self, stop: asyncio.Event) -> None:
        self._last_reconcile = time.monotonic()   # first full reconcile after 1 interval
        while not stop.is_set():
            await _sleep_or_stop(RECON_TICK_SEC, stop)
            if stop.is_set():
                break
            try:
                await self._check_unhedged_leg()
                if time.monotonic() - self._last_reconcile >= self.interval:
                    self._last_reconcile = time.monotonic()
                    await self._reconcile_equity()
            except asyncio.CancelledError:
                raise
            except Exception as exc:   # a guard hiccup must never crash the core
                print(YELLOW + f"[{_ts()}] ⚠️  Reconciliation tick error "
                      f"({type(exc).__name__}): {exc}" + RESET)

    async def _check_unhedged_leg(self) -> None:
        # Scan EVERY symbol — any one unhedged leg past grace trips the panic.
        for eng in self.portfolio.engines.values():
            pos = eng.ledger.open_trade
            if pos is None or pos.hedged or pos.unhedged_since is None:
                continue
            held = time.monotonic() - pos.unhedged_since
            if held > self.unhedged_grace:
                await self._panic(f"UNHEDGED LEG on {eng.spec.name} exposed "
                                  f"{held:0.1f}s (> {self.unhedged_grace:0.0f}s "
                                  f"grace) — execution failure on the partner leg.")
                return

    async def _reconcile_equity(self) -> None:
        expected = self.initial_equity + self.portfolio.realized_pnl
        reported = 0.0
        for b in self.brokers:
            reported += await b.fetch_equity()
        drift = reported - expected
        if abs(drift) > self.discrepancy_tol:
            await self._panic(f"EQUITY DISCREPANCY ${drift:+.2f}: brokers report "
                              f"${reported:.2f} vs ledger ${expected:.2f} "
                              f"(tol ${self.discrepancy_tol:.2f}).")
        else:
            print(GRAY + f"[{_ts()}] 🧮 Reconciliation OK — brokers ${reported:.2f} "
                  f"≈ ledger ${expected:.2f} (drift ${drift:+.2f})." + RESET)

    async def _panic(self, reason: str) -> None:
        if self.panicked:
            return
        self.panicked = True
        send_alert(f"PANIC CLOSE TRIGGERED — {reason}")
        # 1) Latch EVERY symbol's kill-switch so no new entries open anywhere.
        self.portfolio.halt_all("panic")
        # 2) Force-close every open (paper) position across all symbols.
        for eng in self.portfolio.engines.values():
            if eng.ledger.open_trade is not None:
                pnl = eng.ledger.close_position()
                print(RED + f"[{_ts()}] 🛑 {eng.spec.name} paper position "
                      f"force-closed (PnL ${pnl:+.4f})." + RESET)
        # 3) Cancel working orders + flatten on EVERY venue. In --live this hits
        #    the unimplemented stubs, which we surface loudly (no silent close).
        for b in self.brokers:
            try:
                n_orders = await b.cancel_all_orders()
                n_pos = await b.close_all_positions()
                print(RED + f"[{_ts()}] 🛑 {b.name}: cancelled {n_orders} order(s), "
                      f"flattened {n_pos} position(s)." + RESET)
            except NotImplementedError as exc:
                print(RED + f"[{_ts()}] ‼️  {b.name}: AUTOMATED CLOSE UNAVAILABLE "
                      f"({exc}) — MANUAL INTERVENTION REQUIRED." + RESET)
        print(RED + f"[{_ts()}] ⛔ All engines HALTED. Restart required after "
              f"review. (Session PnL ${self.portfolio.realized_pnl:+.4f})" + RESET)


def build_brokers(creds: "ExchangeCredentials", equity_source, live: bool,
                  initial_equity: float = INITIAL_EQUITY_USD):
    """Build one broker PER execution venue: LiveBroker stubs under --live, else
    SimulatedBroker.

    All three venues are first-class, so a panic close cancels/flattens on each.
    ``equity_source`` is anything exposing ``.realized_pnl`` (a single Ledger or
    the whole Portfolio). Each simulated venue reports an equal third of the
    virtual equity (baseline ``initial_equity``, tracking the configured order
    size) so the trio sums to baseline + realized PnL — i.e. zero drift when
    healthy.
    """
    if live:
        return [
            LiveBroker("Binance", creds.binance_key, creds.binance_secret),
            LiveBroker("Bybit", creds.bybit_key, creds.bybit_secret),
            LiveBroker("OKX", creds.okx_key, creds.okx_secret,
                       creds.okx_passphrase),
        ]
    share = initial_equity / 3.0
    return [
        SimulatedBroker("Binance", lambda: share + equity_source.realized_pnl / 3.0),
        SimulatedBroker("Bybit", lambda: share + equity_source.realized_pnl / 3.0),
        SimulatedBroker("OKX", lambda: share + equity_source.realized_pnl / 3.0),
    ]


# ---------------------------------------------------------------------------
# Consolidated multi-asset dashboard (one high-density matrix, 1s refresh)
# ---------------------------------------------------------------------------

def _venue_status(portfolio: "Portfolio") -> str:
    """A compact per-venue connectivity header (shared socket per venue)."""
    any_eng = next(iter(portfolio.engines.values()))
    cells = []
    for venue, label in (("binance", "Binance"), ("bybit", "Bybit"),
                         ("okx", "OKX")):
        book = any_eng.state._book(venue)
        if not book.connected:
            mk = f"{RED}○ down{RESET}"
        elif book.stale:
            mk = f"{YELLOW}↯ stale{RESET}"
        else:
            mk = f"{GREEN}● ok{RESET}"
        cells.append(f"{label} {mk}")
    return "   ".join(cells)


async def _dashboard_loop(portfolio: "Portfolio", stop: asyncio.Event) -> None:
    """Draw ONE consolidated dashboard for ALL symbols every second.

    Printing five separate tables would flood the terminal, so each asset gets a
    single dense row: consolidated best bid/ask across the three venues, which
    venues are live, the current max net-of-fee basis, and its execution state.
    """
    n = len(portfolio.engines)
    while not stop.is_set():
        rows = []
        open_count = 0
        for name, eng in portfolio.engines.items():
            st = eng.state
            bids = [b.bid for b in st.books() if b.bid > 0]
            asks = [b.ask for b in st.books() if b.ask > 0]
            best_bid = max(bids) if bids else 0.0   # consolidated best bid
            best_ask = min(asks) if asks else 0.0   # consolidated best ask
            # Which venues are HEALTHY enough to price against (B / Y / O dots).
            # A stale/down venue shows "·" so its exclusion is visible per row.
            dots = "".join(
                (GREEN + "●" + RESET) if b.healthy else "·"
                for b in st.books())
            edges = evaluate_edges(st)
            best = edges[0] if edges else None
            if best is None:
                basis = f"{GRAY}   --   {RESET}"
            elif best.is_open and best.bps >= portfolio.min_spread_bps:
                # Tradeable: clears fees AND the configured spread hurdle.
                basis = f"{GREEN}{best.bps:+7.2f}✅{RESET}"
            else:
                basis = f"{best.bps:+7.2f} "
            if eng.ledger.open_trade is not None:
                open_count += 1
            rows.append((name, best_bid, best_ask, dots, basis, eng.status,
                         eng.ledger.realized_pnl))

        print(f"\n┌─ [{_ts()}] MULTI-ASSET PERP ARB · {n} symbols · "
              f"Binance ⇄ Bybit ⇄ OKX · taker {TAKER_FEE_PER_FILL*1e2:.2f}%"
              f"/fill ×4 (entry+exit) ─")
        print(f"│ venues  {_venue_status(portfolio)}")
        print(f"│ {'ASSET':<6}{'best bid':>14}{'best ask':>14}  {'B/Y/O':<7}"
              f"{'maxNet(bps)':>13}  {'state':<12}{'pnl($)':>10}")
        for name, bid, ask, dots, basis, status, pnl in rows:
            print(f"│ {name:<6}{_fmt_px(bid):>14}{_fmt_px(ask):>14}  {dots:<7}"
                  f"  {basis:>11}  {status:<12}{pnl:>+10.4f}")
        print(f"└ open positions={open_count}/{n}  "
              f"session PnL ${portfolio.realized_pnl:+.4f}  "
              f"({portfolio.trade_count} trade(s))")
        await _sleep_or_stop(PRINT_INTERVAL_SEC, stop)


# ---------------------------------------------------------------------------
# End-of-session summary (printed once on shutdown)
# ---------------------------------------------------------------------------

def _print_session_summary(portfolio: "Portfolio",
                           logger: "TradeLogger | None",
                           started_at: float, live: bool) -> None:
    """Print a consolidated end-of-run report: totals, per-asset P&L, journal."""
    elapsed = max(0.0, time.monotonic() - started_at)
    mins, secs = divmod(int(elapsed), 60)
    total_trades = portfolio.trade_count
    total_pnl = portfolio.realized_pnl
    mode = "LIVE" if live else "SIMULATION"

    print("\n" + "═" * 70)
    print(f" SESSION SUMMARY · {mode} · uptime {mins:d}m {secs:02d}s")
    print("═" * 70)
    print(f" {'ASSET':<8}{'trades':>8}{'realized PnL($)':>18}")
    # Stable display order = the configured target order.
    traded = [(n, e) for n, e in portfolio.engines.items()
              if e.ledger.trade_count]
    for name, eng in portfolio.engines.items():
        led = eng.ledger
        if led.trade_count:
            print(f" {name:<8}{led.trade_count:>8}{led.realized_pnl:>+18.4f}")
    if not traded:
        print(f" {GRAY}(no trades executed this session){RESET}")
    else:
        best = max(traded, key=lambda kv: kv[1].ledger.realized_pnl)
        worst = min(traded, key=lambda kv: kv[1].ledger.realized_pnl)
        print("─" * 70)
        print(f" best:  {best[0]:<6} ${best[1].ledger.realized_pnl:+.4f}    "
              f"worst: {worst[0]:<6} ${worst[1].ledger.realized_pnl:+.4f}")
    print("─" * 70)
    color = GREEN if total_pnl >= 0 else RED
    print(f" TOTAL  {total_trades} trade(s)   "
          f"session PnL {color}${total_pnl:+.4f}{RESET}")
    # Execution-realism stats: how many intended entries the frictions cost us.
    missed = portfolio.missed_fills
    if portfolio.realism is not None and portfolio.realism.enabled:
        attempted = total_trades + len(portfolio.open_engines()) + missed
        print(f" Realism: ON · {missed} missed fill(s) of {attempted} "
              f"attempted entr{'y' if attempted == 1 else 'ies'} "
              f"(slippage/latency/no-fill)")
    if logger is not None:
        print(f" Journal: {logger.count} trade(s) persisted → {logger.path}")
    print("═" * 70)


# ---------------------------------------------------------------------------
# Orchestration: boot streams + gate + watchdog + printer, run until stopped
# ---------------------------------------------------------------------------

async def run_live(config: "BotConfig",
                   stale_threshold: float = DEFAULT_STALE_THRESHOLD,
                   force_live: bool = False) -> None:
    # dry_run is the canonical safety switch (config-driven); the CLI --live flag
    # is an explicit operator override on top of it.
    live = force_live or (not config.dry_run)
    specs = config.specs
    mode = (RED + "LIVE (authenticated)" + RESET) if live else "SIMULATION (dry-run)"
    symbols = ", ".join(s.name for s in specs)
    print("=" * 70)
    print(" LIVE CORE — MULTI-ASSET cross-venue PERPETUAL arbitrage (WS)")
    print(" Binance USDⓈ-M  ×  Bybit V5 linear  ×  OKX Futures")
    print(f" Tracking {len(specs)} symbols: {symbols}")
    print(f" Mode: {mode} · public book streams · reconciliation guard active")
    print(f" Config: {config.source} · order ${config.order_size_usd:,.2f} · "
          f"min spread {config.min_spread_bps:.2f} bps hurdle")
    rp = config.realism
    if rp.enabled:
        def _eff(label, on, detail):
            mark = (GREEN + label + RESET) if on else (GRAY + label + "·off" + RESET)
            return f"{mark}({detail})" if on else mark
        print(f" Realism: ON (seed {rp.seed}) · "
              + " ".join([
                  _eff("slip", rp.slippage,
                       f"{rp.slippage_bps:.1f}×{rp.slippage_mult:.1f}"
                       f"+{rp.impact_bps_per_10k:.1f}/$10k bps"),
                  _eff("latency", rp.latency,
                       f"{rp.latency_ms:.0f}ms ±{rp.latency_adverse_bps:.1f}/100ms"),
                  _eff("miss", rp.missed_fills, f"p={rp.fill_probability:.2f}"),
                  _eff("partial", rp.partial_fills, f"p={rp.partial_fill_prob:.2f}"),
                  _eff("legfail", rp.leg_failure, f"p={rp.leg_failure_prob:.2f}"),
                  _eff("funding", rp.funding, f"{rp.funding_rate_8h_bps:+.1f}bps/8h"),
              ]))
    else:
        print(" Realism: OFF (idealised top-of-book fills)")
    print(f" Taker buffer {TAKER_FEE_PER_FILL*1e2:.2f}%/fill (entry+exit ⇒ "
          f"{TAKER_FEE*1e2:.2f}% round-trip per leg) · "
          f"stale flag after {stale_threshold:0.0f}s of silence")
    size_note = (f"  ⚠ order ${config.order_size_usd:,.2f} > ceiling — every "
                 f"clip clamped to ${MAX_ORDER_USD:.2f}"
                 if config.order_size_usd > MAX_ORDER_USD else "")
    print(f" Guardrails: size cap ${MAX_ORDER_USD:.2f}/clip · circuit breaker "
          f"{CB_MAX_TRADES_PER_MIN} trades/{CB_WINDOW_SEC:.0f}s · "
          f"{CB_MAX_CONSECUTIVE_LOSSES} losses · {CB_MAX_MISSED_FILL_STREAK} "
          f"missed-fill streak · edge floor {CB_MIN_AVG_EDGE_BPS:.1f}bps/"
          f"{CB_EDGE_WINDOW} → halt{size_note}")
    print("=" * 70)
    for w in config.warnings:
        print(YELLOW + f"[{_ts()}] ⚠️  config: {w}" + RESET)

    # Feature 1: load credentials securely (never logged). The public book
    # streams need no auth; keys are consumed only by the authenticated
    # reconciliation layer when --live is set.
    creds = load_credentials()
    if not _HAS_DOTENV:
        print(f"[{_ts()}] (python-dotenv not installed — reading env directly; "
              f"`pip install python-dotenv` to use a .env file)")
    print(f"[{_ts()}] 🔑 Credentials: "
          f"Binance {'present ✓' if creds.has_binance else 'absent —'}, "
          f"Bybit {'present ✓' if creds.has_bybit else 'absent —'}, "
          f"OKX {'present ✓' if creds.has_okx else 'absent —'}")

    if not live:
        print(GREEN + f"[{_ts()}] 🧪 DRY-RUN: fills are simulated and journaled "
              f"to {TRADE_LOG_PATH}; NO exchange order-submission endpoints are "
              f"called." + RESET)

    if live:
        if not creds.has_all:
            raise SystemExit(
                "--live / dry_run=false requires credentials for ALL THREE "
                "execution venues: "
                "BINANCE_API_KEY/BINANCE_SECRET_KEY, BYBIT_API_KEY/"
                "BYBIT_SECRET_KEY, and OKX_API_KEY/OKX_SECRET_KEY/OKX_PASSPHRASE "
                "in the environment or .env.")
        print(RED + f"[{_ts()}] ⚠️  --live ENABLED: the reconciliation guard will "
              "attempt authenticated equity reads and panic-close on real "
              "venues. The live broker adapters are UNIMPLEMENTED stubs — a "
              "panic will alert and demand manual intervention, NOT auto-close."
              + RESET)

    # Persistent trade journal: append-only CSV, written off the hot path by a
    # background writer thread (one flushed row per executed round-trip).
    trade_logger = TradeLogger(TRADE_LOG_PATH)
    print(f"[{_ts()}] 📓 Trade journal → {trade_logger.path} "
          f"(append-only, flushed per trade)")

    # One isolated engine per symbol (book + ledger + gate, per-symbol locks).
    # Trade size, the spread hurdle, and the execution-realism model all come
    # straight from the config.
    portfolio = Portfolio(specs, config.order_size_usd, logger=trade_logger,
                          min_spread_bps=config.min_spread_bps,
                          realism=config.realism,
                          circuit_breaker_enabled=True)
    watchdog = StalenessWatchdog(portfolio, stale_threshold)
    portfolio.wire_ws_frame(watchdog.on_ws_frame)   # instant stale stand-down
    brokers = build_brokers(creds, portfolio, live,
                            initial_equity=config.order_size_usd)
    guard = ReconciliationGuard(portfolio, brokers, live=live,
                                initial_equity=config.order_size_usd)

    stop = asyncio.Event()
    started_at = time.monotonic()
    # One socket PER VENUE multiplexes all symbols; the gates + dashboard fan
    # out across the portfolio.
    tasks = [
        asyncio.create_task(stream_binance(portfolio, stop), name="binance"),
        asyncio.create_task(stream_bybit(portfolio, stop), name="bybit"),
        asyncio.create_task(stream_okx(portfolio, stop), name="okx"),
        asyncio.create_task(watchdog.watchdog_loop(stop), name="watchdog"),
        asyncio.create_task(guard.loop(stop), name="reconcile"),
        asyncio.create_task(_dashboard_loop(portfolio, stop), name="dashboard"),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Always emit the summary and flush/close the journal on the way out,
        # whether we stopped cleanly or were cancelled by Ctrl-C.
        _print_session_summary(portfolio, trade_logger, started_at, live)
        trade_logger.close()


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="live_core",
        description="Event-driven multi-asset Binance × Bybit × OKX perpetual "
                    "arbitrage mirror (read-only simulation).",
        epilog="Tip: `python live_core.py config --help` to view/edit "
               "simulation parameters and toggle realism effects.",
    )
    p.add_argument(
        "--config",
        default=CONFIG_PATH,
        metavar="PATH",
        help=f"Path to the JSON config (default: {CONFIG_PATH}). Missing file "
             "falls back to built-in defaults.",
    )
    p.add_argument(
        "--stale-threshold",
        type=float,
        default=DEFAULT_STALE_THRESHOLD,
        metavar="SECONDS",
        help="Seconds of WebSocket silence before a venue is flagged stale "
             f"(default: {DEFAULT_STALE_THRESHOLD:0.0f}).",
    )
    p.add_argument(
        "--live",
        action="store_true",
        help="DANGER: override config dry_run and enable the authenticated "
             "reconciliation / panic-close layer against real venues (requires "
             "API credentials). The live broker adapters are unimplemented "
             "stubs by design — see LiveBroker. Default: config dry_run decides "
             "(simulation when true).",
    )
    return p.parse_args(argv)


def main() -> None:
    argv = sys.argv[1:]
    # `python live_core.py config ...` dispatches to the config editor instead
    # of starting the bot.
    if argv and argv[0] == "config":
        raise SystemExit(_config_cli(argv[1:]))

    args = _parse_args(argv)
    if args.stale_threshold <= 0:
        raise SystemExit("--stale-threshold must be a positive number of seconds.")
    if not _HAS_WEBSOCKETS:
        raise SystemExit(
            f"live_core needs websockets:  pip install websockets "
            f"({_WS_IMPORT_ERR})"
        )
    # Init phase: parse config.json into a validated BotConfig (fail-fast on a
    # malformed file) before any streams spin up.
    config = load_config(args.config)
    live = args.live or (not config.dry_run)
    try:
        asyncio.run(run_live(config, stale_threshold=args.stale_threshold,
                             force_live=args.live))
    except KeyboardInterrupt:
        tail = ("" if live
                else " (Simulation only — no positions were real.)")
        print(f"\n👋 Stopped.{tail}")


if __name__ == "__main__":
    main()
