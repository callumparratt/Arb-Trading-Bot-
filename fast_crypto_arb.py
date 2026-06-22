#!/usr/bin/env python3
"""
fast_crypto_arb.py
==================

Read-only arbitrage *simulator* for 15-minute Bitcoin interval markets across
Polymarket and Kalshi.

This script is for educational / paper-trading purposes ONLY:

    * No live execution logic.
    * No private keys, wallets, API keys, or credentials.
    * Only public, unauthenticated endpoints are used.

What it does
------------
1. Discovery phase: finds the *current* active 15-minute BTC interval contract
   on both venues and matches them by expiration context.
2. Order-book polling: every 2 seconds, pulls top-of-book for both outcomes
   on both venues and normalizes everything to a 0.00 - 1.00 probability scale.
3. Math engine + mock ledger: detects cross-venue arbitrage spreads, opens a
   simulated $100 paper trade, and watches for an early profitable close-out.
4. Auto-rollover: when the 15-minute window settles, prints a PnL summary,
   purges stale tokens, re-runs discovery, and locks onto the next block.

Run:
    pip install httpx
    python fast_crypto_arb.py
"""

import asyncio
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# The console output uses emoji/box-drawing characters. On a default Windows
# console (cp1252) those raise UnicodeEncodeError and crash the script, so
# force UTF-8 with a safe fallback. (No-op on terminals that already do UTF-8.)
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

try:
    import httpx
except ImportError:  # pragma: no cover
    raise SystemExit(
        "This script requires httpx. Install it with:  pip install httpx"
    )

# Fee math for the fee-aware ledger. fee_engine has no heavy deps and does not
# import this module, so there is no import cycle.
import fee_engine


def _venue_fee(venue: str, price, contracts) -> float:
    """Taker fee for executing ``contracts`` at ``price`` on ``venue``.

    Used for both entry (buying at the ask) and exit (selling into the bid),
    since taker fees apply symmetrically on Kalshi and Polymarket.
    """
    if venue == "Polymarket":
        return fee_engine.calc_polymarket_fee(price, contracts)
    if venue == "Kalshi":
        return fee_engine.calc_kalshi_fee(price, contracts)
    return 0.0


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POLL_INTERVAL_SEC = 2.0          # main loop cadence
REQUEST_TIMEOUT_SEC = 8.0        # per-request timeout
ARB_THRESHOLD = 0.96            # combined cost below this = arbitrage edge
PAPER_STAKE_USD = 100.0          # notional per simulated trade
EARLY_CLOSE_MIN_PROFIT = 0.50    # min $ profit to trigger an early close-out

# Discovery uses the Gamma API, NOT the CLOB /markets endpoint. CLOB /markets
# is paginated over *thousands* of historical markets (page 1 is full of 2023
# archives like "$20k or $30k first?") and does not surface the current short
# interval. Gamma supports real filtering: closed=false + order by start date
# returns the freshest live markets first. We still hit the CLOB /book endpoint
# for order books, since the Gamma clobTokenIds map straight to CLOB tokens.
# Discovery must find the market whose 15m window is LIVE NOW. Sorting by
# startDate surfaces the most recently *created* market (often a window a day
# ahead), NOT the currently-trading one. Instead filter end_date_min >= now and
# sort by endDate ascending, so the nearest upcoming expiry — the live interval
# — comes first. (end_date_min is injected per-scan via _poly_markets_url().)
POLY_MARKETS_BASE = "https://gamma-api.polymarket.com/markets"
POLY_BOOK_URL = "https://clob.polymarket.com/book"


def _poly_markets_url() -> str:
    """Live Polymarket markets ending from ~1 minute ago onward, soonest first.

    The small negative skew (now - 60s) keeps the in-progress window in view
    right up to its settlement instead of dropping it a minute early.
    """
    floor = _utcnow().replace(microsecond=0) - timedelta(seconds=60)
    end_min = floor.isoformat().replace("+00:00", "Z")
    return (
        f"{POLY_MARKETS_BASE}?closed=false&limit=100"
        f"&order=endDate&ascending=true&end_date_min={end_min}"
    )

# Kalshi: the external-api.* host returns null quote fields (yes_bid/yes_ask all
# None). The canonical public host api.elections.kalshi.com serves real depth,
# but only via the dedicated /orderbook endpoint — the market summary fields are
# still null on these 15m markets, so we MUST read the orderbook for quotes.
KALSHI_HOST = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_MARKETS_URL = (
    f"{KALSHI_HOST}/markets?series_ticker=KXBTC15M&status=open"
)
KALSHI_MARKET_URL = f"{KALSHI_HOST}/markets"  # /<ticker>/orderbook appended

KALSHI_SERIES_PREFIX = "KXBTC15M"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Quote:
    """Top-of-book for a single outcome, normalized to 0..1 probabilities.

    Field order is (best_bid, best_ask) to match ws_mirror.Quote exactly, so a
    positional Quote(...) means the same thing in both modules.
    """
    best_bid: Optional[float] = None  # price to SELL this outcome
    best_ask: Optional[float] = None  # price to BUY this outcome

    def is_valid(self) -> bool:
        return self.best_ask is not None or self.best_bid is not None


@dataclass
class MarketPair:
    """A matched Polymarket <-> Kalshi 15-minute BTC interval."""
    # Polymarket
    poly_question: str
    poly_yes_token: str
    poly_no_token: str
    # Kalshi
    kalshi_ticker: str
    kalshi_title: str
    # Per-venue settlement times (kept separate so we can guard alignment).
    poly_expiry: Optional[datetime] = None
    kalshi_expiry: Optional[datetime] = None

    @property
    def expiry(self) -> Optional[datetime]:
        """Effective settlement time used for rollover (prefer Kalshi's)."""
        return self.kalshi_expiry or self.poly_expiry

    def aligned(self) -> bool:
        """True only if both venues settle at the EXACT same instant.

        Arbitrage is only risk-free across two legs that resolve on the same
        window; a mismatch means the BTC price can move between the two
        settlements, so any apparent edge is directional, not locked.
        """
        if self.poly_expiry is None or self.kalshi_expiry is None:
            return False
        return self.poly_expiry == self.kalshi_expiry

    @staticmethod
    def _fmt_dt(dt: Optional[datetime]) -> str:
        # Include the date: the two venues can share a time-of-day but sit a
        # full day apart, so an HH:MM:SS-only label would look falsely equal.
        return dt.strftime("%b-%d %H:%M:%S UTC") if dt else "unknown"

    def poly_expiry_str(self) -> str:
        return self._fmt_dt(self.poly_expiry)

    def kalshi_expiry_str(self) -> str:
        return self._fmt_dt(self.kalshi_expiry)

    def expiry_str(self) -> str:
        return self._fmt_dt(self.expiry)

    def seconds_to_expiry(self) -> Optional[float]:
        if self.expiry is None:
            return None
        return (self.expiry - _utcnow()).total_seconds()


@dataclass
class PaperTrade:
    """A single simulated $100 arb position held in memory."""
    leg_name: str            # human description of the two legs
    venue_up: str            # which venue we bought UP on
    venue_down: str          # which venue we bought DOWN on
    entry_cost: float        # combined cost per $1 payout (0..2)
    contracts: float         # number of $1-payout contracts bought
    opened_at: float = field(default_factory=time.time)
    ticker: str = ""
    closed: bool = False
    close_pnl: float = 0.0
    close_reason: str = ""
    # Fee-adjusted expected profit of the chosen routing path ($), when known.
    net_margin: Optional[float] = None
    setup: str = ""          # "A" / "B" — which construction was routed
    # Per-leg entry asks and the taker fees paid to open the position. These
    # persist so settlement and early-close PnL can be fully net-of-fee.
    up_price: float = 0.0
    down_price: float = 0.0
    total_entry_fees: float = 0.0


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return _utcnow().strftime("%H:%M:%S")


def _ts_us() -> str:
    """Microsecond-precision wall-clock stamp for latency-sensitive logs."""
    return _utcnow().strftime("%H:%M:%S.%f")


def _norm_price(raw: Any) -> Optional[float]:
    """Normalize a price that may be cents (int 0..100) or a 0..1 float."""
    if raw is None:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if val < 0:
        return None
    # Kalshi commonly returns cents (1..100). Anything > 1.0 is treated as cents.
    if val > 1.0:
        val = val / 100.0
    if val > 1.0:  # still out of range -> bogus
        return None
    return round(val, 4)


def _fmt(v: Optional[float]) -> str:
    return f"{v:0.3f}" if isinstance(v, (int, float)) else "  -  "


def _parse_iso(s: Any) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Discovery phase
# ---------------------------------------------------------------------------

async def _fetch_json(client: httpx.AsyncClient, url: str) -> Optional[Any]:
    """GET a URL and return parsed JSON, or None on any failure."""
    try:
        resp = await client.get(url, timeout=REQUEST_TIMEOUT_SEC)
        if resp.status_code == 429:
            print(f"[{_ts()}] ⚠️  Rate limited (429) on {url[:60]}... backing off")
            await asyncio.sleep(2.0)
            return None
        # 404 = stale/wrong token id. Stay quiet here; the polling layer
        # detects the failed fetch and triggers a clean re-discovery.
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except (httpx.TimeoutException, httpx.HTTPError) as exc:
        print(f"[{_ts()}] ⚠️  Request failed ({type(exc).__name__}): {url[:60]}...")
        return None
    except ValueError:
        print(f"[{_ts()}] ⚠️  Bad JSON from {url[:60]}...")
        return None


# Keywords that mark an unrelated *macro* BTC market we must never pick
# (e.g. "Will BTC hit $20k or $30k first?", year/price-target bets).
_POLY_MACRO_EXCLUDE = (
    "first", "hit", "reach", "all-time", "all time", "ath", "by 2024",
    "by 2025", "by 2026", "this year", "end of", "$20k", "$30k", "$40k",
    "$50k", "$100k", "$150k", "$200k", "20k", "30k", "100k", "halving",
    "dip to", "above $", "below $", "between $",
)

# The live Polymarket 15-minute market is titled by its clock window, e.g.
# "Bitcoin Up or Down - June 19, 11:45AM-12:00PM ET" — the string "15m" only
# appears in the SLUG: "btc-updown-15m-<unix_ts>". So the slug is the reliable
# interval signal; the title tokens are kept as a secondary check.
_POLY_15M_SLUGS = ("btc-updown-15m", "bitcoin-updown-15m", "btc-up-or-down-15m")
_POLY_15M_TOKENS = ("15m", "15-minute", "15 minute", "15 min", "15min", "15-min")
# Must look like a Bitcoin up/down market (guards against e.g. ETH 15m slugs
# that share the "-updown-15m" shape).
_POLY_BTC_TOKENS = ("btc", "bitcoin")
_POLY_UPDOWN_TOKENS = ("up or down", "updown", "up/down")


def _poly_volume(market: Dict[str, Any]) -> float:
    """Best-effort 24h volume for tie-breaking, 0.0 if absent."""
    for k in ("volume24hr", "volumeNum", "volume_24hr", "volume24Hr",
              "volume_24h", "volume"):
        v = market.get(k)
        try:
            if v is not None:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _extract_poly_candidates(payload: Any) -> List[Dict[str, Any]]:
    """Pull BTC 15-minute interval markets out of the Gamma payload.

    Identification is slug-driven: the live market slug is
    ``btc-updown-15m-<unix_ts>``. The human title only carries a clock window
    ("Bitcoin Up or Down - June 19, 11:45AM-12:00PM ET"), so we confirm it is
    a Bitcoin up/down market via the title and require the 15m slug shape.
    Macro price-target markets (e.g. "$20k or $30k first?") are dropped.
    """
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = payload.get("data") or payload.get("markets") or []
    else:
        rows = []

    out: List[Dict[str, Any]] = []
    for m in rows:
        if not isinstance(m, dict):
            continue
        slug = str(m.get("slug") or m.get("market_slug") or "").lower()
        title = str(m.get("question") or m.get("title") or "").lower()
        text = f"{title} {slug}"

        # Must be a Bitcoin up/down market...
        if not any(t in text for t in _POLY_BTC_TOKENS):
            continue
        if not any(t in text for t in _POLY_UPDOWN_TOKENS):
            continue
        # ...must NOT be a macro price-target / long-horizon market...
        if any(bad in text for bad in _POLY_MACRO_EXCLUDE):
            continue
        # ...and must be the 15-minute interval (slug is authoritative).
        is_15m = any(s in slug for s in _POLY_15M_SLUGS) or any(
            tok in text for tok in _POLY_15M_TOKENS
        )
        if not is_15m:
            continue

        token_ids = _poly_token_ids(m)
        if len(token_ids) < 2:
            continue

        out.append(
            {
                "question": m.get("question") or slug or "BTC 15m",
                # outcomes are ["Up", "Down"] -> token[0]=UP(YES), token[1]=DOWN(NO)
                "yes_token": token_ids[0],
                "no_token": token_ids[1],
                "expiry": _parse_iso(
                    m.get("endDate")
                    or m.get("end_date_iso")
                    or m.get("game_start_time")
                ),
                "accepting": m.get("acceptingOrders", m.get("active", True)),
                "tier": "exact",
                "volume": _poly_volume(m),
            }
        )
    return out


def _poly_token_ids(market: Dict[str, Any]) -> List[str]:
    """Extract YES/NO CLOB token ids from a Polymarket market record."""
    raw = market.get("clobTokenIds") or market.get("clob_token_ids")
    if isinstance(raw, str):
        # Sometimes serialized as a JSON-ish string '["123","456"]'
        ids = re.findall(r'"(\d+)"', raw)
        if ids:
            return ids
        return [x.strip() for x in raw.strip("[]").split(",") if x.strip()]
    if isinstance(raw, list):
        return [str(x) for x in raw]
    # Fallback: tokens array with token_id fields
    tokens = market.get("tokens")
    if isinstance(tokens, list):
        return [str(t.get("token_id")) for t in tokens if t.get("token_id")]
    return []


def _extract_kalshi_candidates(payload: Any) -> List[Dict[str, Any]]:
    """Pull open BTC 15-minute markets out of the Kalshi payload."""
    if not isinstance(payload, dict):
        return []
    markets = payload.get("markets") or []
    out: List[Dict[str, Any]] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        ticker = m.get("ticker", "")
        if not ticker.startswith(KALSHI_SERIES_PREFIX):
            continue
        out.append(
            {
                "ticker": ticker,
                "title": m.get("title") or m.get("subtitle") or ticker,
                "expiry": _parse_iso(
                    m.get("close_time") or m.get("expiration_time")
                ),
                # Carry any prices already present so we avoid an extra call.
                "yes_bid": m.get("yes_bid"),
                "yes_ask": m.get("yes_ask"),
                "no_bid": m.get("no_bid"),
                "no_ask": m.get("no_ask"),
            }
        )
    # Sort by soonest expiry so index 0 is the live interval.
    out.sort(key=lambda c: c["expiry"] or _utcnow())
    return out


def _match_pair(
    poly: List[Dict[str, Any]], kalshi: List[Dict[str, Any]]
) -> Optional[MarketPair]:
    """Match a Polymarket market to a Kalshi ticker by nearest expiry."""
    if not poly or not kalshi:
        return None

    # Prefer the soonest-expiring open Kalshi interval that is still in future.
    now = _utcnow()
    live_kalshi = [
        k for k in kalshi if (k["expiry"] is None or k["expiry"] > now)
    ] or kalshi
    k = live_kalshi[0]

    # Match Polymarket by closest expiry to the chosen Kalshi interval,
    # breaking ties by higher 24h volume.
    def expiry_distance(p: Dict[str, Any]) -> float:
        if p["expiry"] is None or k["expiry"] is None:
            return 1e9
        return abs((p["expiry"] - k["expiry"]).total_seconds())

    poly_live = [p for p in poly if p.get("accepting", True)] or poly

    # Tier 1: explicit 15-minute markets. Tier 2 (fallback): Bitcoin-price
    # intraday markets, picked by closest expiry / highest volume.
    exact = [p for p in poly_live if p.get("tier") == "exact"]
    pool = exact or poly_live
    p = min(pool, key=lambda c: (expiry_distance(c), -c.get("volume", 0.0)))

    if not exact:
        print(f"[{_ts()}] ℹ️  No exact 15m Polymarket market — falling back to "
              f"closest Bitcoin-price market by expiry/volume.")

    return MarketPair(
        poly_question=p["question"],
        poly_yes_token=p["yes_token"],
        poly_no_token=p["no_token"],
        kalshi_ticker=k["ticker"],
        kalshi_title=k["title"],
        # Keep each venue's settlement time separate for the alignment guard.
        poly_expiry=p["expiry"],
        kalshi_expiry=k["expiry"],
    )


async def discover(client: httpx.AsyncClient) -> Optional[MarketPair]:
    """Run the full discovery phase and return a matched pair (or None)."""
    print(f"[{_ts()}] 🔎 Discovery: scanning for active BTC 15m interval...")
    poly_payload, kalshi_payload = await asyncio.gather(
        _fetch_json(client, _poly_markets_url()),
        _fetch_json(client, KALSHI_MARKETS_URL),
    )

    poly = _extract_poly_candidates(poly_payload) if poly_payload else []
    kalshi = _extract_kalshi_candidates(kalshi_payload) if kalshi_payload else []

    if not poly:
        print(f"[{_ts()}] ⚠️  No Polymarket BTC 15m candidates found this scan.")
    if not kalshi:
        print(f"[{_ts()}] ⚠️  No Kalshi {KALSHI_SERIES_PREFIX} candidates found.")

    pair = _match_pair(poly, kalshi)
    if pair is None:
        print(f"[{_ts()}] ❌ Could not lock a matched pair. Will retry.")
        return None

    sync = "in sync" if pair.aligned() else "OUT OF SYNC"
    print(f"[{_ts()}] ✅ Locked interval ({sync})")
    print(f"          Poly : {pair.poly_question}  → {pair.poly_expiry_str()}")
    print(f"          Kalshi: {pair.kalshi_ticker}  → {pair.kalshi_expiry_str()}")
    return pair


# ---------------------------------------------------------------------------
# Order-book polling phase
# ---------------------------------------------------------------------------

def _book_top(book: Any) -> Quote:
    """Reduce a Polymarket CLOB book payload to top-of-book Quote."""
    q = Quote()
    if not isinstance(book, dict):
        return q

    asks = book.get("asks") or []
    bids = book.get("bids") or []

    # Polymarket returns asks ascending-ish; best ask = lowest price,
    # best bid = highest price. Be defensive about ordering.
    ask_prices = [_norm_price(a.get("price")) for a in asks if isinstance(a, dict)]
    bid_prices = [_norm_price(b.get("price")) for b in bids if isinstance(b, dict)]
    ask_prices = [a for a in ask_prices if a is not None]
    bid_prices = [b for b in bid_prices if b is not None]

    if ask_prices:
        q.best_ask = min(ask_prices)
    if bid_prices:
        q.best_bid = max(bid_prices)
    return q


async def poll_polymarket(
    client: httpx.AsyncClient, pair: MarketPair
) -> Tuple[Quote, Quote, bool]:
    """Fetch top-of-book for Polymarket YES and NO outcomes.

    The trailing bool is ``ok`` — True only if both book fetches returned a
    payload. A False here usually means the token id 404'd (stale/wrong
    market) and the caller should re-discover rather than print blanks.
    """
    yes_book, no_book = await asyncio.gather(
        _fetch_json(client, f"{POLY_BOOK_URL}?token_id={pair.poly_yes_token}"),
        _fetch_json(client, f"{POLY_BOOK_URL}?token_id={pair.poly_no_token}"),
    )
    ok = isinstance(yes_book, dict) and isinstance(no_book, dict)
    return _book_top(yes_book), _book_top(no_book), ok


def _kalshi_best_bid(levels: Any) -> Optional[float]:
    """Highest resting bid price from a Kalshi book side.

    Levels look like [["0.0010","11575.00"], ... ["0.7400","899.51"]] (full-
    precision dollar strings) or [[price_cents, size], ...] (classic format).
    Either way the best bid is the maximum price across the levels.
    """
    if not isinstance(levels, list) or not levels:
        return None
    best = None
    for lvl in levels:
        if not isinstance(lvl, (list, tuple)) or not lvl:
            continue
        p = _norm_price(lvl[0])
        if p is not None and (best is None or p > best):
            best = p
    return best


async def poll_kalshi(
    client: httpx.AsyncClient, pair: MarketPair
) -> Tuple[Quote, Quote, bool]:
    """Fetch the Kalshi orderbook and derive YES / NO top-of-book quotes.

    Kalshi resting orders are bids on each side: yes_dollars are bids to BUY
    YES, no_dollars are bids to BUY NO. So:
        yes_bid = max(yes side)      no_bid = max(no side)
        yes_ask = 1 - no_bid         no_ask = 1 - yes_bid
    The trailing bool is ``ok`` — True if the orderbook was retrieved.
    """
    payload = await _fetch_json(
        client, f"{KALSHI_MARKET_URL}/{pair.kalshi_ticker}/orderbook"
    )
    yes_q, no_q = Quote(), Quote()
    if not isinstance(payload, dict):
        return yes_q, no_q, False

    # Support both the full-precision ("orderbook_fp" / *_dollars) and the
    # classic ("orderbook" / yes,no) shapes.
    ob = payload.get("orderbook_fp") or payload.get("orderbook") or {}
    if not isinstance(ob, dict):
        return yes_q, no_q, False

    yes_levels = ob.get("yes_dollars") or ob.get("yes")
    no_levels = ob.get("no_dollars") or ob.get("no")

    yes_q.best_bid = _kalshi_best_bid(yes_levels)
    no_q.best_bid = _kalshi_best_bid(no_levels)

    # Asks are the complement of the opposite side's best bid.
    if no_q.best_bid is not None:
        yes_q.best_ask = round(1.0 - no_q.best_bid, 4)
    if yes_q.best_bid is not None:
        no_q.best_ask = round(1.0 - yes_q.best_bid, 4)
    return yes_q, no_q, True


# ---------------------------------------------------------------------------
# Math engine + mock ledger
# ---------------------------------------------------------------------------

class Engine:
    def __init__(self) -> None:
        self.open_trade: Optional[PaperTrade] = None
        self.realized_pnl: float = 0.0
        self.trade_count: int = 0

    # --- entry --------------------------------------------------------------
    def evaluate(
        self,
        pair: MarketPair,
        poly_yes: Quote,
        poly_no: Quote,
        kalshi_yes: Quote,
        kalshi_no: Quote,
        threshold: Optional[float] = None,
    ) -> None:
        # ``threshold`` overrides the raw-cost cutoff for opening. Defaults to
        # ARB_THRESHOLD (the standalone REST sim's behavior). live_core passes a
        # looser cutoff (any inversion) and lets fee_engine be the true gate.
        thr = threshold if threshold is not None else ARB_THRESHOLD
        if self.open_trade is not None:
            return  # one position at a time per interval

        # NOTE — Settlement Index basis risk (for future LIVE expansion):
        # This simulator treats a sub-1.00 combined cost as a locked, risk-free
        # arb. In reality the two venues resolve against DIFFERENT reference
        # indices — Kalshi's BTC markets settle on the CF Benchmarks
        # (CME CF Bitcoin Reference Rate) while Polymarket resolves via a
        # Chainlink price feed. Those indices can diverge by a few dollars at
        # the settlement instant, so a contract pair can BOTH pay "Up" (or both
        # "Down") at the boundary. Before trading real capital, widen
        # ARB_THRESHOLD to absorb this basis (and fees/slippage), or confirm the
        # two indices print the same value at expiry.

        # Combo A: buy UP on Polymarket (poly YES ask) + DOWN on Kalshi (no ask)
        cost_a = _combo_cost(poly_yes.best_ask, kalshi_no.best_ask)
        # Combo B: buy UP on Kalshi (yes ask) + DOWN on Polymarket (no ask)
        cost_b = _combo_cost(kalshi_yes.best_ask, poly_no.best_ask)

        best = None
        if cost_a is not None and cost_a < thr:
            best = ("A", cost_a, "Polymarket", "Kalshi")
        if cost_b is not None and cost_b < thr:
            if best is None or cost_b < best[1]:
                best = ("B", cost_b, "Kalshi", "Polymarket")

        if best is None:
            return

        _combo, cost, venue_up, venue_down = best
        # Per-leg entry asks for fee accounting.
        if _combo == "A":
            up_price, down_price = poly_yes.best_ask, kalshi_no.best_ask
        else:
            up_price, down_price = kalshi_yes.best_ask, poly_no.best_ask
        self.open_position(
            pair, up_price=up_price, down_price=down_price,
            venue_up=venue_up, venue_down=venue_down,
            setup=_combo, raw_threshold=thr,
        )

    def open_position(
        self,
        pair: MarketPair,
        *,
        up_price: float,
        down_price: float,
        venue_up: str,
        venue_down: str,
        setup: str = "",
        raw_threshold: Optional[float] = None,
    ) -> None:
        """Open a simulated position for a SPECIFIC chosen construction.

        Separated from ``evaluate`` so callers (e.g. live_core's net-margin
        router) can route to a particular setup rather than the raw-cheapest
        one. Entry taker fees on both legs are computed here and persisted on
        the trade so settlement / early-close PnL is fully net-of-fee.
        """
        if self.open_trade is not None:
            return
        entry_cost = up_price + down_price          # raw combined cost per $1
        contracts = PAPER_STAKE_USD / entry_cost
        edge = (1.0 - entry_cost) * contracts        # pre-fee locked profit

        # Entry taker fees: UP leg bought at its ask on venue_up, DOWN on venue_down.
        up_fee = _venue_fee(venue_up, up_price, contracts)
        down_fee = _venue_fee(venue_down, down_price, contracts)
        total_entry_fees = up_fee + down_fee
        # Net-of-entry-fee profit if held to settlement (== fee_engine net margin).
        net_margin = edge - total_entry_fees

        self.open_trade = PaperTrade(
            leg_name=f"UP@{venue_up} + DOWN@{venue_down}",
            venue_up=venue_up,
            venue_down=venue_down,
            entry_cost=entry_cost,
            contracts=contracts,
            ticker=pair.kalshi_ticker,
            net_margin=net_margin,
            setup=setup,
            up_price=up_price,
            down_price=down_price,
            total_entry_fees=total_entry_fees,
        )
        self.trade_count += 1

        print("\n" + "═" * 64)
        print("🚨 ARB SPREAD FOUND!")
        print(f"   Detected (µs): {_ts_us()}")
        print(f"   Legs        : {self.open_trade.leg_name}"
              + (f"  (setup {setup})" if setup else ""))
        gate = (f"raw < {raw_threshold:0.3f}" if raw_threshold is not None
                else "raw inversion")
        print(f"   Combined cost: ${entry_cost:0.3f} per $1 payout  ({gate})")
        print(f"   Paper stake  : ${PAPER_STAKE_USD:0.2f}  →  {contracts:0.1f} contracts")
        print(f"   Locked edge  : ${edge:0.2f}  (pre-fee, at settlement)")
        print(f"   Entry fees   : ${total_entry_fees:0.2f}  "
              f"(up ${up_fee:0.2f} + dn ${down_fee:0.2f})")
        print(f"   Net (a/fees) : ${net_margin:+0.2f}  expected payout "
              f"of chosen path")
        print(f"   Opened       : {_ts()}  on {pair.kalshi_ticker}")
        print("═" * 64 + "\n")

    # --- early close --------------------------------------------------------
    def check_early_close(
        self,
        poly_yes: Quote,
        poly_no: Quote,
        kalshi_yes: Quote,
        kalshi_no: Quote,
    ) -> None:
        t = self.open_trade
        if t is None or t.closed:
            return

        # Unwind value = what we could SELL both legs for right now (best bids).
        if t.venue_up == "Polymarket":
            up_bid, down_bid = poly_yes.best_bid, kalshi_no.best_bid
        else:
            up_bid, down_bid = kalshi_yes.best_bid, poly_no.best_bid

        unwind = _combo_cost(up_bid, down_bid)
        if unwind is None:
            return

        # Closing early means crossing the spread with TAKER (market) sells, so
        # exit fees apply on both legs at their exit bids.
        up_exit_fee = _venue_fee(t.venue_up, up_bid, t.contracts)
        down_exit_fee = _venue_fee(t.venue_down, down_bid, t.contracts)
        exit_fees = up_exit_fee + down_exit_fee

        raw_exit_revenue = unwind * t.contracts
        raw_entry_cost = t.entry_cost * t.contracts
        # Net PnL = exit revenue − entry cost − entry fees − exit fees.
        pnl = (raw_exit_revenue - raw_entry_cost
               - t.total_entry_fees - exit_fees)
        if pnl >= EARLY_CLOSE_MIN_PROFIT:
            t.closed = True
            t.close_pnl = pnl
            t.close_reason = "early unwind"
            self.realized_pnl += pnl
            print("\n" + "─" * 64)
            print("💰 EARLY CLOSE-OUT (order book shifted in our favor)")
            print(f"   {t.leg_name}")
            print(f"   Entry ${t.entry_cost:0.3f} → unwind ${unwind:0.3f}  "
                  f"× {t.contracts:0.1f}")
            print(f"   Entry fees ${t.total_entry_fees:0.2f}  +  "
                  f"exit fees ${exit_fees:0.2f} "
                  f"(up ${up_exit_fee:0.2f} + dn ${down_exit_fee:0.2f})")
            print(f"   Realized PnL (net of all fees): ${pnl:+0.2f}")
            print("─" * 64 + "\n")
            self.open_trade = None

    # --- settlement ---------------------------------------------------------
    def settle(self, pair: MarketPair) -> None:
        t = self.open_trade
        print("\n" + "█" * 64)
        print(f"⏰ INTERVAL SETTLED — {pair.kalshi_ticker}  ({_ts()})")
        if t is None:
            print("   No open paper position this interval.")
        else:
            # A held arb pays out exactly $1 per pair (one leg wins $1, the
            # other $0). Winning-leg payout = contracts * $1.00.
            #   PnL = winning payout − raw entry cost − entry fees
            winning_payout = t.contracts * 1.00
            raw_entry_cost = t.entry_cost * t.contracts
            pnl = winning_payout - raw_entry_cost - t.total_entry_fees
            t.closed = True
            t.close_pnl = pnl
            t.close_reason = "settled"
            self.realized_pnl += pnl
            print(f"   Position : {t.leg_name}")
            print(f"   Entry    : ${t.entry_cost:0.3f}  ×  {t.contracts:0.1f} contracts "
                  f"(raw ${raw_entry_cost:0.2f})")
            print(f"   Payout   : ${winning_payout:0.2f}  −  entry fees "
                  f"${t.total_entry_fees:0.2f}")
            print(f"   Settled  (net of fees): ${pnl:+0.2f}")
            self.open_trade = None
        print(f"   Session realized PnL (net of all fees): "
              f"${self.realized_pnl:+0.2f} over {self.trade_count} trade(s)")
        print("█" * 64 + "\n")


def _combo_cost(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round(a + b, 4)


# ---------------------------------------------------------------------------
# Terminal matrix
# ---------------------------------------------------------------------------

def print_matrix(
    pair: MarketPair,
    poly_yes: Quote,
    poly_no: Quote,
    kalshi_yes: Quote,
    kalshi_no: Quote,
    engine: Engine,
) -> None:
    ttl = pair.seconds_to_expiry()
    ttl_str = f"{int(ttl)}s" if ttl is not None else "?"
    cost_a = _combo_cost(poly_yes.best_ask, kalshi_no.best_ask)
    cost_b = _combo_cost(kalshi_yes.best_ask, poly_no.best_ask)

    print(f"\n┌─ [{_ts()}] {pair.kalshi_ticker} ── expires {pair.expiry_str()} "
          f"(T-{ttl_str}) ─")
    print(f"│ {'OUTCOME':<8}{'POLY bid':>10}{'POLY ask':>10}"
          f"{'KAL bid':>10}{'KAL ask':>10}")
    print(f"│ {'UP/YES':<8}{_fmt(poly_yes.best_bid):>10}{_fmt(poly_yes.best_ask):>10}"
          f"{_fmt(kalshi_yes.best_bid):>10}{_fmt(kalshi_yes.best_ask):>10}")
    print(f"│ {'DOWN/NO':<8}{_fmt(poly_no.best_bid):>10}{_fmt(poly_no.best_ask):>10}"
          f"{_fmt(kalshi_no.best_bid):>10}{_fmt(kalshi_no.best_ask):>10}")
    print(f"│ combo  UP@Poly+DOWN@Kal = {_fmt(cost_a)}   "
          f"UP@Kal+DOWN@Poly = {_fmt(cost_b)}   (edge < {ARB_THRESHOLD})")
    pos = engine.open_trade
    if pos:
        print(f"│ 📌 OPEN: {pos.leg_name}  entry ${pos.entry_cost:0.3f} "
              f"× {pos.contracts:0.1f}")
    print(f"└ session PnL (net of fees) ${engine.realized_pnl:+0.2f}  "
          f"({engine.trade_count} trade(s))")


# ---------------------------------------------------------------------------
# Main loop with auto-rollover
# ---------------------------------------------------------------------------

async def run() -> None:
    print("=" * 64)
    print(" FAST CRYPTO ARB — Polymarket × Kalshi BTC 15m  (SIMULATION ONLY)")
    print(" Read-only · public endpoints · no execution · no credentials")
    print("=" * 64)

    engine = Engine()
    headers = {"User-Agent": "fast-crypto-arb-sim/1.0 (read-only)"}

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        pair: Optional[MarketPair] = None
        book_fail_streak = 0
        MAX_BOOK_FAILS = 3  # consecutive failed ticks before forced re-discovery
        wait_ticks = 0
        REDISCOVER_EVERY_TICKS = 8  # ~16s: refresh discovery while out of sync

        while True:
            try:
                # ---- (re)discovery if we have no locked pair ----
                if pair is None:
                    pair = await discover(client)
                    if pair is None:
                        await asyncio.sleep(POLL_INTERVAL_SEC)
                        continue

                # ---- rollover check ----
                ttl = pair.seconds_to_expiry()
                if ttl is not None and ttl <= 0:
                    engine.settle(pair)
                    print(f"[{_ts()}] 🔄 Interval expired — purging tokens, "
                          f"re-discovering...")
                    pair = None  # purge stale tokens; force fresh discovery
                    continue

                # ---- expiry-alignment guard ----
                # A cross-venue arb is only risk-free when both legs settle on
                # the SAME instant. If the venues are an interval apart (e.g.
                # Polymarket lists its 15m window a day ahead of Kalshi's open
                # one), skip entirely — don't poll books or open paper trades.
                if not pair.aligned():
                    print(f"[{_ts()}] ⏳ Windows out of sync "
                          f"(Polymarket: {pair.poly_expiry_str()} vs "
                          f"Kalshi: {pair.kalshi_expiry_str()}) — "
                          f"waiting for rollover.")
                    # Keep the locked pair so we emit ONE clean line per tick
                    # rather than re-printing the discovery block every 2s.
                    # Refresh discovery on a throttle to catch a venue rolling
                    # into alignment; the normal ttl<=0 rollover also re-locks.
                    wait_ticks += 1
                    if wait_ticks >= REDISCOVER_EVERY_TICKS:
                        wait_ticks = 0
                        pair = None
                    await asyncio.sleep(POLL_INTERVAL_SEC)
                    continue
                wait_ticks = 0

                # ---- order-book polling ----
                poly_res, kalshi_res = await asyncio.gather(
                    poll_polymarket(client, pair),
                    poll_kalshi(client, pair),
                )
                poly_yes, poly_no, poly_ok = poly_res
                kalshi_yes, kalshi_no, kalshi_ok = kalshi_res

                # ---- a book fetch failed (e.g. 404 on a stale token) ----
                # Log ONE clean line, skip the matrix, and re-discover if it
                # keeps failing rather than spamming empty tables.
                if not poly_ok or not kalshi_ok:
                    bad = []
                    if not poly_ok:
                        bad.append("Polymarket")
                    if not kalshi_ok:
                        bad.append("Kalshi")
                    book_fail_streak += 1
                    print(f"[{_ts()}] ⚠️  Book fetch failed ({', '.join(bad)}) "
                          f"— skipping tick {book_fail_streak}/{MAX_BOOK_FAILS}.")
                    if book_fail_streak >= MAX_BOOK_FAILS:
                        print(f"[{_ts()}] 🔄 Persistent failures — re-discovering "
                              f"the active interval.")
                        pair = None
                        book_fail_streak = 0
                    await asyncio.sleep(POLL_INTERVAL_SEC)
                    continue

                book_fail_streak = 0  # healthy tick resets the counter

                # ---- gracefully handle fully-empty (but valid) books ----
                if not any(
                    q.is_valid()
                    for q in (poly_yes, poly_no, kalshi_yes, kalshi_no)
                ):
                    print(f"[{_ts()}] ⚠️  Books returned but empty this tick — "
                          f"skipping.")
                    await asyncio.sleep(POLL_INTERVAL_SEC)
                    continue

                engine.evaluate(pair, poly_yes, poly_no, kalshi_yes, kalshi_no)
                engine.check_early_close(poly_yes, poly_no, kalshi_yes, kalshi_no)
                print_matrix(
                    pair, poly_yes, poly_no, kalshi_yes, kalshi_no, engine
                )

            except asyncio.CancelledError:
                raise
            except Exception as exc:  # never crash the loop
                print(f"[{_ts()}] ⚠️  Loop error ({type(exc).__name__}): {exc}")
                # If something is badly wrong with the pair, force re-discovery.
                pair = None if pair is None else pair

            await asyncio.sleep(POLL_INTERVAL_SEC)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n👋 Stopped by user. (Simulation only — no positions were real.)")


if __name__ == "__main__":
    main()
