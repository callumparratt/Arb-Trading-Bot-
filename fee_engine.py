#!/usr/bin/env python3
"""
fee_engine.py
=============

Real-world fee-drag math — the safety gate that decides whether a *raw* price
edge is still profitable after both exchanges take their taker fees.

Everything here is pure arithmetic on top-of-book asks. No network, no orders.

Conventions
-----------
  * Prices are probabilities in [0, 1]. Inputs may arrive as normalized floats
    (0.00–1.00) OR as integer/float cents (1–100); ``_normalize_price`` coerces
    both to floats in [0, 1].
  * A "contract"/"share" pays $1.00 if its side wins, $0.00 otherwise.
  * To lock a guaranteed $1.00 payout you hold ONE UP contract + ONE DOWN
    contract (exactly one side settles to $1). Cost per guaranteed-$1 unit =
    up_ask + down_ask + fees.
"""

import math
from typing import Dict, Optional, Tuple

# --- subtle terminal colors (ANSI) ------------------------------------------
GRAY = "\033[90m"
PURPLE = "\033[35m"
RESET = "\033[0m"

# --- fee constants ----------------------------------------------------------
POLYMARKET_TAKER_RATE = 0.015      # 1.5% flat overhead on executed $ volume
KALSHI_FEE_RATE = 0.07             # 7% coefficient in Kalshi's fee formula
KALSHI_PER_CONTRACT_CAP = 0.07     # hard ceiling: $0.07 per contract


def _normalize_price(price) -> Optional[float]:
    """Coerce a price (cents or 0..1 float) to a float in [0, 1]; None if bad."""
    if price is None:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p < 0:
        return None
    if p > 1.0:          # looks like cents (e.g. 47 -> 0.47)
        p = p / 100.0
    if p > 1.0:          # still out of range -> reject
        return None
    return p


# ---------------------------------------------------------------------------
# 1) Polymarket fee
# ---------------------------------------------------------------------------

def calc_polymarket_fee(price, size, *, fee_rate: float = POLYMARKET_TAKER_RATE) -> float:
    """Approximate Polymarket taker fee as a flat % of executed dollar volume.

        fee = price * size * 1.5%

    ``price``  outcome price (cents or 0..1).
    ``size``   number of contracts/shares executed.
    """
    p = _normalize_price(price)
    if p is None or size is None or size <= 0:
        return 0.0
    return p * float(size) * fee_rate


# ---------------------------------------------------------------------------
# 2) Kalshi fee
# ---------------------------------------------------------------------------

def calc_kalshi_fee(
    price,
    contracts,
    *,
    fee_rate: float = KALSHI_FEE_RATE,
    per_contract_cap: float = KALSHI_PER_CONTRACT_CAP,
) -> float:
    """Kalshi dynamic taker fee.

        fee = ceil_to_cent( 0.07 * contracts * price * (1 - price) )

    Notes
    -----
    * The ``ceil`` in Kalshi's published formula rounds UP to the next CENT
      (not the next dollar) — implemented as ``ceil(value * 100) / 100``. A
      literal whole-unit ceil would massively overstate fees, so we round to
      the cent, which matches Kalshi's fee schedule.
    * Fee is then capped at ``per_contract_cap`` ($0.07) PER contract. With this
      formula the per-contract fee peaks at 0.07*0.5*0.5 = $0.0175, so the cap
      is a defensive backstop that effectively never binds — but we honor it.
    """
    p = _normalize_price(price)
    if p is None or contracts is None or contracts <= 0:
        return 0.0
    raw = fee_rate * float(contracts) * p * (1.0 - p)
    # Round UP to the next cent. Subtract a tiny epsilon first so floating-point
    # noise on an exact cent boundary (e.g. 1.75 -> 1.7500000000000002) doesn't
    # spuriously bump the fee a full cent higher.
    fee = math.ceil(raw * 100.0 - 1e-9) / 100.0
    cap = per_contract_cap * float(contracts)        # $0.07 / contract ceiling
    return min(fee, cap)


# ---------------------------------------------------------------------------
# 3) Gatekeeper
# ---------------------------------------------------------------------------

def _ask(quote) -> Optional[float]:
    return getattr(quote, "best_ask", None) if quote is not None else None


def _eval_setup(
    name: str,
    up_venue: str,
    down_venue: str,
    up_ask,
    down_ask,
    up_fee_fn,
    down_fee_fn,
    stake: float,
) -> Optional[Dict]:
    """Compute the fee-adjusted economics of one hedge construction.

    Buys ``contracts`` UP and ``contracts`` DOWN (matched pairs) with ``stake``
    dollars of pre-fee notional, yielding a guaranteed ``contracts`` * $1 payout.
    """
    up = _normalize_price(up_ask)
    down = _normalize_price(down_ask)
    if up is None or down is None or up <= 0 or down <= 0:
        return None

    raw_cost = up + down                  # pre-fee cost per guaranteed $1
    contracts = stake / raw_cost          # matched pairs $stake buys (pre-fee)
    up_fee = up_fee_fn(up, contracts)
    down_fee = down_fee_fn(down, contracts)
    total_fees = up_fee + down_fee

    gross_cost = raw_cost * contracts     # == stake
    total_cost = gross_cost + total_fees
    payout = contracts * 1.00             # exactly one side pays $1 per pair
    net_margin = payout - total_cost

    return {
        "setup": name,
        "up_venue": up_venue,
        "down_venue": down_venue,
        "up_ask": up,
        "down_ask": down,
        "raw_cost_per_unit": raw_cost,
        "contracts": contracts,
        "up_fee": up_fee,
        "down_fee": down_fee,
        "total_fees": total_fees,
        "gross_cost": gross_cost,
        "total_cost": total_cost,
        "payout": payout,
        "net_margin": net_margin,
        "net_margin_pct": (net_margin / stake * 100.0) if stake else 0.0,
        "is_profitable": net_margin > 0.0,
    }


def evaluate_true_arbitrage(
    live_state, target_stake: float = 100.0
) -> Tuple[bool, Dict]:
    """Fee-aware arbitrage gate over both hedge constructions.

    Setup A: Polymarket UP ask + Kalshi DOWN ask
    Setup B: Kalshi UP ask + Polymarket DOWN ask

    Returns ``(is_profitable, margins)`` where ``is_profitable`` is True if
    EITHER setup nets positive after fees, and ``margins`` is a dict with the
    full per-setup breakdown plus ``best`` (highest net margin) and the echoed
    ``is_profitable`` / ``target_stake``.
    """
    poly_up = _ask(getattr(live_state, "poly_up", None))
    poly_down = _ask(getattr(live_state, "poly_down", None))
    kal_up = _ask(getattr(live_state, "kalshi_up", None))
    kal_down = _ask(getattr(live_state, "kalshi_down", None))

    setup_a = _eval_setup(
        "A", "Polymarket", "Kalshi", poly_up, kal_down,
        calc_polymarket_fee, calc_kalshi_fee, target_stake,
    )
    setup_b = _eval_setup(
        "B", "Kalshi", "Polymarket", kal_up, poly_down,
        calc_kalshi_fee, calc_polymarket_fee, target_stake,
    )

    evaluable = [s for s in (setup_a, setup_b) if s is not None]
    is_profitable = any(s["is_profitable"] for s in evaluable)
    best = (max(evaluable, key=lambda s: s["net_margin"])["setup"]
            if evaluable else None)

    margins = {
        "A": setup_a,
        "B": setup_b,
        "best": best,
        "is_profitable": is_profitable,
        "target_stake": target_stake,
    }
    return is_profitable, margins


# ---------------------------------------------------------------------------
# Transparent formatting for the terminal
# ---------------------------------------------------------------------------

def format_breakdown(margins: Dict, *, dim: bool = False) -> str:
    """Multi-line, cent-level breakdown of both setups for the alert/log."""
    lines = []
    for name in ("A", "B"):
        s = margins.get(name)
        if not s:
            lines.append(f"   Setup {name}: n/a (incomplete book)")
            continue
        flag = "✅ PROFIT" if s["is_profitable"] else "🚫 fee-blocked"
        lines.append(
            f"   Setup {name}: {s['up_venue']} UP {s['up_ask']:.3f} + "
            f"{s['down_venue']} DOWN {s['down_ask']:.3f}  "
            f"|  {s['contracts']:.1f} pairs  "
            f"|  fees ${s['total_fees']:.2f} "
            f"(up ${s['up_fee']:.2f} + dn ${s['down_fee']:.2f})  "
            f"|  net ${s['net_margin']:+.2f} "
            f"({s['net_margin_pct']:+.2f}%)  {flag}"
        )
    text = "\n".join(lines)
    return (GRAY + text + RESET) if dim else text


def format_compact(margins: Dict) -> str:
    """One-line summary for the live matrix (shows fee drag + net per setup)."""
    def one(s) -> str:
        if not s:
            return "n/a"
        return f"net ${s['net_margin']:+.2f} (fee ${s['total_fees']:.2f})"
    return f"A {one(margins.get('A'))}   B {one(margins.get('B'))}"


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("fee_engine self-test")
    print("-" * 60)
    # Polymarket: 100 shares @ 0.45 -> 0.45*100*0.015 = $0.675
    print("poly fee  100 @ 0.45 :", calc_polymarket_fee(0.45, 100), "(expect 0.675)")
    # Kalshi: 100 contracts @ 0.50 -> 0.07*100*0.25 = 1.75 -> ceil cent 1.75
    print("kalshi fee 100 @ 0.50:", calc_kalshi_fee(0.50, 100), "(expect 1.75)")
    print("kalshi fee 100 @ 50c :", calc_kalshi_fee(50, 100), "(cents path; expect 1.75)")
    print("kalshi fee 100 @ 0.97:", calc_kalshi_fee(0.97, 100),
          "(0.07*100*0.97*0.03=0.2037 -> ceil cent 0.21)")

    class _Q:
        def __init__(self, a): self.best_ask = a

    class _S:
        poly_up = _Q(0.45); poly_down = _Q(0.56)
        kalshi_up = _Q(0.50); kalshi_down = _Q(0.50)  # A: 0.45+0.50=0.95

    ok, m = evaluate_true_arbitrage(_S(), target_stake=100.0)
    print("-" * 60)
    print("is_profitable:", ok, "| best:", m["best"])
    print(format_breakdown(m))
