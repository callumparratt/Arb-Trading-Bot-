#!/usr/bin/env python3
"""
selftest.py
===========

Offline, deterministic verification of the Binance × Bybit BTCUSDT perpetual
arbitrage engine. NO network — synthetic frames are fed straight into the real
parsing helpers and the real ``_ArbGate`` so every architectural gate is
exercised exactly as it runs live.

Five gates are checked:

    1. Data parsing & edge math   — Binance b/a + Bybit data.b[0][0] extraction
    2. Taker-fee buffer           — narrow gap swallowed vs. wide gap clears
    3. State lock (is_in_flight)  — 50 concurrent frames -> exactly one entry
    4. Monotonic cooldown         — post-exit window blocks, then re-opens
    5. Convergence exit           — spread snaps back -> position closed cleanly

Run:
    python selftest.py

Exits 0 if every check passes, 1 otherwise.
"""

import asyncio
import json
import sys

import live_core as lc

# Force UTF-8 so the colored/box-drawing readout renders on Windows consoles.
try:  # pragma: no cover - platform dependent
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

_PASS = 0
_FAIL = 0


def check(label, got, want, tol=1e-9):
    """Record + print one assertion. Numeric compares use a tolerance; every
    other type (bool, str, None, list, tuple, ...) compares by equality."""
    global _PASS, _FAIL
    if isinstance(want, (int, float)) and not isinstance(want, bool):
        ok = abs(got - want) <= tol
    else:
        ok = got == want
    tag = f"{GREEN}[PASS]{RESET}" if ok else f"{RED}[FAIL]{RESET}"
    print(f"   {tag} {label}: got {got!r}, want {want!r}")
    _PASS += ok
    _FAIL += (not ok)
    return ok


def section(title):
    print(f"\n{CYAN}{BOLD}{title}{RESET}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def fresh_engine():
    """A wired single-symbol engine: state -> gate.on_update, own ledger."""
    state = lc.PerpBookState()
    ledger = lc.Ledger()
    gate = lc._ArbGate(state, ledger, lc.PAPER_STAKE_USD)
    state.on_update = gate.on_update
    return state, ledger, gate


def spec(name):
    """A throwaway SymbolSpec for tests (per-exchange names are unused offline)."""
    low = name.lower()
    return lc.SymbolSpec(name, f"{low}usdt", f"{name}USDT", f"{name}-USDT-SWAP")


def fresh_portfolio(*names):
    """A Portfolio over the given symbols (default one), each fully wired."""
    names = names or ("SOL",)
    return lc.Portfolio([spec(n) for n in names], lc.PAPER_STAKE_USD)


def binance_frame(bid, ask):
    """A realistic Binance @bookTicker JSON string."""
    return json.dumps({"u": 400900217, "s": "BTCUSDT",
                       "b": f"{bid:.2f}", "B": "5.0",
                       "a": f"{ask:.2f}", "A": "3.0"})


def bybit_frame(bid=None, ask=None, kind="snapshot"):
    """A realistic Bybit orderbook.1 JSON string (omit a side to test deltas)."""
    data = {"s": "BTCUSDT", "u": 1, "seq": 1}
    if bid is not None:
        data["b"] = [[f"{bid:.2f}", "2.5"]]
    if ask is not None:
        data["a"] = [[f"{ask:.2f}", "1.0"]]
    return json.dumps({"topic": "orderbook.1.BTCUSDT", "type": kind,
                       "ts": 1700000000000, "data": data})


# ---------------------------------------------------------------------------
# 1) Data parsing & edge math
# ---------------------------------------------------------------------------

async def test_parsing_and_edge_math():
    section("1) DATA PARSING & EDGE MATH")

    # Binance: best bid <- msg['b'], best ask <- msg['a'].
    bn = lc.parse_binance_book(binance_frame(60000.10, 60000.50))
    check("Binance bid from msg['b']", bn[0], 60000.10)
    check("Binance ask from msg['a']", bn[1], 60000.50)

    # Bybit: best bid <- data['b'][0][0], best ask <- data['a'][0][0].
    by = lc.parse_bybit_book(bybit_frame(60100.00, 60100.50))
    check("Bybit bid from data['b'][0][0]", by[0], 60100.00)
    check("Bybit ask from data['a'][0][0]", by[1], 60100.50)

    # Control frames (subscribe ack / pong) are NOT books.
    ack = lc.parse_bybit_book(json.dumps({"success": True, "op": "subscribe"}))
    check("Bybit subscribe-ack ignored (not a book)", ack, None)

    # Side-only delta keeps the prior level on the untouched side.
    delta = lc.parse_bybit_book(bybit_frame(bid=60105.00, kind="delta"),
                                prev_bid=60100.00, prev_ask=60100.50)
    check("Bybit delta updates bid", delta[0], 60105.00)
    check("Bybit delta keeps prior ask", delta[1], 60100.50)

    # Raw price delta (no fees): Bybit bid vs Binance ask.
    raw_delta = by[0] - bn[1]
    check("raw delta = Bybit_bid - Binance_ask", raw_delta, 99.50, tol=1e-6)

    # And the engine's own edge object computes the same legs.
    state, _, _ = fresh_engine()
    state.update("binance", bn[0], bn[1])
    state.update("bybit", by[0], by[1])
    best = lc.evaluate_edges(state)[0]
    check("engine routes Binance->Bybit (buy cheap ask, sell rich bid)",
          best.direction, "Binance→Bybit")
    check("edge buy leg = Binance ask", best.buy_ask, 60000.50)
    check("edge sell leg = Bybit bid", best.sell_bid, 60100.00)


# ---------------------------------------------------------------------------
# 2) Taker-fee buffer
# ---------------------------------------------------------------------------

async def test_fee_buffer():
    section("2) TAKER FEE BUFFER (0.05% x2)")

    # Round-trip fee on a ~60k book is ~0.10% ~= $60. A $20 gap can't clear it.
    # Case A: narrow gap, swallowed by fees -> NO execution.
    state, ledger, gate = fresh_engine()
    state.update("binance", 60000.00, 60000.50)
    state.update("bybit", 60020.00, 60020.50)     # Bybit bid only $19.50 over ask
    check("Case A narrow gap: NO trade opened", ledger.trade_count, 0)
    check("Case A narrow gap: gate NOT in flight", gate.is_in_flight, False)
    check("Case A narrow gap: no open position", ledger.open_trade, None)

    # Case B: wide gap, cleanly clears the combined fee friction -> EXECUTION.
    state, ledger, gate = fresh_engine()
    state.update("binance", 60000.00, 60000.50)
    state.update("bybit", 60300.00, 60300.50)     # Bybit bid ~$300 over ask
    check("Case B wide gap: execution flag raised", gate.is_in_flight, True)
    check("Case B wide gap: position opened", ledger.open_trade is not None, True)
    check("Case B wide gap: net edge per BTC > 0",
          ledger.open_trade.locked_per_unit > 0, True)

    # Boundary sanity: the edge sign is exactly the fee-buffer inequality.
    e = lc._edge("t", "A", "B", buy_ask=60000.50, sell_bid=60020.00)
    lhs = e.buy_ask * (1 + lc.TAKER_FEE)
    rhs = e.sell_bid * (1 - lc.TAKER_FEE)
    check("buffer inequality matches edge sign (narrow -> closed)",
          e.is_open, lhs < rhs)


# ---------------------------------------------------------------------------
# 3) State lock under a frame storm
# ---------------------------------------------------------------------------

async def test_state_lock_storm():
    section("3) STATE LOCK (is_in_flight) — 50-FRAME STORM")

    state, ledger, gate = fresh_engine()
    # Seed Binance so the very first Bybit frame can form a real inversion.
    state.update("binance", 60000.00, 60000.50)

    async def fire():
        # Identical execution-triggering frame, parsed exactly like the live
        # Bybit stream, then folded into the engine.
        book = lc.parse_bybit_book(bybit_frame(60300.00, 60300.50))
        state.update("bybit", book[0], book[1])

    # 50 identical triggers dispatched together.
    await asyncio.gather(*(fire() for _ in range(50)))

    check("lock engaged on first frame", gate.is_in_flight, True)
    check("exactly ONE entry across 50 frames (49 safely ignored)",
          _entries(ledger), 1)
    check("a single position is held", ledger.open_trade is not None, True)


def _entries(ledger):
    """Total entries = closed trades + (1 if a position is currently open)."""
    return ledger.trade_count + (1 if ledger.open_trade is not None else 0)


# ---------------------------------------------------------------------------
# 4) Monotonic cooldown
# ---------------------------------------------------------------------------

async def test_monotonic_cooldown():
    section("4) MONOTONIC COOLDOWN")

    # Shrink the cooldown so the test stays fast but still exercises the REAL
    # monotonic clock (no back-dating, no mocking of time).
    original = lc.TRADE_COOLDOWN_SEC
    lc.TRADE_COOLDOWN_SEC = 0.4
    try:
        state, ledger, gate = fresh_engine()
        state.update("binance", 60000.00, 60000.50)

        # Open a valid execution.
        state.update("bybit", 60300.00, 60300.50)
        check("initial execution opened", _entries(ledger), 1)

        # Converge -> exit, which stamps last_trade_time and arms the cooldown.
        state.update("bybit", 60000.20, 60000.40)
        check("position exited (cooldown armed)", ledger.open_trade, None)
        check("one completed trade", ledger.trade_count, 1)

        # 100ms later, still inside the window: a fat frame must be REJECTED.
        await asyncio.sleep(0.1)
        state.update("bybit", 60300.00, 60300.50)
        check("frame inside cooldown is rejected", _entries(ledger), 1)

        # Let the monotonic clock pass the cooldown limit, then retry.
        await asyncio.sleep(0.4)
        state.update("bybit", 60300.00, 60300.50)
        check("engine re-opens once cooldown elapses", _entries(ledger), 2)
    finally:
        lc.TRADE_COOLDOWN_SEC = original


# ---------------------------------------------------------------------------
# 5) Convergence exit
# ---------------------------------------------------------------------------

async def test_convergence_exit():
    section("5) CONVERGENCE EXIT LOGIC")

    state, ledger, gate = fresh_engine()
    # Mock an open, active arbitrage position.
    state.update("binance", 60000.00, 60000.50)
    state.update("bybit", 60300.00, 60300.50)
    check("position is active before convergence",
          ledger.open_trade is not None, True)
    check("lock held while in flight", gate.is_in_flight, True)
    balance_before = ledger.realized_pnl

    # Spread snaps back to equilibrium (Bybit bid no longer clears Binance ask).
    state.update("bybit", 60000.20, 60000.40)

    check("exit routine cleared the active position", ledger.open_trade, None)
    check("lock released after exit", gate.is_in_flight, False)
    check("trade booked", ledger.trade_count, 1)
    check("simulated balance updated (PnL credited)",
          ledger.realized_pnl > balance_before, True)


# ---------------------------------------------------------------------------
# 6) Reconciliation guard, unhedged-leg watchdog & panic close
# ---------------------------------------------------------------------------

async def test_reconciliation_guard():
    section("6) RECONCILIATION GUARD / UNHEDGED LEG / PANIC CLOSE")

    # --- Healthy reconciliation: simulated brokers mirror the portfolio -> OK. ---
    pf = fresh_portfolio()
    eng = pf.engines["SOL"]
    brokers = lc.build_brokers(lc.ExchangeCredentials(), pf, live=False)
    check("one broker per execution venue (Binance/Bybit/OKX)",
          [b.name for b in brokers], ["Binance", "Bybit", "OKX"])
    guard = lc.ReconciliationGuard(pf, brokers, live=False)
    await guard._reconcile_equity()
    check("healthy book reconciles without panic", guard.panicked, False)
    check("engine not halted when balances agree", eng.gate.halted, False)

    # --- Injected equity discrepancy beyond tolerance -> PANIC. ---
    pf = fresh_portfolio()
    eng = pf.engines["SOL"]
    # Brokers report nothing — a $100 gap, way past the $5 tolerance.
    skewed = [lc.SimulatedBroker("Binance", lambda: 0.0),
              lc.SimulatedBroker("Bybit", lambda: 0.0)]
    guard = lc.ReconciliationGuard(pf, skewed, live=False)
    await guard._reconcile_equity()
    check("equity discrepancy triggers panic", guard.panicked, True)
    check("panic latches the kill-switch (halted)", eng.gate.halted, True)

    # After a halt, new arb frames must be refused.
    eng.state.update("binance", 60000.00, 60000.50)
    eng.state.update("bybit", 60300.00, 60300.50)
    check("halted engine refuses new entries", eng.ledger.open_trade, None)

    # --- Unhedged leg exposed past the grace window -> PANIC + paper close. ---
    pf = fresh_portfolio()
    eng = pf.engines["SOL"]
    brokers = lc.build_brokers(lc.ExchangeCredentials(), pf, live=False)
    guard = lc.ReconciliationGuard(pf, brokers, live=False, unhedged_grace=0.05)
    # Open a position, then mark ONE leg as failed/unhedged in the past.
    eng.state.update("binance", 60000.00, 60000.50)
    eng.state.update("bybit", 60300.00, 60300.50)
    pos = eng.ledger.open_trade
    check("position open before unhedged fault", pos is not None, True)
    pos.hedged = False
    pos.unhedged_since = lc.time.monotonic() - 1.0   # 1s ago, past 0.05s grace
    await guard._check_unhedged_leg()
    check("stale unhedged leg triggers panic", guard.panicked, True)
    check("paper position force-closed on panic", eng.ledger.open_trade, None)
    check("engine halted after unhedged panic", eng.gate.halted, True)

    # --- A FRESH unhedged leg still inside grace must NOT panic. ---
    pf = fresh_portfolio()
    eng = pf.engines["SOL"]
    guard = lc.ReconciliationGuard(
        pf, lc.build_brokers(lc.ExchangeCredentials(), pf, live=False),
        live=False, unhedged_grace=5.0)
    eng.state.update("binance", 60000.00, 60000.50)
    eng.state.update("bybit", 60300.00, 60300.50)
    eng.ledger.open_trade.hedged = False
    eng.ledger.open_trade.unhedged_since = lc.time.monotonic()   # just now
    await guard._check_unhedged_leg()
    check("fresh unhedged leg within grace does NOT panic", guard.panicked, False)

    # --- LIVE brokers are unimplemented stubs: panic still fires the alert
    #     and the broker calls raise (surfaced, not silently 'closed'). ---
    pf = fresh_portfolio()
    live_brokers = lc.build_brokers(
        lc.ExchangeCredentials(binance_key="k", binance_secret="s",
                               bybit_key="k", bybit_secret="s",
                               okx_key="k", okx_secret="s", okx_passphrase="p"),
        pf, live=True)
    check("live mode builds an OKX broker too", live_brokers[2].name, "OKX")
    raised = False
    try:
        await live_brokers[2].fetch_equity()   # OKX stub must also raise
    except NotImplementedError:
        raised = True
    check("OKX LiveBroker.fetch_equity is an unimplemented stub", raised, True)
    guard = lc.ReconciliationGuard(pf, live_brokers, live=True)
    await guard._panic("test")        # must not raise despite stub brokers
    check("panic completes even when live close is unavailable",
          guard.panicked, True)


# ---------------------------------------------------------------------------
# 7) OKX as a first-class EXECUTION venue (not just a display row)
# ---------------------------------------------------------------------------

async def test_okx_execution_venue():
    section("7) OKX FIRST-CLASS EXECUTION VENUE")

    # Three live books with OKX carrying the single best edge. The gate must
    # ROUTE to an OKX pair, proving evaluate_edges feeds OKX into execution.
    state, ledger, gate = fresh_engine()
    state.update("binance", 60000.00, 60000.50)   # cheapest ask
    state.update("bybit",   60001.00, 60001.50)   # no Binance↔Bybit edge
    state.update("okx",     60400.00, 60400.50)   # OKX bid far above others
    check("six directional edges across three venues",
          len(lc.evaluate_edges(state)), 6)
    best = lc.evaluate_edges(state)[0]
    check("best edge involves OKX", "OKX" in (best.buy_venue, best.sell_venue), True)
    pos = ledger.open_trade
    check("gate opened a position", pos is not None, True)
    check("executed trade is an OKX pair",
          "OKX" in (pos.buy_venue, pos.sell_venue), True)
    check("specifically Binance→OKX (buy cheapest, sell richest bid)",
          pos.direction, "Binance→OKX")

    # All three pairs are exposed to the basis readout (display parity).
    state2 = lc.PerpBookState()
    state2.update("binance", 60000.00, 60000.50)
    state2.update("bybit",   60100.00, 60100.50)
    state2.update("okx",     60200.00, 60200.50)
    seen = []
    for a, b in lc.VENUE_PAIRS:
        e = lc.best_edge_between(state2, a, b)
        seen.append(e is not None)
    check("all three venue pairs produce a basis line", all(seen), True)
    check("VENUE_PAIRS covers the three combinations", list(lc.VENUE_PAIRS),
          [("Binance", "Bybit"), ("Binance", "OKX"), ("Bybit", "OKX")])


# ---------------------------------------------------------------------------
# 8) Multi-symbol parallel tracking: per-symbol locks & cooldowns
# ---------------------------------------------------------------------------

async def test_multisymbol_independence():
    section("8) PER-SYMBOL LOCKS & COOLDOWNS (multi-asset parallelism)")

    pf = fresh_portfolio("PEPE", "SOL", "WIF")
    pe, so, wf = pf.engines["PEPE"], pf.engines["SOL"], pf.engines["WIF"]

    # Books are nested per-symbol: writing PEPE must not touch SOL's prices.
    pe.state.update("binance", 0.00001000, 0.00001001)
    pe.state.update("bybit",   0.00001080, 0.00001081)   # PEPE arb -> locks PEPE
    check("PEPE locked after its own spike", pe.gate.is_in_flight, True)
    check("PEPE opened exactly one trade", pe.ledger.trade_count
          + (1 if pe.ledger.open_trade else 0), 1)
    check("SOL book untouched by PEPE update", so.state.binance.bid, 0.0)
    check("SOL NOT locked by PEPE's trade", so.gate.is_in_flight, False)

    # An independent edge on SOL must still execute despite PEPE being locked.
    so.state.update("binance", 100.00, 100.05)
    so.state.update("bybit",   101.20, 101.25)           # SOL arb -> locks SOL
    check("SOL trades independently while PEPE is locked",
          so.gate.is_in_flight, True)
    check("SOL opened its own position", so.ledger.open_trade is not None, True)

    # WIF saw no edge: it stays idle and unlocked.
    check("WIF remains idle (no spike)", wf.gate.is_in_flight, False)

    # Portfolio aggregates across symbols.
    check("two concurrent open positions across the portfolio",
          len(pf.open_engines()), 2)
    check("portfolio trade_count aggregates per-symbol ledgers",
          pf.trade_count + len(pf.open_engines()), 2)

    # evaluate_all_symbols covers every tracked asset.
    alls = lc.evaluate_all_symbols(pf)
    check("evaluate_all_symbols returns one entry per symbol",
          sorted(alls.keys()), ["PEPE", "SOL", "WIF"])

    # Cooldown is per-symbol too: drive PEPE to convergence -> it cools down,
    # but SOL's lock is unaffected.
    pe.state.update("binance", 0.00001000, 0.00001001)
    pe.state.update("bybit",   0.00000999, 0.00001000)   # converged -> PEPE exits
    check("PEPE released + cooling after convergence", pe.gate.is_in_flight, False)
    check("PEPE is in its cooldown window", pe.cooling_down, True)
    check("SOL still locked, unaffected by PEPE's cooldown",
          so.gate.is_in_flight, True)


# ---------------------------------------------------------------------------
# 9) Phantom-spread guard: stale/disconnected venues are excluded from pricing
# ---------------------------------------------------------------------------

def _wire_book(st, venue, bid, ask, *, connected=True, stale=False):
    """Set a venue book directly (bypassing update()) so we control its exact
    health flags without firing the gate."""
    b = st._book(venue)
    b.bid, b.ask, b.connected, b.stale = bid, ask, connected, stale


async def test_phantom_spread_guard():
    section("9) PHANTOM-SPREAD GUARD (stale / disconnected venue exclusion)")

    # A crossing book between two venues = a real edge ONLY if both are healthy.
    st = lc.PerpBookState()
    _wire_book(st, "binance", 60000.00, 60000.50)
    _wire_book(st, "bybit",   60300.00, 60300.50)   # bybit bid >> binance ask
    check("healthy pair yields an edge", len(lc.evaluate_edges(st)) > 0, True)

    # Mark bybit STALE: its cached crossing price must NOT manufacture a spread.
    st.bybit.stale = True
    check("stale venue excluded — no phantom edge", lc.evaluate_edges(st), [])
    check("stale venue is not healthy", st.bybit.healthy, False)

    # Mark bybit DOWN (disconnected) instead: same strict exclusion.
    st.bybit.stale = False
    st.bybit.connected = False
    check("disconnected venue excluded — no phantom edge",
          lc.evaluate_edges(st), [])

    # With a THIRD healthy venue, only the healthy pair prices; the dead one
    # never appears in ANY edge.
    _wire_book(st, "okx", 60100.00, 60100.50)        # okx healthy
    edges = lc.evaluate_edges(st)                     # bybit still down
    in_edges = {v for e in edges for v in (e.buy_venue, e.sell_venue)}
    check("only healthy venues participate", in_edges, {"Binance", "OKX"})
    check("dead Bybit never appears in any edge", "Bybit" in in_edges, False)

    # --- Execution path: the gate must not OPEN against a stale venue. ---
    state, ledger, gate = fresh_engine()
    _wire_book(state, "binance", 60000.00, 60000.50)
    _wire_book(state, "bybit",   60300.00, 60300.50, stale=True)   # STALE leg
    gate.on_update("binance")
    check("gate does NOT open against a stale venue", ledger.open_trade, None)
    check("gate not flagged in-flight on phantom spread",
          gate.is_in_flight, False)

    # Venue recovers (fresh, synced) -> the real edge now executes.
    state.bybit.stale = False
    gate.on_update("bybit")
    check("gate opens once the venue is healthy",
          ledger.open_trade is not None, True)
    direction = ledger.open_trade.direction          # Binance→Bybit

    # --- Exit path: a leg going stale must HOLD, not phantom-close. ---
    state.bybit.stale = True                          # sell leg goes dark
    gate.on_update("binance")
    check("position HELD while a leg is stale (no phantom close)",
          ledger.open_trade is not None, True)
    check("still in-flight (not phantom-closed)", gate.is_in_flight, True)

    # Recovery + genuine convergence -> normal trustworthy exit.
    _wire_book(state, "bybit", 60000.20, 60000.40)   # healthy + converged
    gate.on_update("bybit")
    check("normal convergence exit resumes once healthy",
          ledger.open_trade, None)
    check("trade booked on the trustworthy exit", ledger.trade_count, 1)


# ---------------------------------------------------------------------------
# 10) Trade journal: a closed round-trip is persisted to CSV with the right fields
# ---------------------------------------------------------------------------

async def test_trade_journal():
    section("10) TRADE JOURNAL (persistent CSV logging of executed trades)")
    import csv as _csv
    import os as _os
    import tempfile

    path = _os.path.join(tempfile.gettempdir(), "selftest_trades.csv")
    if _os.path.exists(path):
        _os.remove(path)

    logger = lc.TradeLogger(path)
    check("journal file created with header", _os.path.exists(path), True)

    # A logger-wired single-symbol engine.
    state = lc.PerpBookState(spec("SOL"))
    ledger = lc.Ledger()
    gate = lc._ArbGate(state, ledger, lc.PAPER_STAKE_USD, logger)
    state.on_update = gate.on_update

    # Open a real edge (cheap Binance ask, rich Bybit bid) then converge to exit.
    _wire_book(state, "binance", 150.00, 150.05)
    _wire_book(state, "bybit",   150.90, 150.95)
    gate.on_update("binance")
    check("position opened", ledger.open_trade is not None, True)

    _wire_book(state, "bybit", 150.02, 150.06)   # converged → trustworthy exit
    gate.on_update("bybit")
    check("round-trip closed", ledger.trade_count, 1)

    logger.close()   # drain the writer thread + flush before reading back

    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(_csv.reader(fh))
    check("file holds header + exactly one trade row", len(rows), 2)
    check("header matches the journal schema", rows[0], lc.TradeLogger.HEADER)

    rec = dict(zip(rows[0], rows[1]))
    check("asset persisted", rec["asset"], "SOL")
    check("buy leg venue persisted", rec["venue_buy"], "Binance")
    check("sell leg venue persisted", rec["venue_sell"], "Bybit")
    check("exec buy price persisted", float(rec["exec_price_buy"]), 150.05, tol=1e-6)
    check("exec sell price persisted", float(rec["exec_price_sell"]), 150.90, tol=1e-6)
    check("expected spread bps is positive", float(rec["expected_spread_bps"]) > 0, True)
    check("realized pnl matches ledger",
          float(rec["realized_pnl"]), ledger.realized_pnl, tol=1e-5)
    check("logger.count tracks persisted rows", logger.count, 1)

    # A fresh logger over the SAME path must APPEND (one header only, not clobber).
    logger2 = lc.TradeLogger(path)
    logger2.close()
    with open(path, newline="", encoding="utf-8") as fh:
        rows2 = list(_csv.reader(fh))
    check("re-open appends (no second header, no data loss)", len(rows2), 2)

    _os.remove(path)


# ---------------------------------------------------------------------------
# 11) Dynamic config loader: externalized thresholds parse, validate & apply
# ---------------------------------------------------------------------------

async def test_dynamic_config():
    section("11) DYNAMIC CONFIG LOADER (externalized thresholds)")
    import json as _json
    import os as _os
    import tempfile

    tmp = tempfile.gettempdir()

    def _write(name, obj):
        p = _os.path.join(tmp, name)
        with open(p, "w", encoding="utf-8") as fh:
            if isinstance(obj, str):
                fh.write(obj)
            else:
                _json.dump(obj, fh)
        return p

    # --- A full, valid config round-trips into a BotConfig. ---
    p = _write("selftest_cfg_full.json",
               {"dry_run": False, "order_size_usd": 250.0,
                "min_spread_bps": 3.5, "target_markets": ["SOL", "WIF"]})
    cfg = lc.load_config(p)
    check("dry_run parsed", cfg.dry_run, False)
    check("order_size_usd parsed", cfg.order_size_usd, 250.0)
    check("min_spread_bps parsed", cfg.min_spread_bps, 3.5)
    check("target_markets resolved in configured order",
          tuple(cfg.target_markets), ("SOL", "WIF"))
    check("specs resolve to SymbolSpec objects in order",
          [s.name for s in cfg.specs], ["SOL", "WIF"])
    _os.remove(p)

    # --- Missing file → safe built-in defaults (dry_run stays ON). ---
    missing = _os.path.join(tmp, "selftest_cfg_missing.json")
    if _os.path.exists(missing):
        _os.remove(missing)
    dflt = lc.load_config(missing)
    check("missing file falls back to defaults (dry_run on)", dflt.dry_run, True)
    check("default order size = PAPER_STAKE_USD",
          dflt.order_size_usd, lc.PAPER_STAKE_USD)
    check("default markets = full catalog",
          sorted(dflt.target_markets), sorted(lc.SYMBOL_CATALOG))

    # --- Malformed JSON → a clear SystemExit (fail fast, never silent). ---
    bad = _write("selftest_cfg_bad.json", "{ not valid json ")
    raised = False
    try:
        lc.load_config(bad)
    except SystemExit:
        raised = True
    check("malformed JSON raises SystemExit", raised, True)
    _os.remove(bad)

    # --- Out-of-range / wrong-typed values are rejected loudly. ---
    negsize = _write("selftest_cfg_neg.json", {"order_size_usd": -5})
    raised = False
    try:
        lc.load_config(negsize)
    except SystemExit:
        raised = True
    check("non-positive order_size_usd rejected", raised, True)
    _os.remove(negsize)

    boolspread = _write("selftest_cfg_boolspread.json", {"min_spread_bps": True})
    raised = False
    try:
        lc.load_config(boolspread)
    except SystemExit:
        raised = True
    check("boolean where a number is expected rejected", raised, True)
    _os.remove(boolspread)

    # --- Unknown markets dropped with a warning; valid ones kept. ---
    mixed = _write("selftest_cfg_mixed.json",
                   {"target_markets": ["SOL", "NOTACOIN", "WIF"]})
    mc = lc.load_config(mixed)
    check("unknown market dropped, valid kept",
          tuple(mc.target_markets), ("SOL", "WIF"))
    check("a warning was recorded for the unknown market",
          any("NOTACOIN" in w for w in mc.warnings), True)
    _os.remove(mixed)

    # --- All-unknown markets fall back to the full catalog. ---
    allbad = _write("selftest_cfg_allbad.json",
                    {"target_markets": ["XXX", "YYY"]})
    ab = lc.load_config(allbad)
    check("all-unknown markets fall back to full catalog",
          sorted(ab.target_markets), sorted(lc.SYMBOL_CATALOG))
    _os.remove(allbad)

    # --- min_spread_bps hurdle is ENFORCED by the gate. A positive net edge
    #     (~5 bps) that clears the fee buffer but sits below a 50 bps hurdle
    #     must be BLOCKED. ---
    st = lc.PerpBookState(spec("SOL"))
    led = lc.Ledger()
    gate = lc._ArbGate(st, led, lc.PAPER_STAKE_USD, None, min_spread_bps=50.0)
    st.on_update = gate.on_update
    _wire_book(st, "binance", 100.00, 100.05)
    _wire_book(st, "bybit",   100.20, 100.25)   # ~5 bps net — clears fees only
    gate.on_update("binance")
    check("net edge below the configured hurdle is blocked",
          led.open_trade, None)
    check("blocked-by-hurdle leaves the gate unlocked", gate.is_in_flight, False)

    # Same book, 0-bps hurdle → opens (proves it was the hurdle, not the fees).
    st2 = lc.PerpBookState(spec("SOL"))
    led2 = lc.Ledger()
    gate2 = lc._ArbGate(st2, led2, lc.PAPER_STAKE_USD, None, min_spread_bps=0.0)
    st2.on_update = gate2.on_update
    _wire_book(st2, "binance", 100.00, 100.05)
    _wire_book(st2, "bybit",   100.20, 100.25)
    gate2.on_update("binance")
    check("same edge opens with a 0-bps hurdle",
          led2.open_trade is not None, True)

    # --- Portfolio threads the hurdle to EVERY gate. ---
    pf = lc.Portfolio([spec("SOL"), spec("WIF")], lc.PAPER_STAKE_USD,
                      min_spread_bps=7.5)
    check("portfolio stores the configured hurdle", pf.min_spread_bps, 7.5)
    check("each per-symbol gate receives the hurdle",
          [e.gate.min_spread_bps for e in pf.engines.values()], [7.5, 7.5])

    # --- dry_run wiring: dry_run=True builds NO live brokers (no order endpoints).
    #     The brokers are pure SimulatedBroker stand-ins. ---
    sim_brokers = lc.build_brokers(lc.ExchangeCredentials(), pf, live=False,
                                   initial_equity=pf.stake)
    check("dry-run uses simulated brokers (no live order endpoints)",
          all(isinstance(b, lc.SimulatedBroker) for b in sim_brokers), True)

    # --- realism block parses; absent block → disabled defaults. ---
    rp = _write("selftest_cfg_realism.json",
                {"realism": {"enabled": True, "slippage_bps": 2.0,
                             "fill_probability": 0.9, "seed": 99}})
    rc = lc.load_config(rp)
    check("realism block parsed (enabled)", rc.realism.enabled, True)
    check("realism slippage parsed", rc.realism.slippage_bps, 2.0)
    check("realism seed parsed", rc.realism.seed, 99)
    _os.remove(rp)
    check("config without a realism block → disabled by default",
          cfg.realism.enabled, False)


# ---------------------------------------------------------------------------
# 12) Execution realism: slippage, latency, partial fills, leg failure, funding
# ---------------------------------------------------------------------------

def _params(**over):
    """A RealismParams with everything off, overridden per-test for isolation."""
    base = dict(enabled=True, slippage_bps=0.0, impact_bps_per_10k=0.0,
                latency_ms=0.0, latency_adverse_bps=0.0, fill_probability=1.0,
                partial_fill_prob=0.0, partial_fill_min_ratio=1.0,
                leg_failure_prob=0.0, funding_rate_8h_bps=0.0, seed=7)
    base.update(over)
    return lc.RealismParams(**base)


async def test_execution_realism():
    section("12) EXECUTION REALISM (slippage / latency / partials / funding)")

    # An ideal Binance→Bybit edge: gross 0.50 on ~100 (≈50 bps), fees ~10 bps.
    edge = lc._edge("Binance→Bybit", "Binance", "Bybit", 100.00, 100.50)

    # --- SLIPPAGE degrades the fill: buy higher, sell lower, net edge shrinks. ---
    m = lc.ExecutionModel(_params(slippage_bps=2.0), seed=7)
    fill = m.simulate_entry(edge, 100.0)
    check("slippage: a fill is still produced", fill is not None, True)
    check("slippage: buy leg fills ABOVE the ask", fill.exec_buy > 100.00, True)
    check("slippage: sell leg fills BELOW the bid", fill.exec_sell < 100.50, True)
    check("slippage: realised edge is worse than ideal",
          fill.per_unit < edge.per_unit, True)
    check("slippage: but still positive (didn't cross to a loss)",
          fill.per_unit > 0, True)

    # --- Slippage so large the edge EVAPORATES → no fill (never trade a loss). ---
    m = lc.ExecutionModel(_params(slippage_bps=300.0), seed=7)
    check("evaporated edge → simulate_entry returns None",
          m.simulate_entry(edge, 100.0), None)

    # --- FILL PROBABILITY 0 → the quote is always gone → no fill. ---
    m = lc.ExecutionModel(_params(fill_probability=0.0), seed=7)
    check("zero fill probability → no fill", m.simulate_entry(edge, 100.0), None)

    # --- PARTIAL FILL: ratio in [min, 1], and size scales by it. ---
    m = lc.ExecutionModel(_params(partial_fill_prob=1.0,
                                  partial_fill_min_ratio=0.5), seed=7)
    fill = m.simulate_entry(edge, 100.0)
    check("partial: fill ratio within [0.5, 1.0]",
          0.5 <= fill.fill_ratio <= 1.0, True)
    check("partial: contracts scale by the fill ratio",
          fill.contracts, (100.0 / fill.exec_buy) * fill.fill_ratio, tol=1e-9)

    # --- LEG FAILURE: one leg unfilled → unhedged exposure flagged. ---
    m = lc.ExecutionModel(_params(leg_failure_prob=1.0), seed=7)
    fill = m.simulate_entry(edge, 100.0)
    check("leg failure: position marked UNHEDGED", fill.hedged, False)
    check("leg failure: unhedged_since stamped", fill.unhedged_since is not None, True)

    # --- LATENCY adds an adverse drift (worse fill) that scales with latency. ---
    fast = lc.ExecutionModel(_params(latency_ms=50.0,
                                     latency_adverse_bps=5.0), seed=7)
    slow = lc.ExecutionModel(_params(latency_ms=500.0,
                                     latency_adverse_bps=5.0), seed=7)
    f_fast = fast.simulate_entry(edge, 100.0)
    f_slow = slow.simulate_entry(edge, 100.0)
    check("latency: higher latency = worse (higher) buy fill",
          f_slow.exec_buy > f_fast.exec_buy, True)

    # --- REPRODUCIBILITY: same seed → identical fills (byte-for-byte runs). ---
    a = lc.ExecutionModel(_params(slippage_bps=1.0, latency_ms=100.0,
                                  latency_adverse_bps=3.0), seed=42)
    b = lc.ExecutionModel(_params(slippage_bps=1.0, latency_ms=100.0,
                                  latency_adverse_bps=3.0), seed=42)
    check("reproducible: identical seed → identical buy fill",
          a.simulate_entry(edge, 100.0).exec_buy,
          b.simulate_entry(edge, 100.0).exec_buy, tol=0.0)

    # --- FUNDING: accrues over the hold and is netted out of PnL at close. ---
    led = lc.Ledger()
    fund_fill = lc.Fill(exec_buy=100.00, exec_sell=100.50, contracts=1.0,
                        per_unit=edge.per_unit, hedged=True, unhedged_since=None,
                        funding_rate_per_sec=(10.0 * 1e-4) / (8.0 * 3600.0),
                        fill_ratio=1.0, slipped_bps=0.0)
    led.open_position(edge, 100.0, fill=fund_fill)
    pos = led.open_trade
    locked = pos.locked_pnl
    pos.opened_at = lc.time.monotonic() - 3600.0   # pretend it was held 1 hour
    pnl = led.close_position()
    check("funding: a held position pays funding (PnL < locked edge)",
          pnl < locked, True)

    # Idealised close (no funding) returns exactly the locked edge.
    led2 = lc.Ledger()
    led2.open_position(edge, 100.0)                # no fill → funding rate 0
    locked2 = led2.open_trade.locked_pnl
    check("no-funding close returns the locked edge exactly",
          led2.close_position(), locked2, tol=1e-12)

    # --- GATE INTEGRATION: an enabled model degrades the booked entry. ---
    pf = lc.Portfolio([spec("SOL")], lc.PAPER_STAKE_USD,
                      realism=_params(slippage_bps=2.0))
    g = pf.engines["SOL"]
    check("realism enabled → gate carries an execution model",
          g.gate.execution is not None, True)
    _wire_book(g.state, "binance", 100.00, 100.05)
    _wire_book(g.state, "bybit",   101.20, 101.25)   # fat, clears slippage
    g.gate.on_update("binance")
    check("realism: a position still opens on a fat edge",
          g.ledger.open_trade is not None, True)
    check("realism: booked buy price is slipped ABOVE the raw ask",
          g.ledger.open_trade.entry_buy > 100.05, True)

    # --- MISSED FILL counter: an always-miss model opens nothing. ---
    pf = lc.Portfolio([spec("SOL")], lc.PAPER_STAKE_USD,
                      realism=_params(fill_probability=0.0))
    g = pf.engines["SOL"]
    _wire_book(g.state, "binance", 100.00, 100.05)
    _wire_book(g.state, "bybit",   101.20, 101.25)
    g.gate.on_update("binance")
    check("missed fill: no position opened", g.ledger.open_trade, None)
    check("missed fill: gate stays unlocked", g.gate.is_in_flight, False)
    check("missed fill: counter incremented", g.gate.missed_fills, 1)

    # --- DISABLED (default) → no execution model, idealised behaviour intact. ---
    pf = lc.Portfolio([spec("SOL")], lc.PAPER_STAKE_USD)
    check("realism off by default → no execution model on the gate",
          pf.engines["SOL"].gate.execution, None)
    pf2 = lc.Portfolio([spec("SOL")], lc.PAPER_STAKE_USD,
                       realism=lc.RealismParams(enabled=False))
    check("explicit disabled realism → still no execution model",
          pf2.engines["SOL"].gate.execution, None)

    # --- A realism leg-failure flows into the existing unhedged-leg panic. ---
    pf = lc.Portfolio([spec("SOL")], lc.PAPER_STAKE_USD,
                      realism=_params(leg_failure_prob=1.0))
    g = pf.engines["SOL"]
    _wire_book(g.state, "binance", 100.00, 100.05)
    _wire_book(g.state, "bybit",   101.20, 101.25)
    g.gate.on_update("binance")
    check("realism leg-failure opens an UNHEDGED position",
          g.ledger.open_trade is not None and not g.ledger.open_trade.hedged, True)
    guard = lc.ReconciliationGuard(
        pf, lc.build_brokers(lc.ExchangeCredentials(), pf, live=False),
        live=False, unhedged_grace=0.05)
    g.ledger.open_trade.unhedged_since = lc.time.monotonic() - 1.0
    await guard._check_unhedged_leg()
    check("realism unhedged leg trips the reconciliation panic",
          guard.panicked, True)


# ---------------------------------------------------------------------------
# 13) Config editor & per-effect realism toggles (silence/edit without JSON)
# ---------------------------------------------------------------------------

async def test_config_editor():
    section("13) CONFIG EDITOR & PER-EFFECT REALISM TOGGLES")
    import contextlib
    import io
    import os as _os
    import tempfile

    edge = lc._edge("Binance→Bybit", "Binance", "Bybit", 100.00, 100.50)

    # --- Every realism effect toggle defaults ON. ---
    rp = lc.RealismParams()
    check("realism effect toggles default ON",
          [rp.slippage, rp.latency, rp.missed_fills, rp.partial_fills,
           rp.leg_failure, rp.funding], [True] * 6)

    # --- A toggle silences its effect while KEEPING the tuned magnitude. ---
    m = lc.ExecutionModel(_params(slippage_bps=50.0, slippage=False), seed=7)
    check("slippage OFF → fills at the raw ask despite slippage_bps>0",
          m.simulate_entry(edge, 100.0).exec_buy, 100.00, tol=1e-9)

    m = lc.ExecutionModel(_params(funding_rate_8h_bps=100.0, funding=False), seed=7)
    check("funding OFF → zero funding rate despite funding_rate_8h_bps>0",
          m.simulate_entry(edge, 100.0).funding_rate_per_sec, 0.0, tol=1e-18)

    m = lc.ExecutionModel(_params(fill_probability=0.0, missed_fills=False), seed=7)
    check("missed_fills OFF → always fills despite fill_probability=0",
          m.simulate_entry(edge, 100.0) is not None, True)

    m = lc.ExecutionModel(_params(partial_fill_prob=1.0,
                                  partial_fill_min_ratio=0.5,
                                  partial_fills=False), seed=7)
    check("partial_fills OFF → full size despite partial_fill_prob=1",
          m.simulate_entry(edge, 100.0).fill_ratio, 1.0, tol=1e-9)

    m = lc.ExecutionModel(_params(leg_failure_prob=1.0, leg_failure=False), seed=7)
    check("leg_failure OFF → stays hedged despite leg_failure_prob=1",
          m.simulate_entry(edge, 100.0).hedged, True)

    # --- Editor primitives: _apply_set / _toggle_effect. ---
    raw = {}
    lc._apply_set(raw, "order_size_usd", 250.0)
    lc._apply_set(raw, "realism.slippage_bps", 3.0)
    check("_apply_set writes a top-level key", raw["order_size_usd"], 250.0)
    check("_apply_set writes a realism.* key", raw["realism"]["slippage_bps"], 3.0)
    rejected = False
    try:
        lc._apply_set(raw, "realism.bogus", 1)
    except SystemExit:
        rejected = True
    check("_apply_set rejects an unknown field", rejected, True)

    raw = {}
    lc._toggle_effect(raw, "funding", False)
    check("_toggle_effect sets a single effect", raw["realism"]["funding"], False)
    lc._toggle_effect(raw, "all", True)
    check("_toggle_effect 'all' flips master + every effect ON",
          [raw["realism"]["enabled"]]
          + [raw["realism"][e] for e in lc.REALISM_EFFECTS], [True] * 7)
    lc._toggle_effect(raw, "realism", False)
    check("_toggle_effect 'realism' flips ONLY the master",
          (raw["realism"]["enabled"], raw["realism"]["slippage"]), (False, True))

    # --- Full CLI round-trip on a TEMP file (never the real config.json). ---
    path = _os.path.join(tempfile.gettempdir(), "selftest_editor_config.json")
    if _os.path.exists(path):
        _os.remove(path)
    quiet = io.StringIO()
    with contextlib.redirect_stdout(quiet):
        rc1 = lc._config_cli(["--config", path, "--init"])
        rc2 = lc._config_cli(["--config", path, "--disable", "funding",
                              "--set", "realism.slippage_bps=3.0",
                              "--set", "order_size_usd=250"])
    check("config --init returns 0", rc1, 0)
    check("config edit returns 0", rc2, 0)
    cfg = lc.load_config(path)
    check("edit persisted: order size", cfg.order_size_usd, 250.0)
    check("edit persisted: slippage_bps", cfg.realism.slippage_bps, 3.0)
    check("toggle persisted: funding OFF", cfg.realism.funding, False)
    check("untouched effect stays ON", cfg.realism.slippage, True)

    # --- An invalid edit is rejected and does NOT corrupt the file. ---
    with open(path, encoding="utf-8") as fh:
        before = fh.read()
    rejected = False
    try:
        with contextlib.redirect_stdout(quiet):
            lc._config_cli(["--config", path, "--set", "order_size_usd=-5"])
    except SystemExit:
        rejected = True
    with open(path, encoding="utf-8") as fh:
        after = fh.read()
    check("invalid edit raises SystemExit", rejected, True)
    check("invalid edit left the file byte-for-byte unchanged", after, before)
    _os.remove(path)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def _run_all():
    print(f"{BOLD}{'=' * 64}{RESET}")
    print(f"{BOLD} SELF-TEST — multi-asset Binance×Bybit×OKX perp arb (offline){RESET}")
    print(f"{BOLD}{'=' * 64}{RESET}")
    for t in (test_parsing_and_edge_math,
              test_fee_buffer,
              test_state_lock_storm,
              test_monotonic_cooldown,
              test_convergence_exit,
              test_reconciliation_guard,
              test_okx_execution_venue,
              test_multisymbol_independence,
              test_phantom_spread_guard,
              test_trade_journal,
              test_dynamic_config,
              test_execution_realism,
              test_config_editor):
        await t()
    print(f"\n{BOLD}{'=' * 64}{RESET}")
    total = _PASS + _FAIL
    color = GREEN if _FAIL == 0 else RED
    print(f"{color}{BOLD} RESULT: {_PASS}/{total} checks passed"
          + ("" if _FAIL == 0 else f"  ({_FAIL} FAILED)") + f"{RESET}")
    print(f"{BOLD}{'=' * 64}{RESET}")
    return 0 if _FAIL == 0 else 1


def main():
    return asyncio.run(_run_all())


if __name__ == "__main__":
    raise SystemExit(main())
