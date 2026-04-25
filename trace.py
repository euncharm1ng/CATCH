import argparse
import sqlite3
import pandas as pd
import sys
import os
import json
from collections import deque

SCREENING_THRESHOLD = 0.01  # flag swaps where amount_in > 1% of total pool liquidity
OUTLIER_REVENUE_THRESHOLD  = 0.90   # flag swaps where amount_in > 10% of total pool liquidity
OUTLIAR_LOSS_THRESHOLD     = 0.2    # flag losses where victim's output USD value is >20% of their input USD value
ARBITRAGE_LOOKAHEAD_BLOCKS = 10
DEFAULT_DB        = "./transfers.db"
DEFAULT_EXCHANGES = "./contracts/exchange.csv"
DEFAULT_PRICE_ORACLE = "./utils/price_oracle.csv"
DEFAULT_STATS       = "./utils/gain_stats.csv"
DEFAULT_RAW_DIR     = "./raw"

_TX_SENDER_CACHE = {}

def parse_reserve_data(hex_data):
    if not isinstance(hex_data, str) or not hex_data.startswith("0x"):
        return 0, 0
    clean = hex_data[2:]
    if len(clean) >= 128:
        return int(clean[:64], 16), int(clean[64:128], 16)
    return 0, 0


def _load_block_tx_sender_map(block_number, raw_dir=DEFAULT_RAW_DIR):
    """Load tx_hash->sender map for a block from raw/<block_number>.json."""
    if block_number in _TX_SENDER_CACHE:
        return _TX_SENDER_CACHE[block_number]

    path = os.path.join(raw_dir, f"{int(block_number)}.json")
    sender_map = {}

    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                payload = json.load(f)
            block = payload.get("block", {})
            if isinstance(block, dict) and "result" in block:
                block = block["result"]
            txs = block.get("transactions", []) if isinstance(block, dict) else []
            for tx in txs:
                tx_hash = (tx.get("hash") or "").lower()
                tx_from = (tx.get("from") or "").lower()
                if tx_hash and tx_from:
                    sender_map[tx_hash] = tx_from
        except (OSError, ValueError, TypeError):
            sender_map = {}

    _TX_SENDER_CACHE[block_number] = sender_map
    return sender_map


def _get_tx_sender(tx_hash, block_number, raw_dir=DEFAULT_RAW_DIR):
    if tx_hash is None or block_number is None:
        return None
    sender_map = _load_block_tx_sender_map(block_number, raw_dir=raw_dir)
    return sender_map.get(str(tx_hash).lower())


def _s(h):
    """Truncate a hex address/hash to 0x + 8 chars for readable output."""
    return h[:10] + "..." if h and len(h) > 10 else h


_PRICE_ORACLE_CACHE = None


def _load_price_oracle(path=DEFAULT_PRICE_ORACLE):
    prices = {}
    if not os.path.exists(path):
        return prices

    with open(path, "r") as f:
        for line in f:
            parts = [p.strip() for p in line.strip().split(",")]
            if len(parts) != 3:
                continue
            _, token_address, usd_price = parts
            if token_address.lower() == "token_address":
                continue
            try:
                prices[token_address.lower()] = float(usd_price)
            except ValueError:
                continue
    return prices


def _get_token_price_usd(token_address):
    global _PRICE_ORACLE_CACHE
    if not token_address:
        return None
    if _PRICE_ORACLE_CACHE is None:
        _PRICE_ORACLE_CACHE = _load_price_oracle()
    return _PRICE_ORACLE_CACHE.get(token_address.lower().strip())


def _to_usd(amount_raw, token_address, decimals=18):
    price = _get_token_price_usd(token_address)
    if price is None:
        return None
    return (float(amount_raw) / (10 ** decimals)) * price


def calculate_catch_metrics(tx_hash, current_addr, exchange_addr, transfers, threshold=SCREENING_THRESHOLD):
    tx = transfers[transfers["transaction_hash"] == tx_hash]
    if tx.empty:
        return False, 0.0

    sent     = tx[(tx["source"] == current_addr)  & (tx["target"] == exchange_addr)]
    received = tx[(tx["source"] == exchange_addr) & (tx["target"] == current_addr)]
    if sent.empty or received.empty:
        raise ValueError(f"Missing sent or received transfer in {tx_hash}")

    token_in  = sent.iloc[0]["address"].lower()
    amount_in = float(sent.iloc[0]["amount"])
    token_out = received.iloc[0]["address"].lower()

    reserve_rows = tx[tx["reserve_data"].notna() & (tx["reserve_data"] != "")]
    if reserve_rows.empty:
        raise ValueError(f"No reserve data in {tx_hash}")
    r0, r1 = parse_reserve_data(reserve_rows.iloc[0]["reserve_data"])
    if r0 == 0 and r1 == 0:
        raise ValueError(f"Invalid reserve data in {tx_hash}")
    
    # Sync fires after the swap, so back-calculate the pre-swap input reserve.
    # token0 is the lexicographically smaller address (Uniswap V2 convention).
    r_in_post   = r0 if token_in < token_out else r1
    r_in_before = r_in_post - amount_in
    if r_in_before <= 0:
        raise ValueError(f"Back-calculated pre-swap reserve is non-positive in {tx_hash}")

    ratio = amount_in / (2 * r_in_before)
    return ratio > threshold, ratio


# ── Sandwich detection ──────────────────────────────────────────────────────────────

def detect_sandwich(victim_tx_hash, victim_addr, exchange_addr, transfers, threshold=0.02):
    received = transfers[
        (transfers["transaction_hash"] == victim_tx_hash) &
        (transfers["source"] == exchange_addr) &
        (transfers["target"] == victim_addr)
    ]
    if received.empty:
        return None

    token_out    = received.iloc[0]["address"]
    victim_index = received.iloc[0]["transaction_index"]

    by_token = transfers[transfers["address"] == token_out]
    potential_a1 = by_token[
        (by_token["transaction_index"] < victim_index) &
        (by_token["source"] == exchange_addr)
    ]
    potential_a2 = by_token[
        (by_token["transaction_index"] > victim_index) &
        (by_token["target"] == exchange_addr)
    ]
    
    if potential_a1.empty or potential_a2.empty:
        return None

    for _, a1 in potential_a1.iterrows():
        attacker  = a1["target"]
        amount_a1 = float(a1["amount"])
        if attacker == victim_addr:
            continue

        for _, a2 in potential_a2[potential_a2["source"] == attacker].iterrows():
            amount_a2 = float(a2["amount"])
            max_val   = max(amount_a1, amount_a2)
            if max_val == 0:
                continue
            if abs(amount_a2 - amount_a1) / max_val <= threshold:
                # Detect when the output of the fronbun matches the input of the backrun, and calculate profit if possible.
                # Profit = backrun token_out_amount - frontrun token_in_amount
                # (both in the token the sandwicher starts and ends with, i.e. the
                #  token they sent into the exchange on the frontrun)
                frontrun_paid = transfers[
                    (transfers["transaction_hash"] == a1["transaction_hash"]) &
                    (transfers["source"] == attacker) &
                    (transfers["target"] == exchange_addr)
                ]
                backrun_received = transfers[
                    (transfers["transaction_hash"] == a2["transaction_hash"]) &
                    (transfers["source"] == exchange_addr) &
                    (transfers["target"] == attacker)
                ]

                profit_token  = None
                profit_amount = None
                if not frontrun_paid.empty and not backrun_received.empty:
                    profit_token  = frontrun_paid.iloc[0]["address"]
                    profit_amount = (
                        float(backrun_received.iloc[0]["amount"])
                        - float(frontrun_paid.iloc[0]["amount"])
                    )

                profit_str = (
                    f"  profit={profit_amount:.4e}  token={_s(profit_token)}"
                    if profit_amount is not None else ""
                )

                block_number = a1.get("block_number")
                frontrun_sender = _get_tx_sender(a1["transaction_hash"], block_number)
                backrun_sender = _get_tx_sender(a2["transaction_hash"], block_number)
                
                print(f"      [sandwich found]")
                print(f"        attacker : {attacker}")
                print(f"        frontrun : {_s(a1['transaction_hash'])}  (idx {a1['transaction_index']})")
                print(f"        frontrun sender : {frontrun_sender or 'N/A'}")
                print(f"        victim   : {_s(victim_tx_hash)}  (idx {victim_index})")
                print(f"        backrun  : {_s(a2['transaction_hash'])}  (idx {a2['transaction_index']})")
                print(f"        backrun sender  : {backrun_sender or 'N/A'}")
                if profit_str:
                    print(f"       {profit_str}")
                victim_paid = transfers[
                    (transfers["transaction_hash"] == victim_tx_hash) &
                    (transfers["source"] == victim_addr) &
                    (transfers["target"] == exchange_addr)
                ]

                victim_token_in = None
                victim_amount_in = None
                if not victim_paid.empty:
                    victim_token_in = victim_paid.iloc[0]["address"]
                    victim_amount_in = float(victim_paid.iloc[0]["amount"])

                victim_token_out = received.iloc[0]["address"]
                victim_amount_out = float(received.iloc[0]["amount"])

                victim_in_usd = (
                    _to_usd(victim_amount_in, victim_token_in)
                    if victim_token_in is not None and victim_amount_in is not None
                    else None
                )
                victim_out_usd = _to_usd(victim_amount_out, victim_token_out)
                profit_usd = (
                    _to_usd(profit_amount, profit_token)
                    if profit_token is not None and profit_amount is not None
                    else None
                )

                placement = "N/A"
                if profit_usd is not None and os.path.exists(DEFAULT_STATS):
                    with open(DEFAULT_STATS, "r") as f:
                        next(f, None)
                        for line in f:
                            percentile, value_usd = [p.strip() for p in line.strip().split(",")]
                            if profit_usd > float(value_usd):
                                placement = percentile
                                break

                print("\n        victim in :", f"{victim_amount_in:.4e}" if victim_amount_in is not None else "N/A", f"token={_s(victim_token_in)}")
                print("        victim out:", f"{victim_amount_out:.4e}", f"token={_s(victim_token_out)}")
                print("        victim in USD :", f"{victim_in_usd:.6f}" if victim_in_usd is not None else "N/A")
                print("        victim out USD:", f"{victim_out_usd:.6f}" if victim_out_usd is not None else "N/A")
                print("        profit USD    :", f"{profit_usd:.6f}" if profit_usd is not None else "N/A")
                print("        percentile    : over", placement)
                if isinstance(placement, str) and placement.startswith("p"):
                    if int(placement[1:]) >= OUTLIER_REVENUE_THRESHOLD * 100:
                        print(f"        [outlier detected: profit exceeds {OUTLIER_REVENUE_THRESHOLD*100:.0f}th percentile]")
                
                if victim_out_usd/victim_in_usd > OUTLIAR_LOSS_THRESHOLD if victim_in_usd is not None and victim_out_usd is not None else False:
                    print(f"        [outlier detected: loss exceeds {OUTLIAR_LOSS_THRESHOLD*100:.0f}th percentile]")

                {
                    "attacker":     attacker,
                    "profit_token": profit_token,
                    "profit_amount": profit_amount,
                    "frontrun_tx":  a1["transaction_hash"],
                    "backrun_tx":   a2["transaction_hash"],
                }
                return attacker

    return None


# ── Arbitrage detection ─────────────────────────────────────────────────────────────

def _build_swaps(tx_transfers, exchange_set):
    """
    Reconstruct swap dicts from all transfers in a single transaction.
    Each exchange interaction (tokens in + tokens out) becomes one swap entry.
    Handles both simple (BOT↔pool) and fully-routed (pool→pool) multi-hop paths.
    """
    swaps = []
    touched = (set(tx_transfers["source"]) | set(tx_transfers["target"])) & exchange_set
    print("touched : ", touched)
    for exchange in touched:
        ins  = tx_transfers[tx_transfers["target"] == exchange]
        outs = tx_transfers[tx_transfers["source"] == exchange]
        print("ins : ", ins)
        print("outs : ", outs)
        if ins.empty or outs.empty:
            continue
        for _, in_row in ins.iterrows():
            for _, out_row in outs.iterrows():
                swaps.append({
                    "contract_address":  exchange,
                    "from_address":      in_row["source"],
                    "to_address":        out_row["target"],
                    "token_in_address":  in_row["address"],
                    "token_out_address": out_row["address"],
                    "token_in_amount":   float(in_row["amount"]),
                    "token_out_amount":  float(out_row["amount"]),
                })
    return swaps


def _amounts_close(a, b, tol=0.03):
    mx = max(a, b)
    return mx == 0 or abs(a - b) / mx <= tol


def _outs_match_ins(s_out, s_in):
    """True when s_out feeds into s_in: same token, connected addresses, matching amounts."""
    if s_out["token_out_address"] != s_in["token_in_address"]:
        return False
    addr_ok = (
        s_out["contract_address"] == s_in["from_address"]
        or s_out["to_address"]    == s_in["contract_address"]
        or s_out["to_address"]    == s_in["from_address"]
    )
    return addr_ok and _amounts_close(s_out["token_out_amount"], s_in["token_in_amount"])


def _start_end_pairs(swaps):
    """
    Find all (start_swap, [end_swaps]) where start and end together close a cycle:
      start.token_in == end.token_out  and  start.from_address == end.to_address.
    """
    pool_addrs = {s["contract_address"] for s in swaps}
    pairs = []
    for i, start in enumerate(swaps):
        if start["from_address"] in pool_addrs:
            continue
        ends = [
            end for j, end in enumerate(swaps)
            if i != j
            and start["token_in_address"]   == end["token_out_address"]
            and start["contract_address"]   != end["contract_address"]
            and start["from_address"]       == end["to_address"]
        ]
        if ends:
            pairs.append((start, ends))
    return pairs


def _shortest_route(start, ends, all_swaps, max_len=None):
    if not ends or (max_len is not None and max_len < 2):
        return None
    for end in ends:
        if _outs_match_ins(start, end):
            return [start, end]
    if max_len == 2:
        return None
    others = [s for s in all_swaps if s is not start and s not in ends]
    if not others:
        return None
    best     = None
    max_rem  = None if max_len is None else max_len - 1
    for nxt in others:
        if _outs_match_ins(start, nxt):
            rest = _shortest_route(nxt, ends, others, max_rem)
            if rest is not None and (best is None or len(rest) < len(best)):
                best    = rest
                max_rem = len(rest) - 1
    return None if best is None else [start] + best


def _find_arbitrages(swaps):
    """Return list of arbitrage dicts found among swaps in a single transaction."""
    results = []
    used    = []
    for start, ends in _start_end_pairs(swaps):
        if start in used:
            continue
        route = _shortest_route(start, [e for e in ends if e not in used], swaps)
        if route is None:
            continue
        results.append({
            "account_address": route[0]["from_address"],
            "profit_token":    route[0]["token_in_address"],
            "start_amount":    route[0]["token_in_amount"],
            "end_amount":      route[-1]["token_out_amount"],
            "profit_amount":   route[-1]["token_out_amount"] - route[0]["token_in_amount"],
            "hops":            len(route),
        })
        used.extend(route)
    return results


def detect_arbitrage(victim_tx_hash, exchange_addr, transfers, exchange_map):
    """
    Search for arbitrage transactions that occur after victim_tx_hash (same block)
    and pass through exchange_addr.  Returns the arbitrageur's address or None.
    """
    victim_rows = transfers[transfers["transaction_hash"] == victim_tx_hash]
    if victim_rows.empty:
        return None

    victim_index = victim_rows.iloc[0]["transaction_index"]
    victim_block = victim_rows.iloc[0]["block_number"]
    exchange_set = set(exchange_map.keys())

    later = transfers[
        (
            (
                (transfers["block_number"] == victim_block) &
                (transfers["transaction_index"] > victim_index)
            )
            |
            (
                (transfers["block_number"] > victim_block) &
                (transfers["block_number"] <= victim_block + ARBITRAGE_LOOKAHEAD_BLOCKS)
            )
        )
    ]

    for tx_hash, tx_transfers in later.groupby("transaction_hash"):
        # Only inspect transactions that touch the same pool
        if (exchange_addr not in tx_transfers["source"].values and
                exchange_addr not in tx_transfers["target"].values):
            continue

        swaps = _build_swaps(tx_transfers, exchange_set)
        if len(swaps) < 2:
            continue

        for arb in _find_arbitrages(swaps):
            print(f"      [arbitrage found]")
            print(f"        account  : {arb['account_address']}")
            print(f"        tx       : {_s(tx_hash)}")
            print(f"        profit   : {arb['profit_amount']:.4e}  token={_s(arb['profit_token'])}  hops={arb['hops']}")
            return arb["account_address"]

    return None


# ─────────────────────────────────────────────────────────────────────────────

def load_data(db_path, exchange_path):
    if not os.path.exists(db_path):
        print(f"Error: database not found at {db_path}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(exchange_path):
        print(f"Error: exchanges file not found at {exchange_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading transfers from {db_path}...")
    conn = sqlite3.connect(db_path)
    transfers = pd.read_sql_query(
        """
        SELECT
            token_address           AS address,
            LOWER(from_address)     AS source,
            LOWER(to_address)       AS target,
            value                   AS amount,
            transaction_index,
            transaction_hash,
            block_number,
            reserve_data
        FROM transfers
        """,
        conn,
    )
    conn.close()

    print(f"Loading exchanges from {exchange_path}...")
    exchanges = pd.read_csv(exchange_path)
    if "name" not in exchanges.columns:
        exchanges["name"] = exchanges.get("label", "Unknown")
    exchanges["address"] = exchanges["address"].str.lower()

    return transfers, exchanges


def trace_address(target_addr, transfers, exchanges, max_depth=100, mode=None):
    target_addr  = target_addr.lower()
    exchange_map = dict(zip(exchanges["address"], exchanges["name"]))

    SEP = "─" * 60
    print(f"\n{SEP}")
    print(f" Tracing  {target_addr}")
    print(f" depth={max_depth}   mode={mode or 'off'}")
    print(SEP)

    queue             = deque([target_addr])
    visited           = {target_addr}
    depth_map         = {target_addr: 0}
    cluster_eoas      = {target_addr}
    cluster_exchanges = set()

    while queue:
        current = queue.popleft()
        d       = depth_map[current]
        if d >= max_depth:
            continue

        # print(f"\n[{d}]  {current}")

        outgoing = transfers[transfers["source"] == current][
            ["target", "address", "transaction_hash", "amount"]
        ].drop_duplicates()

        for _, row in outgoing.iterrows():
            neighbor = row["target"]
            ex_name  = exchange_map.get(neighbor)

            if ex_name:
                cluster_exchanges.add(neighbor)
                if mode is not None:
                    try:
                        suspicious, ratio = calculate_catch_metrics(row["transaction_hash"], current, neighbor, transfers)
                    except ValueError as e:
                        print(f"    → pool  {_s(neighbor)}   tx={_s(row['transaction_hash'])}   [skip: {e}]")
                        suspicious = False
                    else:
                        if suspicious:
                            print(f"    → pool  {_s(neighbor)}   tx={_s(row['transaction_hash'])}   ratio={ratio:.4f}  ▶ screening")
                    if suspicious:
                        if mode == "sandwich":
                            actor = detect_sandwich(row["transaction_hash"], current, neighbor, transfers)
                            # actor  = result["attacker"] if result else None
                        else:  # mode == "arb"
                            actor = detect_arbitrage(row["transaction_hash"], neighbor, transfers, exchange_map)
                        if actor is None:
                            print(f"      no {mode} detected")
                        elif actor not in visited:
                            visited.add(actor)
                            depth_map[actor] = d + 1
                            queue.append(actor)
                            cluster_eoas.add(actor)
            elif neighbor not in visited:
                visited.add(neighbor)
                depth_map[neighbor] = d + 1
                queue.append(neighbor)
                cluster_eoas.add(neighbor)

        incoming = transfers[transfers["target"] == current][
            ["source", "address", "transaction_hash", "amount"]
        ].drop_duplicates()

        for _, row in incoming.iterrows():
            neighbor = row["source"]
            if not exchange_map.get(neighbor) and neighbor not in visited:
                visited.add(neighbor)
                depth_map[neighbor] = d + 1
                queue.append(neighbor)
                cluster_eoas.add(neighbor)

    eoa_list = sorted(cluster_eoas)
    pool_list = sorted(cluster_exchanges)
    print(f"\n{SEP}")
    print(f" Result   EOAs={len(cluster_eoas)}   pools={len(cluster_exchanges)}")
    print(SEP)
    print(" EOAs:")
    for addr in eoa_list[:100]:
        print(f"   {addr}")
    if len(eoa_list) > 100:
        print(f"   ... ({len(eoa_list) - 100} more)")
    if pool_list:
        print(" Pools:")
        for addr in pool_list:
            print(f"   {addr}")


def parsearg():
    parser = argparse.ArgumentParser(description="Trace an address and cluster related EOAs.")
    parser.add_argument("address",     help="Target address to trace")
    parser.add_argument("--db",        default=DEFAULT_DB,       help=f"Path to transfers SQLite DB (default: {DEFAULT_DB})")
    parser.add_argument("--exchanges", default=DEFAULT_EXCHANGES, help=f"Path to exchanges CSV (default: {DEFAULT_EXCHANGES})")
    parser.add_argument("--depth",     type=int, default=100,    help="Max BFS depth (default: 100)")
    parser.add_argument(
        "--mode",
        choices=["sandwich", "arb"],
        default=None,
        help="MEV detection mode: 'sandwich' detects sandwich attacks, 'arb' detects arbitrage (default: off)",
    )
    return parser.parse_args()


def main():
    args = parsearg()
    transfers, exchanges = load_data(args.db, args.exchanges)
    trace_address(args.address, transfers, exchanges, max_depth=args.depth, mode=args.mode)


if __name__ == "__main__":
    main()
