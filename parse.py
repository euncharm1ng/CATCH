import json
import csv
import sys
import os
import argparse
import sqlite3
import glob

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
V2_SYNC_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# Reserve topics to screen. Add/remove entries to control extraction coverage.
SYNC_TOPICS = [
    V2_SYNC_TOPIC,
    V3_SWAP_TOPIC,
]
ETH_ADDRESS    = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

RAW_DIR     = "raw"
PARSED_DIR  = "parsed"
DEFAULT_DB  = "transfers.db"

FIELDNAMES = [
    "token_address", "from_address", "to_address", "value",
    "transaction_index", "transaction_hash", "block_number", "reserve_data",
]


def _to_uint256_words(reserve0, reserve1):
    return f"0x{reserve0:064x}{reserve1:064x}"


def _decode_v3_swap_to_reserve_data(data_hex):
    """Convert a Uniswap V3 Swap log data payload into V2-shaped reserve_data words.

    The returned value is a 2-word hex string (reserve0, reserve1) so downstream
    consumers can parse it with the existing reserve parser.
    """
    if not isinstance(data_hex, str) or not data_hex.startswith("0x"):
        return ""

    clean = data_hex[2:]
    words = [clean[i:i + 64] for i in range(0, len(clean), 64)]
    if len(words) < 5:
        return ""

    try:
        sqrt_price_x96 = int(words[2], 16)
        liquidity = int(words[3], 16)
    except ValueError:
        return ""

    if sqrt_price_x96 == 0 or liquidity == 0:
        return ""

    # Active-range virtual reserves derived from V3 state:
    # reserve0 = L / sqrt(P), reserve1 = L * sqrt(P), with sqrt(P)=sqrtPriceX96/2^96.
    reserve0 = (liquidity << 96) // sqrt_price_x96
    reserve1 = (liquidity * sqrt_price_x96) >> 96
    return _to_uint256_words(reserve0, reserve1)


def _decode_v2_sync_to_reserve_data(data_hex):
    if not isinstance(data_hex, str) or not data_hex.startswith("0x"):
        return ""
    clean = data_hex[2:]
    if len(clean) < 128:
        return ""
    return "0x" + clean[:128]


RESERVE_DECODER_BY_TOPIC = {
    V2_SYNC_TOPIC: _decode_v2_sync_to_reserve_data,
    V3_SWAP_TOPIC: _decode_v3_swap_to_reserve_data,
}


def extract_transfers(file_path):
    """Parse a raw JSON block file and return a list of transfer row dicts."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None

    block_data = data.get("block", {})
    if "result" in block_data:
        block_data = block_data["result"]

    block_number = block_data.get("number")
    if block_number and str(block_number).startswith("0x"):
        block_number = int(block_number, 16)

    transactions = block_data.get("transactions", [])

    logs_data = data.get("logs", {})
    if "result" in logs_data:
        logs_data = logs_data["result"]
    elif not isinstance(logs_data, list):
        logs_data = []

    tx_sync_data = {}
    for log in logs_data:
        topics = log.get("topics", [])
        if not topics:
            continue
        topic0 = topics[0]
        if topic0 not in SYNC_TOPICS:
            continue

        decoder = RESERVE_DECODER_BY_TOPIC.get(topic0)
        if decoder is None:
            continue

        tx_hash = log.get("transactionHash")
        reserve_data = decoder(log.get("data", ""))
        if tx_hash and reserve_data:
            tx_sync_data[tx_hash] = reserve_data

    all_transfers = []

    for tx in transactions:
        tx_hash = tx.get("hash")
        try:
            value = int(tx.get("value", "0x0"), 16)
        except ValueError:
            value = 0
        if value > 0:
            tx_index = int(tx.get("transactionIndex", "0x0"), 16)
            all_transfers.append({
                "token_address":     ETH_ADDRESS,
                "from_address":      tx.get("from"),
                "to_address":        tx.get("to"),
                "value":             str(value),
                "transaction_index": tx_index,
                "transaction_hash":  tx_hash,
                "block_number":      block_number,
                "reserve_data":      tx_sync_data.get(tx_hash, ""),
                "_sort":             (tx_index, -1),
            })

    for log in logs_data:
        topics = log.get("topics", [])
        if not topics or topics[0] != TRANSFER_TOPIC or len(topics) != 3:
            continue
        tx_hash   = log.get("transactionHash")
        from_addr = "0x" + topics[1][-40:]
        to_addr   = "0x" + topics[2][-40:]
        data_hex  = log.get("data", "0x0") or "0x0"
        if data_hex == "0x":
            data_hex = "0x0"
        try:
            value = int(data_hex, 16)
        except ValueError:
            value = 0
        if value > 0:
            tx_index  = int(log.get("transactionIndex", "0x0"), 16)
            log_index = int(log.get("logIndex",         "0x0"), 16)
            all_transfers.append({
                "token_address":     log.get("address"),
                "from_address":      from_addr,
                "to_address":        to_addr,
                "value":             str(value),
                "transaction_index": tx_index,
                "transaction_hash":  tx_hash,
                "block_number":      block_number,
                "reserve_data":      tx_sync_data.get(tx_hash, ""),
                "_sort":             (tx_index, log_index),
            })

    all_transfers.sort(key=lambda r: r["_sort"])
    for row in all_transfers:
        del row["_sort"]

    return all_transfers


# ── CSV ────────────────────────────────────────────────────────────────────────

def write_csv(transfers, source_path):
    os.makedirs(PARSED_DIR, exist_ok=True)
    out = os.path.join(PARSED_DIR, os.path.basename(source_path).replace(".json", ".csv"))
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(transfers)
    return out


# ── SQLite ─────────────────────────────────────────────────────────────────────

def init_db(db_path):
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            token_address     TEXT,
            from_address      TEXT,
            to_address        TEXT,
            value             TEXT,
            transaction_index INTEGER,
            transaction_hash  TEXT,
            block_number      INTEGER,
            reserve_data      TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_block   ON transfers (block_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tx_hash ON transfers (transaction_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_from    ON transfers (from_address)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_to      ON transfers (to_address)")
    conn.commit()
    return conn


def write_db(transfers, conn, block_number):
    # Replace any existing rows for this block so re-runs are idempotent.
    if block_number is not None:
        conn.execute("DELETE FROM transfers WHERE block_number = ?", (block_number,))
    conn.executemany(
        "INSERT INTO transfers VALUES (?,?,?,?,?,?,?,?)",
        [(r["token_address"], r["from_address"], r["to_address"], r["value"],
          r["transaction_index"], r["transaction_hash"], r["block_number"], r["reserve_data"])
         for r in transfers],
    )
    conn.commit()


# ── Core ───────────────────────────────────────────────────────────────────────

def parse_block(file_path, db_conn=None):
    transfers = extract_transfers(file_path)
    if transfers is None:
        return False

    out_csv = write_csv(transfers, file_path)
    print(f"  CSV -> {out_csv}  ({len(transfers)} rows)")

    if db_conn is not None:
        block_number = transfers[0]["block_number"] if transfers else None
        write_db(transfers, db_conn, block_number)
        print(f"  DB  -> {len(transfers)} row(s) inserted (block {block_number})")

    return True


# ── CLI ────────────────────────────────────────────────────────────────────────

def parsearg():
    parser = argparse.ArgumentParser(
        description="Parse raw block JSON files into CSV and/or SQLite."
    )
    parser.add_argument(
        "file", nargs="?",
        help="Path to a single raw JSON block file",
    )
    parser.add_argument(
        "--all", action="store_true", dest="all_files",
        help=f"Process every JSON file in {RAW_DIR}/",
    )
    parser.add_argument(
        "--db", nargs="?", const=DEFAULT_DB, default=None, metavar="PATH",
        help=f"Also write to a SQLite database (default path when flag is bare: {DEFAULT_DB})",
    )
    args = parser.parse_args()

    if not args.file and not args.all_files:
        parser.print_help()
        sys.exit(1)

    if args.file and args.all_files:
        print("Error: specify either a file or --all, not both.", file=sys.stderr)
        sys.exit(1)
    return args


def main() -> int:
    args = parsearg()

    db_conn = init_db(args.db) if args.db else None

    try:
        if args.all_files:
            json_files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")))
            if not json_files:
                print(f"No JSON files found in {RAW_DIR}/")
                sys.exit(0)
            print(f"Processing {len(json_files)} file(s)...")
            failed = 0
            for i, path in enumerate(json_files, 1):
                print(f"[{i}/{len(json_files)}] {path}")
                if not parse_block(path, db_conn):
                    failed += 1
            if failed:
                print(f"\n{failed}/{len(json_files)} file(s) failed.", file=sys.stderr)
                sys.exit(1)
        else:
            if not parse_block(args.file, db_conn):
                sys.exit(1)
    finally:
        if db_conn:
            db_conn.close()
            print(f"\nDatabase saved to {args.db}")


if __name__ == "__main__":
    raise SystemExit(main())
