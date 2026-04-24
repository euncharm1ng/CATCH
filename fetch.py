#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.error
import urllib.request

# DEFAULT_URL = "https://rpc.hoodi.ethpandaops.io"
# DEFAULT_URL = "http://143.248.47.18:8545"
DEFAULT_URL = "https://sepolia-rollup.arbitrum.io/rpc"



def to_hex_block(block_number: int) -> str:
    if block_number < 0:
        raise ValueError("block number must be non-negative")
    return hex(block_number)


def json_rpc(url: str, method: str, params: list) -> dict:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "CATCH-fetch/1.0 (+https://github.com)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", None)
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {error_body[:500]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Connection error to {url}: {exc}") from exc

    if not body.strip():
        raise RuntimeError(f"Empty response from {url} (status={status})")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        snippet = body[:500].replace("\n", " ")
        raise RuntimeError(f"Non-JSON response from {url} (status={status}): {snippet}") from exc


def fetch_block(block_number: int, url: str, full_tx: bool) -> dict:
    return json_rpc(url, "eth_getBlockByNumber", [to_hex_block(block_number), full_tx])


def fetch_receipts(tx_hashes: list, url: str) -> list:
    receipts = []
    for tx_hash in tx_hashes:
        res = json_rpc(url, "eth_getTransactionReceipt", [tx_hash])
        receipts.append(res)
    return receipts


def fetch_logs(block_number: int, url: str) -> dict:
    block_hex = to_hex_block(block_number)
    return json_rpc(url, "eth_getLogs", [{"fromBlock": block_hex, "toBlock": block_hex}])


def fetch_traces(block_number: int, url: str) -> dict:
    block_hex = to_hex_block(block_number)
    try:
        return json_rpc(url, "debug_traceBlockByNumber", [block_hex, {}])
    except RuntimeError:
        return json_rpc(url, "trace_block", [block_hex])



def fetch_and_save_block(block_number: int, args) -> bool:
    try:
        result = {"block": fetch_block(block_number, args.url, args.full_tx)}
        block = result["block"].get("result")
        tx_hashes = []
        if block and block.get("transactions"):
            if args.full_tx:
                tx_hashes = [tx.get("hash") for tx in block["transactions"] if tx.get("hash")]
            else:
                tx_hashes = list(block["transactions"])

        if args.receipts:
            result["receipts"] = fetch_receipts(tx_hashes, args.url)
        if args.logs:
            result["logs"] = fetch_logs(block_number, args.url)
        if args.traces:
            result["traces"] = fetch_traces(block_number, args.url)
    except Exception as exc:
        print(f"Error fetching block {block_number}: {exc}", file=sys.stderr)
        return False

    filename = f"raw/{block_number}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(f"Saved {filename}")
    return True

def parsearg():
    parser = argparse.ArgumentParser(description="Fetch raw block data via JSON-RPC.")
    parser.add_argument("block", type=int, help="Start block number (decimal)")
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="End block number (inclusive). If omitted, fetches only the start block.",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"JSON-RPC endpoint (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--full-tx",
        action="store_true",
        help="Include full transaction objects (default: hashes only)",
    )
    parser.add_argument(
        "--receipts",
        action="store_true",
        help="Fetch transaction receipts for each block",
    )
    parser.add_argument(
        "--logs",
        action="store_true",
        help="Fetch logs for each block",
    )
    parser.add_argument(
        "--traces",
        action="store_true",
        help="Fetch internal traces for each block (debug/trace API required)",
    )
    return parser.parse_args()

def main() -> int:
    args = parsearg()

    start_block = args.block
    end_block = args.end if args.end is not None else start_block
    if end_block < start_block:
        print(f"Error: --end ({end_block}) must be >= start block ({start_block})", file=sys.stderr)
        return 1

    total = end_block - start_block + 1
    failed = 0
    for block_number in range(start_block, end_block + 1):
        if total > 1:
            print(f"[{block_number - start_block + 1}/{total}] Fetching block {block_number}...")
        if not fetch_and_save_block(block_number, args):
            failed += 1

    if failed:
        print(f"{failed}/{total} block(s) failed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
