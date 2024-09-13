import requests
import json
import time
import csv
import logging
import argparse
from urllib.parse import quote
from solana.rpc.core import RPCException
from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed

# Database URL
db_base_url = (
    "https://metrics.solana.com:3000/api/datasources/proxy/uid/HsKEnOt4z/query"
)

testnet_hashing_program_id = [
    "8EsZ3RG7DMDUgxijW4K8kLLVeugJ4t4xa73thXwSotnw",
    "6vXi4dvzDYSw48bX7SHeidjME7VsYmdfvAe1LmS5yP2F",
]


# RateLimiter class definition
class RateLimiter:
    def __init__(self, max_requests_per_second, logger=None):
        self.max_requests_per_second = max_requests_per_second
        self.interval = 1.0 / max_requests_per_second
        self.last_request_time = None
        self.logger = logger or logging.getLogger(__name__)

    def check_rate_limit(self):
        current_time = time.time()
        if self.last_request_time is not None:
            elapsed_time = current_time - self.last_request_time
            if elapsed_time < self.interval:
                wait_time = self.interval - elapsed_time
                self.logger.info(
                    f"Rate limit exceeded. Waiting for {wait_time:.2f} seconds."
                )
                time.sleep(wait_time)

        self.last_request_time = time.time()


# Connect to RPC client with retries
def connect_rpc_client(endpoint: str, rate_limiter: RateLimiter) -> Client:
    logger.info("Connecting to network at " + endpoint)
    rpc_client = Client(endpoint=endpoint, commitment=Confirmed)
    for attempt in range(10):
        rate_limiter.check_rate_limit()
        try:
            res = rpc_client.get_slot(commitment=Confirmed).value
            return rpc_client
        except RPCException as e:
            logger.error(f"Error in RPC: {e}")
        time.sleep(2)
    msg = f"Error: Could not connect to cluster {endpoint} after 10 attempts. Exiting."
    logger.error(msg)
    exit(-1)


# Fetch leader identity for the next slot
def get_slot_leader(slot, url):
    headers = {"Content-Type": "application/json"}
    data = {"jsonrpc": "2.0", "id": 1, "method": "getSlotLeaders", "params": [slot, 1]}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json().get("result", [None])[0]
    return None


# Fetch the replay time for the next slot
def fetch_next_leader_replay_time(slot, url, db):
    next_slot_leader = get_slot_leader(slot + 4, url)
    replay_time = 0
    if next_slot_leader:
        query = f"""SELECT time,slot,replay_time,replay_total_elapsed FROM "autogen"."replay-slot-stats" WHERE "host_id"='{next_slot_leader}' AND slot={slot} ORDER BY time ASC LIMIT 10"""
        encoded_query = quote(query)
        url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get("results", [])
            for result in data:
                series = result.get("series", [])
                for s in series:
                    for value in s["values"]:
                        replay_time = value[3] / 1000
    return replay_time


# Fetch the bank time for the leader slot
def fetch_leader_bank_time(slot, url, db, leader_identity):
    query = f"""SELECT * FROM "autogen"."leader-slot-start-to-cleared-elapsed-ms" WHERE "host_id"='{leader_identity}' AND slot={slot} ORDER BY time ASC LIMIT 30"""
    encoded_query = quote(query)
    leader_time = 0
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json().get("results", [])
        for result in data:
            series = result.get("series", [])
            for s in series:
                for value in s["values"]:
                    leader_time = value[1]
    return leader_time


# Fetch block data for a given slot
def get_block_for_slot(
    cluster,
    rpc_url,
    slot,
    rpc_client: Client,
    db,
    leader_identity,
    is_last_leader_slot=False,
):
    total_txn = total_vote_txn = total_cu = block_reward = 0
    leader_bank_time_ms = replay_time_ms = rakurai_transfer = rakurai_program = (
        other_transfer
    ) = others = 0
    cu_usage_range = {}

    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        try:
            logger.info(f"Calculating block reward for slot {slot}")
            rate_limiter.check_rate_limit()
            resp = rpc_client.get_block(slot, max_supported_transaction_version=0)

            if not resp or not resp.value:
                logger.info(f"Slot {slot} was skipped or missing. Returning None.")
                break

            total_txn = len(resp.value.transactions)
            block_reward += resp.value.rewards[0].lamports

            for each_txn in resp.value.transactions:
                compute_units_consumed = int(each_txn.meta.compute_units_consumed)
                total_cu += compute_units_consumed

                if compute_units_consumed not in cu_usage_range:
                    cu_usage_range[compute_units_consumed] = 1
                else:
                    cu_usage_range[compute_units_consumed] += 1

                if compute_units_consumed in [150, 450]:
                    rakurai_transfer += 1
                if compute_units_consumed == 300:
                    other_transfer += 1

                for each_acc in each_txn.transaction.message.account_keys:
                    if str(each_acc) == "Vote111111111111111111111111111111111111111":
                        total_vote_txn += 1
                    elif str(each_acc) in testnet_hashing_program_id:
                        rakurai_program += 1

            leader_bank_time_ms = fetch_leader_bank_time(
                slot, rpc_url, db, leader_identity
            )
            replay_time_ms = fetch_next_leader_replay_time(slot, rpc_url, db)
            break
        except Exception as e:
            attempt += 1
            logger.error(f"Error in RPC for slot {slot}, retry attempt {attempt}: {e}")

    if cluster == "t":
        others = (
            total_txn
            - total_vote_txn
            - rakurai_transfer
            - other_transfer
            - rakurai_program
        )
        other_txns = 0
        ranges_to_remove = []
        for cu_range, txns in cu_usage_range.items():
            if txns <= 3:
                other_txns += txns
                ranges_to_remove.append(cu_range)
        cu_usage_range["others <= 3"] = other_txns
        for cu_range in ranges_to_remove:
            del cu_usage_range[cu_range]

        for cu, txns in cu_usage_range.items():
            logger.info(f"CU {cu} - Txns {txns}")

        return {
            "slot": slot,
            "total_txn": total_txn,
            "total_vote_txn": total_vote_txn,
            "rakurai_transfer": rakurai_transfer,
            "rakurai_program": rakurai_program,
            "other_transfer": other_transfer,
            "other": others,
            "total_cu": total_cu,
            "cu_x": round(total_cu / 48000000, 5),
            "leader_time_ms": round(leader_bank_time_ms, 5555555),
            "replay_time_ms": round(replay_time_ms, 5),
            "block_rewards": round(block_reward, 5),
            "block_rewards_sol": round(block_reward / 1000000000, 5),
        }
    else:
        return {
            "slot": slot,
            "total_txn": total_txn,
            "total_vote_txn": total_vote_txn,
            "other_txns": total_txn - total_vote_txn,
            "total_cu": total_cu,
            "cu_x": round(total_cu / 48000000, 5),
            "leader_time_ms": round(leader_bank_time_ms, 5),
            "replay_time_ms": round(replay_time_ms, 5),
            "block_rewards": round(block_reward, 5),
            "block_rewards_sol": round(block_reward / 1000000000, 5),
        }


# Slot processing
def process_slots(args, db):
    try:
        rpc_client = connect_rpc_client(args.rpc_url, rate_limiter)
        slots = [args.start_slot + i for i in range(args.count)]
        leader_identity = get_slot_leader(args.start_slot, args.rpc_url)
        logger.info(f"Leader for first slot {args.start_slot}: {leader_identity}")

        with open(args.output_file, mode="w", newline="") as file:
            if args.cluster == "t":
                writer = csv.DictWriter(
                    file,
                    fieldnames=[
                        "slot",
                        "total_txn",
                        "total_vote_txn",
                        "rakurai_transfer",
                        "rakurai_program",
                        "other_transfer",
                        "other",
                        "total_cu",
                        "cu_x",
                        "leader_time_ms",
                        "replay_time_ms",
                        "block_rewards",
                        "block_rewards_sol",
                    ],
                )
            else:
                writer = csv.DictWriter(
                    file,
                    fieldnames=[
                        "slot",
                        "total_txn",
                        "total_vote_txn",
                        "other_txns",
                        "total_cu",
                        "cu_x",
                        "leader_time_ms",
                        "replay_time_ms",
                        "block_rewards",
                        "block_rewards_sol",
                    ],
                )
            writer.writeheader()

            for slot in slots:
                logger.info(f"Processing slot: {slot}")
                result = get_block_for_slot(
                    args.cluster,
                    args.rpc_url,
                    slot,
                    rpc_client,
                    db,
                    leader_identity,
                )
                if result:
                    writer.writerow(result)

    except Exception as e:
        logger.error(f"Error during slot processing: {e}")


# Command line arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Solana block data")
    parser.add_argument(
        "--cluster",
        required=False,
        type=str,
        default="m",
        help="Cluster name (m: mainnet, t: testnet)",
    )
    parser.add_argument(
        "--rpc_url",
        required=False,
        type=str,
        help="RPC URL for Solana (default soalna public RPCs)",
    )
    parser.add_argument(
        "--start_slot", type=int, required=True, help="Starting slot to process"
    )
    parser.add_argument(
        "--count", type=int, default=4, help="Number of slots to process"
    )
    parser.add_argument(
        "--output_file",
        default="block_metrics_analyzer_results.csv",
        help="CSV file to save results",
    )
    parser.add_argument(
        "--max_requests_per_second",
        type=int,
        default=20,
        help="Rate limit for RPC requests",
    )
    args = parser.parse_args()

    # Setup logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_file_name = f"{time.strftime('%Y-%m-%d%H-%M-%S')}.log"
    handler = logging.FileHandler(log_file_name, mode="w")
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [Line:%(lineno)d] %(message)s")
    )
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # Initialize rate limiter
    rate_limiter = RateLimiter(args.max_requests_per_second, logger)
    if args.cluster == "m":
        process_slots(args, "mainnet-beta")
    elif args.cluster == "t":
        process_slots(args, "tds")
