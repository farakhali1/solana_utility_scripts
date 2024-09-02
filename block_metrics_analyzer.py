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

db_base_url = (
    "https://metrics.solana.com:3000/api/datasources/proxy/uid/HsKEnOt4z/query"
)


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
                # Calculate true wait time
                wait_time = self.interval - elapsed_time
                self.logger.info(
                    f"Rate limit exceeded. Waiting for {wait_time} seconds."
                )
                time.sleep(1)

        self.last_request_time = time.time()


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
    msg = f"Error: Could not connect to cluster {endpoint} after 10 attempts. Script exited."
    logger.error(msg)
    exit(-1)


def get_slot_leader(slot, url):
    headers = {
        "Content-Type": "application/json",
    }
    data = {"jsonrpc": "2.0", "id": 1, "method": "getSlotLeaders", "params": [slot, 1]}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        data = response.json()
        return data["result"][0]
    else:
        return None


def fetch_next_leader_replay_time(slot, url, db):
    next_slot_leader = get_slot_leader(slot + 4, url)
    replay_time = 0
    if next_slot_leader != None:
        # Replay Slot
        query = f"""SELECT time,slot,replay_time,replay_total_elapsed FROM "autogen"."replay-slot-stats" WHERE "host_id"='{next_slot_leader}' AND slot={slot} ORDER BY time ASC LIMIT 10"""
        encoded_query = quote(query)

        url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            if results:
                for result in results:
                    series = result.get("series", [])
                    for s in series:
                        for value in s["values"]:
                            replay_time = value[3] / 1000
            else:
                logger.info("No results found.")
        else:
            logger.info(f"Failed to retrieve data. Status code: {response.status_code}")
    return replay_time


def fetch_leader_bank_time(slot, url, db, leader_identity):
    # Banking

    query = f"""SELECT * FROM "autogen"."leader-slot-start-to-cleared-elapsed-ms" WHERE "host_id"='{leader_identity}' AND slot={slot} ORDER BY time ASC LIMIT 30"""
    encoded_query = quote(query)
    leader_time = 0
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])
        if results:
            for result in results:
                series = result.get("series", [])
                for s in series:
                    for value in s["values"]:
                        leader_time = value[1]
        else:
            logger.info("No results found.")
    else:
        logger.info(f"Failed to retrieve data. Status code: {response.status_code}")
    return leader_time


def get_current_slot(url):
    headers = {
        "Content-Type": "application/json",
    }
    data = {"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response


def get_block_for_slot(rpc_url, slot, rpc_client: Client, db, leader_identity):
    total_txn = 0
    total_vote_txn = 0
    total_cu = 0
    block_reward = 0
    leader_bank_time_ms = 0
    replay_time_ms = 0

    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        try:
            logger.info(f"Calculating block reward for slot {slot}")
            rate_limiter.check_rate_limit()
            resp = rpc_client.get_block(slot, max_supported_transaction_version=0)

            if resp is None or resp.value is None:
                logger.info(f"Slot {slot} was skipped or missing. Returning None.")
                break

            total_txn = len(resp.value.transactions)
            block_reward += resp.value.rewards[0].lamports

            for each_txn in resp.value.transactions:
                total_cu += each_txn.meta.compute_units_consumed
                for each_acc in each_txn.transaction.message.account_keys:
                    if str(each_acc) == "Vote111111111111111111111111111111111111111":
                        total_vote_txn += 1

            leader_bank_time_ms = fetch_leader_bank_time(
                slot, rpc_url, db, leader_identity
            )
            replay_time_ms = fetch_next_leader_replay_time(slot, rpc_url, db)
            break
        except Exception as e:
            attempt += 1
            logger.error(f"Error in RPC for slot {slot}, retry attempt {attempt}: {e}")

    return {
        "slot": slot,
        "total_txn": total_txn,
        "total_vote_txn": total_vote_txn,
        "other_txns": total_txn - total_vote_txn,
        "total_cu": total_cu,
        "cu_x": round(total_cu / 48000000, 2),
        "leader_time_ms": round(leader_bank_time_ms, 2),
        "replay_time_ms": round(replay_time_ms, 2),
        "block_rewards": round(block_reward, 2),
        "block_rewards_sol": round(block_reward / 1000000000, 3),
    }


def process_slots(args, db):
    try:
        rpc_client = connect_rpc_client(args.rpc_url, rate_limiter)
        slots = [args.start_slot + i for i in range(args.count)]
        logger.info(f"{rpc_client.get_slot().value}")
        # leader_identity = rpc_client.get_slot_leaders(int(args.start_slot)) # method not supported in lib(solana) need to query using post
        leader_identity = get_slot_leader(int(args.start_slot), args.rpc_url)
        logger.info(f"Leader identity at slot {args.start_slot} is {leader_identity}")
        with open(f"metrics.csv", mode="w", newline="") as file:
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
            for each_slot in slots:
                logger.info(f"processing slot {each_slot}")
                result = get_block_for_slot(
                    args.rpc_url, each_slot, rpc_client, db, leader_identity
                )
                writer.writerow(result)
                time.sleep(2)
    except Exception as e:
        msg = f"Error: error block metrics calculation {e}"
        logger.info(msg)


def main():
    parser = argparse.ArgumentParser(
        description="Calculates and reports various metrics for any given block"
    )
    parser.add_argument(
        "--cluster",
        type=str,
        default="m",
        help="Cluster m=mainnet, t=testnet",
        required=False,
    )
    parser.add_argument(
        "--rpc_url",
        type=str,
        default="https://api.mainnet-beta.solana.com",
        help="RPC URL for query data from chain",
    )
    parser.add_argument(
        "--start_slot", type=int, help="The starting slot to process.", required=True
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Number of slots to process starting from  `--start-slot`, default is 4.",
    )
    parser.add_argument(
        "--req_per_sec",
        type=int,
        default=20,
        help="Number of RPC requests per second (default: 20)",
    )
    return parser.parse_args()


if __name__ == "__main__":

    args = main()

    # Logging Setup
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_file_name = f"{time.strftime('%Y-%m-%d%H-%M-%S')}.log"
    handler = logging.FileHandler(log_file_name)
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [Line:%(lineno)d] %(message)s")
    )
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    rate_limiter = RateLimiter(int(args.req_per_sec), logger=logger)

    logger.info(
        f"Getting (cluster = {args.cluster}) data about block from {args.start_slot} to {args.start_slot + args.count - 1}"
    )

    logger.info(f"Cluster: {args.cluster}")
    logger.info(f"RPC URL: {args.rpc_url}")

    if args.cluster == "m":
        process_slots(args, "mainnet-beta")
    elif args.cluster == "t":
        process_slots(args, "tds")
    else:
        logger.info("invalid cluster type {use m=mainnet, t=testnet}")
