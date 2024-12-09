import requests
from urllib.parse import quote
import json
import csv

db = "tds"
# db = "mainnet-beta"
base_url = "https://metrics.solana.com:3000/api/datasources/proxy/uid/HsKEnOt4z/query"
rpc_url = "https://api.testnet.solana.com"


# Function to get slot leader
def get_slot_leader(slot):
    headers = {"Content-Type": "application/json"}
    data = {"jsonrpc": "2.0", "id": 1, "method": "getSlotLeaders", "params": [slot, 1]}
    response = requests.post(rpc_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        data = response.json()
        return data["result"][0]
    return None


# Function to get leader stats
def get_leader_stats(slot, leader_identity):
    query = f"""SELECT time,elapsed FROM "autogen"."leader-slot-start-to-cleared-elapsed-ms" WHERE "host_id"='{leader_identity}' AND slot={slot}"""
    encoded_query = quote(query)
    url = f"{base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        # print(data)
        results = data.get("results", [])
        if results:
            for result in results:
                series = result.get("series", [])
                for s in series:
                    for value in s["values"]:
                        return value[0], value[1]  # Time in seconds
    return 0, 0


# Function to get replay stats
def get_replay_stats(slot, leader_identity):
    query = f"""SELECT time, replay_total_elapsed, total_transactions, total_entries FROM "autogen"."replay-slot-stats" WHERE "host_id"='{leader_identity}' AND slot={slot}"""
    encoded_query = quote(query)
    url = f"{base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # print(f" leader {leader_identity} {data}")
        results = data.get("results", [])
        if results:
            for result in results:
                series = result.get("series", [])
                for s in series:
                    for value in s["values"]:
                        return value[0], value[1], value[2], value[3]
    return None, None, None, None


# CSV File setup
csv_file = "leader_replay_stats.csv"
csv_headers = [
    "Slot",
    "Leader Identity",
    "Total Transactions",
    "Total Entries",
    "Leader Log Time",
    "Leader Bank Time",
    "Next Leader Replay Log Time",
    "Next Leader Replay Elapsed",
    "Next Leader Overlap",
    "Rakurai Replay Log Time",
    "Rakurai Replay Elapsed",
    "Rakurai Overlap",
]

with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(csv_headers)

    # Processing slots
    start_slot = 305861303
    for slot in range(start_slot, start_slot + 2):
        leader_identity = get_slot_leader(slot)
        print(f"Processing slot {slot}, leader {leader_identity}")

        # Leader stats
        leader_log_time, leader_bank_time = get_leader_stats(slot, leader_identity)

        if leader_log_time != 0 and leader_bank_time != 0:
            next_leader_identity = get_slot_leader(slot + 4)

            # Replay stats for next leader
            next_leader_replay_stats = get_replay_stats(slot, next_leader_identity)
            if next_leader_replay_stats[0] is None:
                next_leader_replay_stats = [None] * 4

            # Replay stats for Rakurai
            rakurai_replay_stats = get_replay_stats(
                slot, "3hLQCguLNPe7XUouGDKqDXjCqSgzWiVzHmionC32f11Q"
            )
            if rakurai_replay_stats[0] is None:
                rakurai_replay_stats = [None] * 4

            # Calculate overlaps
            next_leader_overlap = (
                next_leader_replay_stats[0]
                - leader_log_time
                - (next_leader_replay_stats[1] / 1000)
                if next_leader_replay_stats[0] is not None
                else None
            )
            rakurai_overlap = (
                rakurai_replay_stats[0]
                - leader_log_time
                - (rakurai_replay_stats[1] / 1000)
                if rakurai_replay_stats[0] is not None
                else None
            )

            # Write row to CSV
            result = [
                slot,
                leader_identity,
                rakurai_replay_stats[2],
                rakurai_replay_stats[3],
                leader_log_time,
                leader_bank_time,
                *next_leader_replay_stats[:2],
                next_leader_overlap,
                *rakurai_replay_stats[:2],
                rakurai_overlap,
            ]
            writer.writerow(result)
