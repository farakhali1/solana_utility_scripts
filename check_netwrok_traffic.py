import requests
import time
import os
import csv
import argparse
import pandas as pd
from urllib.parse import quote


from solana.rpc.core import RPCException
from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed

db_base_url = (
    "https://metrics.solana.com:3000/api/datasources/proxy/uid/HsKEnOt4z/query"
)

# tables name
db_tables_names = {
    "quic": "quic_streamer_tpu",
    "sigverify": "tpu-verifier",
    "sigverify_vote": "tpu-vote-verifier",
    "quic_forwards": "quic_streamer_tpu_forwards",
    "centeral_scheduler": "banking_stage_scheduler_counts",
    "centeral_scheduler_banking": "banking_stage_worker_counts",
    "multi_iterator_banking": "banking_stage-leader_slot_packet_counts",
}

# selected Node Pubkeys
selected_node_pubkeys = [
    "7Zm1pE4FubFYZDyAQ5Labh3A4cxDcvve1s3WCRgEAZ84",
    "Awes4Tr6TX8JDzEhCZY2QVNimT6iD1zWHzf1vNyGvpLM",
    "Hz5aLvpKScNWoe9YZWxBLrQA3qzHJivBGtfciMekk8m5",
    "5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm",
    "EkvdKhULbMFqjKBKotAzGi3kwMvMpYNDKJXXQQmi6C1f",
    "Ninja1spj6n9t5hVYgF3PdnYz2PLnkt7rvaw3firmjs",
    "JupRhwjrF5fAcs6dFhLH59r3TJFvbcyLP2NRM8UGH9H",
    "FzQqaDStQQHs52YKeCnDovwSqvyZBCgs2kJcmvoFZwaS",
    "BUv44cVtsdvU9z2BfFGk6s5JZZWrmVnq5qCaii5ARyyB",
    "AEHqTB2RtJjegsR2ePjvoJSm6AA5pnYKWVbcsn6kqTBD",
    "5ejbTALcBsKQ7Cj1iSuu2mY5jqbYHqh9gF5ERXLiYj1z",
    "Aw5wEMXhbygFLR7jHtHpih8QvxVBGAMTqsQ2SjWPk1ex",
    "C1ocKDYMCm2ooWptMMnpd5VEB2Nx4UMJgRuYofysyzcA",
    "G2TBEh2ahNGS9tGnuBNyDduNjyfUtGhMcssgRb8b6KfH",
    "FbYX2uN573G5WsgiPdHU6fS5PNUyjdXfGfpZNkYUuT4k",
    "HnfPZDrbJFooiP9vvgWrjx3baXVNAZCgisT58gyMCgML",
    "Certusm1sa411sMpV9FPqU5dXAYhmmhygvxJ23S6hJ24",
    "DDnAqxJVFo2GVTujibHt5cjevHMSE9bo8HJaydHoshdp",
    "PUmpKiNnSVAZ3w4KaFX6jKSjXUNHFShGkXbERo54xjb",
    "AicQr2zCWBLiBwt2r6o7iTemmtyE7q5pTKyuuupbXEQA",
    "Frog1Fks1AVN8ywFH3HTFeYojq6LQqoEPzgQFx2Kz5Ch",
    "3hkPdLyQReJwdWe7Y8JL7jRhYaBCC8GZcmWwjpiLXC9f",
    "HzrEstnLfzsijhaD6z5frkSE2vWZEH5EUfn3bU9swo1f",
    "6aDs9tUm2gErcPn2c1TZnp5cu2bQV9BzyuwW4baWQYd4",
    "6y7V8dL673XFzm9QyC5vvh3itWkp7wztahBd2yDqsyrK",
    "CAo1dCGYrB6NhHh5xb1cGjUiu86iyCfMTENxgHumSve4",
    "BSVckjdW2f8kcXPGcrPPtV9kUDBZ8w8PjrrGVnxgEdwq",
    "6TkKqq15wXjqEjNg9zqTKADwuVATR9dW3rkNnsYme1ea",
    "HpcB5Qg8Y9E73dUkot5e8HkgAJbExsYeUzniY4bCuKac",
    "GYx8kpp7SsRwtQEEsGQjAxb4hFMMmT91kFJuDeky3YGQ",
    "5Us18hLZPXJTS4QVuGSsUw137Dyd2tgBaem24Xsf5nBS",
    "LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc",
    "Fc6NNdS2j3EmrWbU6Uqt6wsKB5ef72NjaWfNxKYbULGD",
    "TiMxX1yasS4CiGyRcnn7sy9T2fvaNdFpkf8tFDhhDkG",
    "GREEDkgav1ox1jYyd9Anv6exLqKV2vYnxMw5prGwmNKc",
    "AaapDdocMdZQaMAF1gXqKX2ixd7YYSxTpKHMcsbcF318",
    "HH5dA42XF1HxNk1TRpG6LuKfLViMYNdAz5iWrFM4hWFi",
    "9pBHfuE19q7PRbupJf8CZAMwv6RHjasdyMN9U9du7Nx2v",
    "1KXvrkPXwkGF6NK1zyzVuJqbXfpenPVPP6hoiK9bsK3",
    "CBUGET5PnvLc3HvEeFYj64iTvdKhYV6pujTPDdDh785K",
    "spcti6GQVvinbtHU9UAkbXhjTcBJaba1NVx4tmK4M5F",
    "Cu9Ls6dsTL6cxFHZdStHwVSh1uy2ynXz8qPJMS5FRq86",
    "FphFJA451qptiGyCeCN3xvrDi8cApGAnyR5vw2KxxQ1q",
    "4VrjyXQT61WFSjuG3ehgqZUK1jqvYqB46veQbXLotq3n",
    "3V2xaccDpFib4DbTksdiveNDmiwpXBqSWyjSof3w1Bg7",
    "8JpfpVyew5Y9cLQCHkt5gqT4vDZLL46ZknMbSThVjzrg",
    "2gDeeRa3mwPPtw1CMWPkEhRWo9v5izNBBfEXanr8uibX",
    "Ed9WjPnZfAXsPttcqxMwj94qsuXVRyBsyXnDkxFva2Zv",
]


def sum_csv_rows_by_host_id(input_file, output_file_path):
    df = pd.read_csv(input_file)
    columns_to_sum = [col for col in df.columns if col != "host_id"]
    result_df = df.groupby("host_id")[columns_to_sum].sum().reset_index()
    file_exists = os.path.exists(output_file_path)
    with open(output_file_path, mode="a", newline="") as output_file:
        result_df.to_csv(output_file, index=False, header=not file_exists)


def get_data_from_table(
    db_name, table_name, leader_identity, start_time, end_time, output_file_path
):
    query = f"""SELECT * FROM "autogen"."{table_name}" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db_name}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    # rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            # rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                output_file_path,
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"No {table_name} data for leader {leader_identity}")
            return
    else:
        print(f"Error in getting {table_name} data for leader {leader_identity}")
        return
    print(f"\t- '{table_name}' data fetched")


def extract_data_from_db(
    db_name, directory_path, leader_identity, start_time, end_time
):
    try:
        for table_key, table_name in db_tables_names.items():
            output_file = f"{directory_path}/{table_key}/{leader_identity}.csv"
            get_data_from_table(
                db_name, table_name, leader_identity, start_time, end_time, output_file
            )
            sum_csv_rows_by_host_id(
                output_file,
                f"{directory_path}/{table_key}/summarized_data_by_identity.csv",
            )

    except Exception as e:
        msg = f"Error: Error in extracting data from metrics DB | Error: {e}"
        print(msg)


def setup_directories(output_directory_path):
    try:
        os.makedirs(output_directory_path)
    except Exception as e:
        print(f"Directory already Exist {e}")
        exit(-1)
    for each_table in db_tables_names:
        os.makedirs(f"{output_directory_path}/{each_table}")


def connect_rpc_client(endpoint: str) -> Client:
    print("Connecting to network at " + endpoint)
    rpc_client = Client(endpoint=endpoint, commitment=Confirmed, timeout=30)
    for attempt in range(10):
        try:
            res = rpc_client.get_slot(commitment=Confirmed).value
            return rpc_client
        except RPCException as e:
            print(f"Error in RPC: {e}")
        time.sleep(2)
    print(
        f"Error: Could not connect to cluster {endpoint} after 10 attempts. Script exited."
    )
    exit(-1)


def parse_args():
    # Argument Parsing
    parser = argparse.ArgumentParser(
        description="Extract Selected Validators logs to Analyse Network Traffic Demand"
    )
    parser.add_argument(
        "--rpc_url",
        type=str,
        default="https://api.mainnet-beta.solana.com",
        help="RPC URL for query Epoch info from chain",
    )
    parser.add_argument(
        "--epoch",
        type=int,
        default=None,
        help="Get Network Traffic Stats for given Epoch (default: current_epoch - 1)",
    )
    args = parser.parse_args()
    return args


def get_first_slot_in_epoch(
    first_normal_epoch, slots_per_epoch, first_normal_slot, epoch
):
    MINIMUM_SLOTS_PER_EPOCH = 32
    if epoch <= first_normal_epoch:
        return (2**epoch - 1) * MINIMUM_SLOTS_PER_EPOCH
    else:
        return (epoch - first_normal_epoch) * slots_per_epoch + first_normal_slot


def get_slot_time(rpc_client: Client, slot):
    max_retries = 5
    attempt = 0
    time = None
    while attempt < max_retries:
        try:
            time = rpc_client.get_block_time(slot).value
            break
        except Exception as e:
            attempt += 1
    return time


def get_epoch_start_end_time(args):
    rpc_client = connect_rpc_client(args.rpc_url)
    try:
        epoch_info = rpc_client.get_epoch_info(commitment=Confirmed).value
    except Exception as e:
        print("Unable to get epoch info")
        exit(-1)
    target_epoch = epoch_info.epoch if args.epoch is None else args.epoch
    if target_epoch > epoch_info.epoch:
        print(f"Invalid Epoch {args.epoch} Specified current epoch {target_epoch}")
        exit(-1)
    try:
        epoch_schedule = rpc_client.get_epoch_schedule().value
    except Exception as e:
        print("Unable to get epoch schedule info")
        exit(-1)

    first_slot = get_first_slot_in_epoch(
        epoch_schedule.first_normal_epoch,
        epoch_schedule.slots_per_epoch,
        epoch_schedule.first_normal_slot,
        target_epoch,
    )
    start_time = get_slot_time(rpc_client, first_slot)
    end_time = get_slot_time(rpc_client, (first_slot + 431999))

    if start_time is None:
        print("Unable to fetch Epoch Start Time")
        exit(-1)
    if end_time is None:
        print("Unable to fetch Epoch End Time")
        exit(-1)
    print(
        f"Getting Network Traffic Stats for EPoch {target_epoch} | First Slot {first_slot} Start Time {start_time} last slot {(first_slot+431999)} end time {end_time}"
    )
    return (start_time * 1000000000, end_time * 1000000000)


def main():
    args = parse_args()
    epoch_start_time, epoch_end_time = get_epoch_start_end_time(args)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_directory_path = (
        f"{current_dir}/network_traffic_{time.strftime('%Y-%m-%d-%H')}"
    )
    setup_directories(output_directory_path)

    for each_leader in selected_node_pubkeys:
        print(f"Fetching Data for Leader {each_leader}")
        extract_data_from_db(
            "mainnet-beta",
            output_directory_path,
            each_leader,
            epoch_start_time,
            epoch_end_time,
        )
        print("")


if __name__ == "__main__":

    main()
