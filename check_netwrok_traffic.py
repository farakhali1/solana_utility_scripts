import requests
import json
import time
import os
import csv
import pandas as pd
import argparse
from collections import defaultdict
from urllib.parse import quote

from solana.rpc.core import RPCException
from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed

db_base_url = (
    "https://metrics.solana.com:3000/api/datasources/proxy/uid/HsKEnOt4z/query"
)


selected_nodes_pubkeys = [
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


def connect_rpc_client(endpoint: str) -> Client:
    print("Connecting to network at " + endpoint)
    rpc_client = Client(endpoint=endpoint, commitment=Confirmed)
    for attempt in range(10):
        try:
            res = rpc_client.get_slot(commitment=Confirmed).value
            return rpc_client
        except RPCException as e:
            print(f"Error in RPC: {e}")
        time.sleep(2)
    msg = f"Error: Could not connect to cluster {endpoint} after 10 attempts. Script exited."
    print(msg)
    exit(-1)


output_directory = "/home/harkos/solana_utility_scripts/4-sep-network-traffic/"


# Report and reset metrics iff the interval has elapsed and the worker did some work.
def get_centeral_scheduler_banking_logs(start_time, end_time, url, db, leader_identity):
    query = f"""SELECT * FROM "autogen"."banking_stage_worker_counts" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                f"{output_directory}/centeral_scheduler_banking/{leader_identity}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"Series empty for leader {leader_identity}")
            return
    else:
        print(f"Error in response for leader {leader_identity}")
        return
    print(f"Data fetched for leader identity {leader_identity}  | rows {rows}")


def get_multiterator_scanner_banking_logs(
    start_time, end_time, url, db, leader_identity
):
    query = f"""SELECT * FROM "autogen"."banking_stage-leader_slot_packet_counts" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                f"{output_directory}/multiiterator_banking/{leader_identity}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"Series empty for leader {leader_identity}")
            return
    else:
        print(f"Error in response for leader {leader_identity}")
        return
    print(f"Data fetched for leader identity {leader_identity}  | rows {rows}")


# report only when has some data
def get_centeral_scheduler_logs(start_time, end_time, url, db, leader_identity):
    query = f"""SELECT * FROM "autogen"."banking_stage_scheduler_counts" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                f"{output_directory}/scheduler/{leader_identity}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"Series empty for leader {leader_identity}")
            return
    else:
        print(f"Error in response for leader {leader_identity}")
        return
    print(f"Data fetched for leader identity {leader_identity}  | rows {rows}")


# No need to report a datapoint if no batches/packets received
def get_voting_sigverify_logs(start_time, end_time, url, db, leader_identity):
    query = f"""SELECT * FROM "autogen"."quic_streamer_tpu_forwards" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                f"{output_directory}/quic_streamer_tpu_forwards/{leader_identity}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"Series empty for leader {leader_identity}")
            return
    else:
        print(f"Error in response for leader {leader_identity}")
        return
    print(f"Data fetched for leader identity {leader_identity}  | rows {rows}")


# No need to report a datapoint if no batches/packets received
def get_sigverify_logs(start_time, end_time, url, db, leader_identity):
    query = f"""SELECT * FROM "autogen"."tpu-verifier" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND  "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                f"{output_directory}/sigverify/{leader_identity}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"Series empty for leader {leader_identity}")
            return
    else:
        print(f"Error in response for leader {leader_identity}")
        return
    print(f"Data fetched for leader identity {leader_identity}  | rows {rows}")


# report metrics when last report time elapsed time > 5sec
def get_quic_logs(start_time, end_time, url, db, leader_identity):
    query = f"""SELECT * FROM "autogen"."quic_streamer_tpu_forwards" WHERE "host_id"='{leader_identity}' AND "time">={start_time} AND "time" <={end_time} ORDER BY time ASC"""
    encoded_query = quote(query)
    url = f"{db_base_url}?db={db}&q={encoded_query}&epoch=ms"
    response = requests.get(url)
    rows = 0
    if response.status_code == 200:
        series = response.json().get("results", [])[0].get("series", [])
        if series:
            data = series[0].get("values", [])
            rows = len(data)
            columns = series[0].get("columns", [])
            with open(
                f"{output_directory}/quic/{leader_identity}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                csvwriter.writerows(data)
        else:
            print(f"Series empty for leader {leader_identity}")
            return
    else:
        print(f"Error in response for leader {leader_identity}")
        return
    print(f"Data fetched for leader identity {leader_identity}  | rows {rows}")


# f"{output_directory}/quic/{leader_identity}.csv"
def sum_csv_rows_by_host_id(leader_identity, output_file_path):
    input_file = f"{output_directory}/quic_streamer_tpu_forwards/{leader_identity}.csv"
    if os.path.exists(input_file):
        df = pd.read_csv(input_file)
        columns_to_sum = [col for col in df.columns if col != "host_id"]
        result_df = df.groupby("host_id")[columns_to_sum].sum().reset_index()
        file_exists = os.path.exists(output_file_path)
        with open(output_file_path, mode="a", newline="") as output_file:
            result_df.to_csv(output_file, index=False, header=not file_exists)

        print(f"Results successfully written to {output_file_path}")


def get_cluster_node_pubkeys(rpc_client: Client):
    cluster_nodes = rpc_client.get_cluster_nodes().value
    # print(f"Total Nodes {cluster_nodes}")
    cluster_nodes_pubkey = []
    for each in cluster_nodes:
        node_json = json.loads(each.to_json())
        pubkey = node_json.get("pubkey")
        if pubkey:
            cluster_nodes_pubkey.append(pubkey)
    return cluster_nodes_pubkey


def extract_data_from_db(args, db):
    try:
        start_time = 1725249260000000000
        end_time = 1725437099000000000
        # rpc_client = connect_rpc_client(args.rpc_url)
        # cluster_node_pubkey = get_cluster_node_pubkeys(rpc_client)
        # print(f"Total Nodes {cluster_node_pubkey}")
        for each_leader in selected_nodes_pubkeys:
            get_voting_sigverify_logs(
                start_time,
                end_time,
                args.rpc_url,
                db,
                each_leader,
            )
            # sum_csv_rows_by_host_id(
            #     each_leader, f"{output_directory}/quic_streamer_tpu_forwards/combined.csv"
            # )

    except Exception as e:
        msg = f"Error: error block metrics calculation {e}"
        print(msg)


def parse_args():
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
    return parser.parse_args()


def main():
    args = parse_args()

    if args.cluster == "m":
        extract_data_from_db(args, "mainnet-beta")
    elif args.cluster == "t":
        extract_data_from_db(args, "tds")
    else:
        print("invalid cluster type {use m=mainnet, t=testnet}")


if __name__ == "__main__":

    main()
