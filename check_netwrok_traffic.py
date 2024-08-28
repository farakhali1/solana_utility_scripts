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
    
    
def get_sigverify_logs(time, url, db, leader_identity):
    # quic
    query1 = f"""SELECT * FROM "autogen"."quic_streamer_tpu_forwards" WHERE "host_id"='{leader_identity}' AND "time">={time} ORDER BY time ASC"""
    encoded_query1 = quote(query1)
    url1 = f"{db_base_url}?db={db}&q={encoded_query1}&epoch=ms"
    response1 = requests.get(url1)
    
    # sigverify
    query2 = f"""SELECT time,host_id,total_packets,total_valid_packets FROM "autogen"."tpu-verifier" WHERE "host_id"='{leader_identity}' AND "time">={time} ORDER BY time ASC"""
    encoded_query2 = quote(query2)
    url2 = f"{db_base_url}?db={db}&q={encoded_query2}&epoch=ms"
    response2 = requests.get(url2)
    
    if response1.status_code == 200 or response2.status_code == 200:
        with open(f'{leader_identity}-.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            if response1.status_code == 200:
                data = response1.json()
                results = data.get("results", [])
                if results:
                    series = results[0].get("series", [])
                    if series:
                        columns = series[0].get("columns", [])
                        csvwriter.writerow(columns)
                        for s in series[0].get("values", []):
                            csvwriter.writerow(s)
                        print("Data successfully written to output.csv")
                    else:
                        print("No series found in results.")
                else:
                    print("No results found.")
            else:
                print(f"Failed to retrieve data. Status code: {response1.status_code}")
                
            if response2.status_code == 200:
                data = response2.json()
                results = data.get("results", [])
                if results:
                    series = results[0].get("series", [])
                    if series:
                        columns = series[0].get("columns", [])
                        csvwriter.writerow(columns)
                        for s in series[0].get("values", []):
                            csvwriter.writerow(s)
                        print("Data successfully written to output.csv")
                    else:
                        print("No series found in results.")
                else:
                    print("No results found.")
            else:
                print(f"Failed to retrieve data. Status code: {response1.status_code}")

def get_cluster_node_pubkeys(rpc_client:Client):
    cluster_nodes = rpc_client.get_cluster_nodes().value
    print(f"Total Nodes {cluster_nodes.__len__()}")
    cluster_nodes_pubkey = []
    for each in cluster_nodes:
        node_json = json.loads(each.to_json()) 
        pubkey = node_json.get("pubkey")
        if pubkey:
            cluster_nodes_pubkey.append(pubkey)
    return cluster_nodes_pubkey
 
def process_slots(args, db):
    try:
        time = 1724506671000000000
        rpc_client = connect_rpc_client(args.rpc_url)
        cluster_node_pubkey = get_cluster_node_pubkeys(rpc_client)
        count=1
        for each_leader in cluster_node_pubkey:
            print(f"Count {count} Nodes Pubkey {each_leader}")
            count+=1
            get_sigverify_logs(
                time, args.rpc_url, db, each_leader, 
            )
        print("Data successfully written to output.csv")
        
    except Exception as e:
        msg = f"Error: error block metrics calculation {e}"
        print(msg)

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
    return parser.parse_args()


if __name__ == "__main__":
    
    args = main()

    # Logging Setup
    if args.cluster == "m":
        process_slots(args,"mainnet-beta")
    elif args.cluster == "t":
        process_slots(args, "tds")
    else:
        print("invalid cluster type {use m=mainnet, t=testnet}")

