import requests
import json
import time
import os
import csv
import pandas as pd
from urllib.parse import quote

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


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_directory_path = (
        f"{current_dir}/network_traffic_{time.strftime('%Y-%m-%d-%H')}"
    )
    setup_directories(output_directory_path)

    start_time = 1725249260000000000
    end_time = 1725437099000000000
    for each_leader in selected_node_pubkeys:
        print(f"Fetching Data for Leader {each_leader}")
        extract_data_from_db(
            "mainnet-beta", output_directory_path, each_leader, start_time, end_time
        )
        print("")
        

if __name__ == "__main__":

    main()
