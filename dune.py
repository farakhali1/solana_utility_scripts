import pandas as pd
import subprocess
from io import StringIO

api_key = ""
query_id = "4002940"
start = 284631965
end = 284776195
chunk_size = 7000
csv_filename = "dune_query_results_combined.csv"
first_chunk = True
# offset=0

for offset in range(0, 7000000000000, chunk_size):
    curl_command = [
        "curl",
        "-H", f"X-Dune-API-Key:{api_key}",
        f"https://api.dune.com/api/v1/query/{query_id}/results/csv?allow_partial_results=true&limit={chunk_size}&offset={offset}"
    ]

    result = subprocess.run(curl_command, capture_output=True, text=True)

    if result.stdout.strip():  # Check if the output is not empty
        df = pd.read_csv(StringIO(result.stdout))

        if first_chunk:
            df.to_csv(csv_filename, index=False, mode='w')
            first_chunk = False
        else:
            df.to_csv(csv_filename, index=False, mode='a', header=False)

        print(f"Appended results to {csv_filename} (offset: {offset})")
    else:
        print(f"No data returned for offset {offset}")
    # break

print("All data has been written to the CSV file.")
