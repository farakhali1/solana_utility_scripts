import pandas as pd
import subprocess
import time
from io import StringIO
import math

api_key = ""
query_id = "4039439"
chunk_size = 250000
max_entries = 584902 
iterations = math.ceil(max_entries / chunk_size)
csv_filename = "285552001-285553999.csv"
first_chunk = True
end_index = chunk_size * iterations

for offset in range(0, end_index, chunk_size):
    time.sleep(2)
    curl_command = [
        "curl",
        "-H", f"X-Dune-API-Key:{api_key}",
        f"https://api.dune.com/api/v1/query/{query_id}/results/csv?allow_partial_results=true&limit={chunk_size}&offset={offset}"
    ]
    result = subprocess.run(curl_command, capture_output=True, text=True)

    if result.stdout.strip(): 
        df = pd.read_csv(StringIO(result.stdout))
        df.to_csv(csv_filename, index=False, mode='w' if first_chunk else 'a', header=first_chunk)
        first_chunk = False

        print(f"Appended results to {csv_filename} (offset: {offset})")
    else:
        print(f"No data returned for offset {offset}")

print("All data has been written to the CSV file.")