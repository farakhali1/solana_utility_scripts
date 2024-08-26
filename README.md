# solana_utility_scripts

A comprehensive collection of essential scripts for automating, managing, and optimizing your Solana workflows.

## Build Dependencies

```bash
pip install -r requirements.txt
```

## Validator Block Rewards Calculator

This script [get_epoch_block_rewards.py](./get_epoch_block_rewards.py) calculates the block rewards for a specific validator in a given epoch (default: current epoch). Retrieves the leader slots for the validator, and calculates the total rewards for those slots.

## How to run

```bash

python3 get_epoch_block_rewards.py --help

python script_name.py --identity_pubkey <IDENTITY_PUBKEY> [--rpc_url <RPC_URL>] [--req_per_sec <REQ_PER_SEC>] [--epoch <EPOCH>]
```
