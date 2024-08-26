# solana_utility_scripts

A comprehensive collection of essential scripts for automating, managing, and optimizing your Solana workflows.

## Build Dependencies

```bash
pip install -r requirements.txt
```

## Validator Block Rewards Calculator

This script [get_epoch_block_rewards.py](./get_epoch_block_rewards.py) calculates the block rewards for a specific validator in a given epoch (default: current epoch). Retrieves the leader slots for the validator, and calculates the total rewards for those slots.

### How to run

```bash
python3 get_epoch_block_rewards.py [-h] --identity_pubkey <IDENTITY_PUBKEY> [--rpc_url <RPC_URL>] [--req_per_sec <REQ_PER_SEC>] [--epoch <EPOCH>]
```

## Block Metrics Analyzer

## Overview

The [block_metrics_analyzer.py](./block_metrics_analyzer.py) script is used to analyze various metrics for any given Solana blockchain block. It extracts key performance indicators such as slot, transaction counts, compute units (CUs), and block processing times.

### How to run

```bash
python3 block_metrics_analyzer.py [-h] --start_slot START_SLOT [--cluster CLUSTER] [--rpc_url RPC_URL] [--count COUNT] [--req_per_sec REQ_PER_SEC]
```
