import requests
import statistics

"""
Jito and Helius Validator APY Calculation
=========================================

This module provides functions to calculate the APY for Jito overall and a given validator. 
It includes methods for calculating both the overall network APY and the APY for a specific validator.

Steps to Calculate Jito Overall APY (Stakewiz Method)
-----------------------------------------------------

To calculate the APY for the entire Jito network, the following formula is used:

    APY = ((1 + (mev_reward / active_stake)) ^ epoch_per_year) - 1

Where:
- mev_reward (float): Total MEV earned in the last epoch.
- active_stake (float): Total active stake for Jito in the last epoch.
- epoch_per_year (int): The number of epochs in a year. Typically, there are 182 epochs in a year (approx. 177 base epochs).

Steps to Calculate APY for a Specific Validator
------------------------------------------------

To calculate the APY for a specific validator, use the following formula:

    APY = ((1 + (mev_reward / active_stake)) ^ epoch_per_year) - 1

Where:
- mev_reward (float): Check the balance of the validator’s Tip Distribution Account (TDA) at the last slot of the epoch.
- active_stake (float): Total active stake on the validator in the given epoch.
- epoch_per_year (int): Typically 182 epochs per year (approx. 177 base epochs).

--------------------------------

To obtain a stable APY estimation, the median value of the last 10 epochs is used. This helps smooth out any variations.

Tip Distribution Account (TDA)
------------------------------
Each validator creates its own Tip Distribution Account (TDA) every epoch. The TDA is used to receive all MEV tips for the respective epoch.

Sources:
    # Specific Epoch Rewards: https://jito-foundation.gitbook.io/mev/jito-solana/data-tracking/tracking-mev-rewards#specific-epoch-rewards-all-validators
    # Network MEV Stats: https://jito-foundation.gitbook.io/mev/jito-solana/data-tracking/tracking-mev-rewards#network-mev-stats
"""


# Constants
URL = "https://kobe.mainnet.jito.network/api/v1"
TARGET_VOTE_ACCOUNT = "he1iusunGwqrNtafDtLdhsUQDFvo13z9sUa36PauBtk"
HEADERS = {"Content-Type": "application/json"}
EPOCHS_PER_YEAR = 177


# Helper functions
def fetch_data(endpoint, method="GET", payload=None):
    url = f"{URL}/{endpoint}"
    response = requests.request(method, url, json=payload, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Failed to fetch data from {url}: {response.status_code}, {response.text}"
        )


def get_current_epoch():
    data = fetch_data("mev_rewards")
    return data.get("epoch")


def get_mev_rewards_for_epochs(start_epoch, end_epoch):
    rewards = []
    for epoch in range(start_epoch, end_epoch):
        payload = {"epoch": epoch}
        data = fetch_data("mev_rewards", method="POST", payload=payload)
        reward = data.get("mev_reward_per_lamport")
        if reward is not None:
            rewards.append(reward)
    return rewards


def get_validator_data_for_epochs(start_epoch, end_epoch):
    validator_info = {}
    for epoch in range(start_epoch, end_epoch):
        payload = {"epoch": epoch}
        data = fetch_data("validators", method="POST", payload=payload)
        validators = data.get("validators", [])
        for validator in validators:
            if validator.get("vote_account") == TARGET_VOTE_ACCOUNT:
                active_stake = validator.get("active_stake", 0)
                mev_rewards = validator.get("mev_rewards", 0)
                mev_commission_bps = validator.get("mev_commission_bps", 0)
                if TARGET_VOTE_ACCOUNT not in validator_info:
                    validator_info[TARGET_VOTE_ACCOUNT] = []

                apy = (
                    round(
                        (((1 + (mev_rewards / active_stake)) ** EPOCHS_PER_YEAR) - 1), 4
                    )
                    if active_stake > 0
                    else 0
                )
                true_apy = 0

                if mev_commission_bps != 100 and active_stake > 0:
                    stakers_reward = mev_rewards - (
                        mev_rewards * mev_commission_bps / 10000
                    )
                    true_apy = round(
                        (
                            ((1 + (stakers_reward / active_stake)) ** EPOCHS_PER_YEAR)
                            - 1
                        ),
                        4,
                    )

                validator_info[TARGET_VOTE_ACCOUNT].append(
                    (
                        epoch,
                        active_stake,
                        mev_rewards,
                        mev_commission_bps,
                        apy,
                        true_apy,
                    )
                )
    return validator_info


def calculate_jito_apy(current_epoch):
    start_epoch = current_epoch - 10
    mev_rewards = get_mev_rewards_for_epochs(start_epoch, current_epoch)

    if mev_rewards:
        median_reward = round(statistics.median(mev_rewards), 5)
        apy = ((1 + median_reward) ** EPOCHS_PER_YEAR) - 1
        return apy
    else:
        print("No MEV rewards data available for the last 10 epochs.")


def store_validator_info(current_epoch):
    start_epoch = current_epoch - 10
    validator_info = get_validator_data_for_epochs(start_epoch, current_epoch)
    jito_overall_apy = calculate_jito_apy(current_epoch)
    if TARGET_VOTE_ACCOUNT in validator_info:
        median_apy = statistics.median(
            entry[4] for entry in validator_info[TARGET_VOTE_ACCOUNT]
        )
        median_true_apy = statistics.median(
            entry[5] for entry in validator_info[TARGET_VOTE_ACCOUNT]
        )
        print(f"Validator: {TARGET_VOTE_ACCOUNT}")
        print(f"Current Epoch: {current_epoch}")
        print(f"Validator APY: {round(median_apy * 100, 4)}%")
        print(f"Validator True APY: {round(median_true_apy * 100, 4)}%")
        print(f"Jito Overall APY: {round(jito_overall_apy * 100, 4)}%")

        print("Detail Info:")
        print(f"\tEpoch\tActive Stake\tMEV Rewards\tMEV Commission\tAPY\tTrue APY")
        for record in validator_info[TARGET_VOTE_ACCOUNT]:
            epoch, active_stake, mev_rewards, mev_commission_bps, apy, true_apy = record
            print(
                f"\t{epoch}\t{active_stake}\t{mev_rewards}\t{mev_commission_bps}\t\t{round(apy * 100, 4)}%\t{round(true_apy * 100, 4)}"
            )
    else:
        print(f"No data found for vote account {TARGET_VOTE_ACCOUNT}.")


try:
    current_epoch = get_current_epoch()

    store_validator_info(current_epoch)
    calculate_jito_apy(current_epoch)

except Exception as e:
    print(f"Error: {e}")
