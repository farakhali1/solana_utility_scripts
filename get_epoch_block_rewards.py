import argparse
import time
import logging

from solana.rpc.core import RPCException
from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed


class RateLimiter:
    def __init__(self, max_requests_per_second, logger=None):
        self.max_requests_per_second = max_requests_per_second
        self.interval = 1.0 / max_requests_per_second
        self.last_request_time = None
        self.logger = logger or logging.getLogger(__name__)

    def check_rate_limit(self):
        current_time = time.time()

        if self.last_request_time is not None:
            elapsed_time = current_time - self.last_request_time
            if elapsed_time < self.interval:
                # Calculate true wait time
                wait_time = self.interval - elapsed_time
                self.logger.info(
                    f"Rate limit exceeded. Waiting for {wait_time} seconds."
                )
                time.sleep(1)

        self.last_request_time = time.time()


def connect_rpc_client(endpoint: str, rate_limiter: RateLimiter) -> Client:
    logger.info("Connecting to network at " + endpoint)
    rpc_client = Client(endpoint=endpoint, commitment=Confirmed, timeout=30)
    for attempt in range(10):
        rate_limiter.check_rate_limit()
        try:
            res = rpc_client.get_slot(commitment=Confirmed).value
            return rpc_client
        except RPCException as e:
            logger.error(f"Error in RPC: {e}")
        time.sleep(2)
    msg = f"Error: Could not connect to cluster {endpoint} after 10 attempts. Script exited."
    logger.error(msg)
    exit(-1)


def get_first_slot_in_epoch(
    first_normal_epoch, slots_per_epoch, first_normal_slot, epoch
):
    MINIMUM_SLOTS_PER_EPOCH = 32  # Replace with the actual value if different

    if epoch <= first_normal_epoch:
        return (2**epoch - 1) * MINIMUM_SLOTS_PER_EPOCH
    else:
        return (epoch - first_normal_epoch) * slots_per_epoch + first_normal_slot


def get_block_rewards(
    rpc_client: Client,
    leader_slots,
    epoch_info,
    rate_limiter: RateLimiter,
    max_retries=3,
):
    total_fee = 0
    remaining_slots = []

    for each_slot in leader_slots:
        if epoch_info.absolute_slot < each_slot:
            remaining_slots.append(each_slot)
            continue

        attempt = 0
        while attempt < max_retries:
            try:
                logger.info(f"Calculating block reward for slot {each_slot}")
                rate_limiter.check_rate_limit()
                resp = rpc_client.get_block(
                    each_slot, max_supported_transaction_version=0
                )
                if (
                    resp.value.rewards[0].reward_type
                    == resp.value.rewards[0].reward_type.Fee
                ):
                    total_fee += resp.value.rewards[0].lamports
                break
            except Exception as e:
                attempt += 1
                logger.error(
                    f"Error in RPC for slot {each_slot}, retry attempt {attempt} {e}"
                )
                if attempt >= max_retries:
                    remaining_slots.append(each_slot)

    return total_fee, remaining_slots


def get_leader_slots(
    rpc_client: Client, identity_pubkey, epoch, rate_limiter: RateLimiter
):
    rate_limiter.check_rate_limit()
    epoch_schedule = rpc_client.get_epoch_schedule().value
    first_slot = get_first_slot_in_epoch(
        epoch_schedule.first_normal_epoch,
        epoch_schedule.slots_per_epoch,
        epoch_schedule.first_normal_slot,
        epoch,
    )
    logger.info(
        f"Epoch {epoch} first slot {first_slot} expected last slot {first_slot+432000-1}"
    )
    rate_limiter.check_rate_limit()
    leader_schedule = rpc_client.get_leader_schedule(epoch=first_slot).value.items()
    leader_slots_indexs = []
    for each_leader in leader_schedule:
        if each_leader[0].__str__() == identity_pubkey:
            leader_slots_indexs = each_leader[1]
    leader_slots = []
    if leader_slots_indexs != "":
        for item in leader_slots_indexs:
            leader_slots.append(item + first_slot)
    return leader_slots


def transfer_block_reward_to_intermediate_wallet(
    identity_pubkey, rpc_url, rate_limiter, epoch=None
):
    try:
        rpc_client = connect_rpc_client(rpc_url, rate_limiter)
        rate_limiter.check_rate_limit()
        current_epoch = rpc_client.get_epoch_info(commitment=Confirmed).value.epoch
        target_epoch = current_epoch if epoch is None else epoch
        leader_slots = get_leader_slots(
            rpc_client, identity_pubkey, target_epoch, rate_limiter
        )
        logger.info(f"Current Epoch {current_epoch} Target Epoch {target_epoch}")
        if leader_slots != []:
            logger.info(
                f"Validator {identity_pubkey} has {len(leader_slots)} leader slots in epoch {target_epoch} | leader slots {leader_slots}"
            )
            rate_limiter.check_rate_limit()
            epoch_info = rpc_client.get_epoch_info(commitment=Confirmed).value
            total_block_rewards, remaning_slots = get_block_rewards(
                rpc_client, leader_slots, epoch_info, rate_limiter
            )
            if epoch_info.epoch == target_epoch:
                logger.info(
                    f"\n***\nCurrent Epoch {epoch_info.epoch} is not completed yet | Block rewards for produced block/slots are {total_block_rewards} lamports, remaning slots {remaning_slots}\n***\n"
                )
            else:
                logger.info(
                    f"\n***\nTotal block reward for epoch {epoch} with {len(leader_slots)} leader slots is {total_block_rewards} lamports | unable to fetch for slots {remaning_slots}\n***\n"
                )
        else:
            msg = f"WARNING: Validator {identity_pubkey} has no leader slot in {target_epoch} epoch"
            logger.info(msg)
    except Exception as e:
        msg = f"Error: error during block calculation {e}"
        logger.info(msg)


def parse_args():
    # Argument Parsing
    parser = argparse.ArgumentParser(
        description="Calculate Validator Block Rewards for an Epoch"
    )
    parser.add_argument(
        "--req_per_sec",
        type=int,
        default=20,
        help="Number of RPC requests per second (default: 20)",
    )
    parser.add_argument(
        "--identity_pubkey",
        type=str,
        required=True,
        help="Validator Identity pubkey",
    )
    parser.add_argument(
        "--rpc_url",
        type=str,
        default="https://api.mainnet-beta.solana.com",
        help="RPC URL for query data from chain",
    )
    parser.add_argument(
        "--epoch",
        type=int,
        default=None,
        help="Get block Reward for Epoch (default: current Epoch)",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()

    # Logging Setup
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_file_name = f"{time.strftime('%Y-%m-%d%H-%M-%S')}.log"
    handler = logging.FileHandler(log_file_name)
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [Line:%(lineno)d] %(message)s")
    )
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    rate_limiter = RateLimiter(int(args.req_per_sec), logger=logger)

    logger.info(f"Keypair Path: {args.identity_pubkey}")
    logger.info(f"RPC URL: {args.rpc_url}")
    logger.info(f"RPC Rate Limit: {args.req_per_sec}")

    transfer_block_reward_to_intermediate_wallet(
        args.identity_pubkey, args.rpc_url, rate_limiter, epoch=args.epoch
    )
