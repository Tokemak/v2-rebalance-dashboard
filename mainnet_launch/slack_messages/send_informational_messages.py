from mainnet_launch.slack_messages.concentration.high_pool_exposure import (
    post_ownership_exposure_message,
)

# from mainnet_launch.slack_messages.incentives.vault_claimed_reward_events import
#     post_vault_claimed_reward_events_message,
# )

from mainnet_launch.slack_messages.solver.solver_plans_and_events import post_autopools_without_generated_plans


def post_all_information_messages():
    post_ownership_exposure_message(percent_cutoff=50.0)
    post_autopools_without_generated_plans()


if __name__ == "__main__":
    from mainnet_launch.constants import profile_function

    profile_function(post_all_information_messages)
