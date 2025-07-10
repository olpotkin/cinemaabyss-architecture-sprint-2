import random
import os


def should_use_new_service() -> bool:
    gradual = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
    percent = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

    if not gradual:
        return False
    return random.randint(1, 100) <= percent
