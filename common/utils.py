# Exchange constants for RabbitMQ exchanges
import logging
import hashlib


TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE = (
    "transactions_and_transaction_items_exchange"
)
MENU_ITEMS_EXCHANGE = "menu_items_exchange"
USERS_EXCHANGE = "users_exchange"
STORES_EXCHANGE = "stores_exchange"
REPLIES_EXCHANGE = "replies_exchange"


# List of all exchanges to be declared
REQUIRED_EXCHANGES = [
    TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
    MENU_ITEMS_EXCHANGE,
    USERS_EXCHANGE,
    STORES_EXCHANGE,
    REPLIES_EXCHANGE,
]

HEARTBEAT = 600  # 10 minutes

logger = logging.getLogger(__name__)


def get_joiner_partition(user_id, users_joiners_count):
    """
    Calculate the joiner partition for a given user_id using consistent hashing.

    This ensures that the same user_id always maps to the same joiner node,
    which is critical for distributed join operations.

    Args:
        user_id: The user ID to hash (string or int)
        users_joiners_count: Total number of users joiner nodes

    Returns:
        int: The partition number (1 to users_joiners_count)
    """
    user_id_str = str(user_id)

    hash_object = hashlib.sha256(user_id_str.encode("utf-8"))
    hash_int = int(hash_object.hexdigest(), 16)

    return (hash_int % users_joiners_count) + 1


def log_action(action, result, level=logging.INFO, error=None, extra_fields=None):
    """
    Centralized logging function for consistent log format

    Args:
        action: The action being performed
        result: The result of the action (success, fail, etc.)
        level: Logging level (INFO, ERROR, DEBUG, etc.)
        error: Optional error information
        extra_fields: Optional dict with additional fields to log (e.g., client, etc.)
    """
    log_parts = [
        f"action: {action}",
        f"result: {result}",
    ]

    if error:
        log_parts.append(f"error: {error}")

    if extra_fields:
        for key, value in extra_fields.items():
            log_parts.append(f"{key}: {value}")

    log_message = " | ".join(log_parts)
    logger.log(level, log_message)
