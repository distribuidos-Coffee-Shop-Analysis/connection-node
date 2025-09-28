# Exchange constants for RabbitMQ exchanges
TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE = (
    "transactions_and_transaction_items_exchange"
)
MENU_ITEMS_EXCHANGE = "menu_items_exchange"
USERS_EXCHANGE = "users_exchange"
STORES_EXCHANGE = "stores_exchange"

# List of all exchanges to be declared
REQUIRED_EXCHANGES = [
    TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
    MENU_ITEMS_EXCHANGE,
    USERS_EXCHANGE,
    STORES_EXCHANGE,
]
