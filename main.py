#!/usr/bin/env python3

from common.config import initialize_config
from server.main import Server
import logging
import os
import sys


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    try:
        server_config, middleware_config = initialize_config()
    except KeyError as e:
        print(f"Configuration Error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Configuration Parse Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(
        f"action: config | result: success | port: {server_config.port} | "
        f"listen_backlog: {server_config.listen_backlog} | logging_level: {server_config.logging_level}"
    )

    # Initialize server with configuration structs
    server = Server(server_config, middleware_config)

    try:
        server.run()
    except KeyboardInterrupt:
        logging.info(
            "action: shutdown | result: in_progress | msg: received keyboard interrupt"
        )
        # The server's signal handler will handle the graceful shutdown
    except Exception as e:
        logging.error(f"action: server_main | result: fail | error: {e}")


if __name__ == "__main__":
    main()
