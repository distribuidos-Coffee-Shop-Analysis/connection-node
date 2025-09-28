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
        # Initialize configuration
        server_config, middleware_config = initialize_config()

        # Initialize logging
        initialize_log(server_config.logging_level)

        # Log config parameters at the beginning of the program to verify the configuration
        # of the component
        logging.debug(
            "action: config | result: success | port: %s | listen_backlog: %s | logging_level: %s",
            server_config.port,
            server_config.listen_backlog,
            server_config.logging_level,
        )

        # Initialize and run server
        server = Server(server_config, middleware_config)
        server.run()

    except KeyError as e:
        print(f"Configuration Error: {e}", file=sys.stderr)
    except ValueError as e:
        print(f"Configuration Parse Error: {e}", file=sys.stderr)
    except KeyboardInterrupt:
        logging.info(
            "action: shutdown | result: in_progress | msg: received keyboard interrupt"
        )
        # The server's signal handler will handle the graceful shutdown
    except Exception as e:
        logging.error("action: server_main | result: fail | error: %s", e)


if __name__ == "__main__":
    main()
