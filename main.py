#!/usr/bin/env python3

from common.config import initialize_config
from common.health import run_health_server
from common.persistence import SessionManager
from middleware.publisher import RabbitMQPublisher
from server.main import Server
import logging
import os
import sys
import threading


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


def recover_orphaned_sessions(middleware_config):
    """
    Recover orphaned sessions from previous crash.
    Sends cleanup signals for all clients that were connected when the process died.
    """
    try:
        session_manager = SessionManager()
        orphaned_sessions = session_manager.get_active_sessions()
        
        if not orphaned_sessions:
            logging.info("action: recover_orphaned_sessions | result: success | orphaned_count: 0")
            return
        
        logging.info(
            "action: recover_orphaned_sessions | result: start | orphaned_count: %d",
            len(orphaned_sessions)
        )
        
        # Create publisher for sending cleanup signals
        publisher = RabbitMQPublisher(middleware_config)
        
        # Send cleanup signal for each orphaned session
        for client_id in orphaned_sessions:
            logging.info(
                "action: recovering_state | result: in_progress | msg: sending cleanup for orphaned client | client_id: %s",
                client_id
            )
            
            # Send cleanup signal
            success = publisher.send_cleanup_signal(client_id)
            
            if success:
                # Remove session file after successful cleanup signal
                session_manager.remove_session(client_id)
                logging.info(
                    "action: recover_orphaned_session | result: success | client_id: %s",
                    client_id
                )
            else:
                logging.error(
                    "action: recover_orphaned_session | result: fail | client_id: %s",
                    client_id
                )
        
        # Close publisher
        publisher.close()
        
        logging.info(
            "action: recover_orphaned_sessions | result: success | recovered: %d",
            len(orphaned_sessions)
        )
        
    except Exception as e:
        logging.error(
            "action: recover_orphaned_sessions | result: fail | error: %s",
            e
        )


def main():
    try:
        # Initialize configuration
        server_config, middleware_config = initialize_config()

        # Initialize logging
        initialize_log(server_config.logging_level)

        # Start health check server in daemon thread
        health_thread = threading.Thread(
            target=run_health_server,
            args=(12346,),
            daemon=True
        )
        health_thread.start()
        logging.info("action: start_health_thread | result: success | port: 12346")

        # Log config parameters at the beginning of the program to verify the configuration
        # of the component
        logging.debug(
            "action: config | result: success | port: %s | listen_backlog: %s | logging_level: %s",
            server_config.port,
            server_config.listen_backlog,
            server_config.logging_level,
        )

        # Recover orphaned sessions from previous crash
        recover_orphaned_sessions(middleware_config)

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
