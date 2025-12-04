"""
Session persistence manager for tracking active client connections.
Uses file-based storage for atomic operations and crash recovery.
"""
import os
import logging
from typing import List


class SessionManager:
    """
    Manages persistent tracking of active client sessions using file-based storage.
    Each client session is represented by a file in the sessions directory.
    This provides atomic operations at the OS level and survives process crashes.
    """

    def __init__(self, storage_dir="storage/sessions"):
        """
        Initialize the session manager with a storage directory.

        Args:
            storage_dir: Directory path for storing session files
        """
        self.storage_dir = storage_dir
        self.logger = logging.getLogger(__name__)

        # Ensure the storage directory exists
        try:
            os.makedirs(self.storage_dir, exist_ok=True)
            self.logger.info(
                "action: session_manager_init | result: success | storage_dir: %s",
                self.storage_dir,
            )
        except Exception as e:
            self.logger.error(
                "action: session_manager_init | result: fail | error: %s", e
            )
            raise

    def add_session(self, client_id: str) -> bool:
        """
        Add a new client session by creating a file.

        Args:
            client_id: The unique client identifier

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            file_path = os.path.join(self.storage_dir, client_id)
            # Create empty file (like 'touch' command)
            with open(file_path, "a"):
                pass
            self.logger.debug(
                "action: add_session | result: success | client_id: %s", client_id
            )
            return True
        except Exception as e:
            self.logger.error(
                "action: add_session | result: fail | client_id: %s | error: %s",
                client_id,
                e,
            )
            return False

    def remove_session(self, client_id: str) -> bool:
        """
        Remove a client session by deleting the file.
        Safe to call multiple times (idempotent).

        Args:
            client_id: The unique client identifier

        Returns:
            bool: True if successful or file doesn't exist, False on other errors
        """
        try:
            file_path = os.path.join(self.storage_dir, client_id)
            os.remove(file_path)
            self.logger.debug(
                "action: remove_session | result: success | client_id: %s", client_id
            )
            return True
        except FileNotFoundError:
            # File doesn't exist - this is OK (idempotent operation)
            self.logger.debug(
                "action: remove_session | result: already_removed | client_id: %s",
                client_id,
            )
            return True
        except Exception as e:
            self.logger.error(
                "action: remove_session | result: fail | client_id: %s | error: %s",
                client_id,
                e,
            )
            return False

    def get_active_sessions(self) -> List[str]:
        """
        Get a list of all active client sessions.

        Returns:
            List[str]: List of client IDs with active sessions
        """
        try:
            # List all files in the sessions directory
            if not os.path.exists(self.storage_dir):
                return []

            sessions = [
                f
                for f in os.listdir(self.storage_dir)
                if os.path.isfile(os.path.join(self.storage_dir, f))
            ]
            self.logger.debug(
                "action: get_active_sessions | result: success | count: %d",
                len(sessions),
            )
            return sessions
        except Exception as e:
            self.logger.error(
                "action: get_active_sessions | result: fail | error: %s", e
            )
            return []

    def clear_all(self) -> bool:
        """
        Remove all session files (for testing or cleanup).

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not os.path.exists(self.storage_dir):
                return True

            sessions = self.get_active_sessions()
            for session in sessions:
                file_path = os.path.join(self.storage_dir, session)
                try:
                    os.remove(file_path)
                except Exception as e:
                    self.logger.warning(
                        "action: clear_session | result: fail | client_id: %s | error: %s",
                        session,
                        e,
                    )

            self.logger.info(
                "action: clear_all_sessions | result: success | removed: %d",
                len(sessions),
            )
            return True
        except Exception as e:
            self.logger.error("action: clear_all_sessions | result: fail | error: %s", e)
            return False
