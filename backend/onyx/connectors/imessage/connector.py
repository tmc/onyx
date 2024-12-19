"""
iMessage connector implementation.
Provides access to iMessage conversations through macOS chat.db database.
"""
import os
from datetime import datetime
import sqlite3
from typing import Any, Iterator

from onyx.connectors.interfaces import LoadConnector, PollConnector
from onyx.connectors.models import ConnectorMissingCredentialError, Document, Section
from onyx.configs.constants import DocumentSource
from onyx.connectors.imessage.utils import create_document_from_chat


class IMessageConnector(LoadConnector, PollConnector):
    def __init__(self) -> None:
        self.db_path: str | None = None
        self.conn: sqlite3.Connection | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        self.db_path = os.path.expanduser(
            credentials.get("db_path", "~/Library/Messages/chat.db")
        )

        if not os.path.exists(self.db_path):
            raise ConnectorMissingCredentialError("iMessage")

        return credentials

    def _load_messages(
        self, start_time: float | None = None, end_time: float | None = None
    ) -> Iterator[Document]:  # Updated return type annotation
        if not self.db_path:
            return

        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()

            query = """
            SELECT
                chat.ROWID as chat_id,
                chat.chat_identifier,
                message.ROWID,
                message.text,
                message.attributedBody,
                message.date,
                message.is_from_me,
                handle.id as sender
            FROM chat
            JOIN chat_message_join ON chat.ROWID = chat_message_join.chat_id
            JOIN message ON chat_message_join.message_id = message.ROWID
            LEFT JOIN handle ON message.handle_id = handle.ROWID
            """

            conditions = []
            params = []
            if start_time is not None:
                conditions.append("message.date >= ?")
                params.append(int(start_time * 1e9))
            if end_time is not None:
                conditions.append("message.date <= ?")
                params.append(int(end_time * 1e9))

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY chat.ROWID, message.date"
            cursor.execute(query, params)

            current_chat_id = None
            current_chat_messages = []
            current_chat_data = {}

            for row in cursor.fetchall():
                chat_id, chat_identifier, msg_id, text, body, date, is_from_me, sender = row

                if current_chat_id is None:
                    current_chat_id = chat_id
                    current_chat_data = {"chat_identifier": chat_identifier}

                if current_chat_id != chat_id:
                    if current_chat_messages:
                        yield create_document_from_chat(current_chat_data, current_chat_messages)

                    current_chat_id = chat_id
                    current_chat_data = {"chat_identifier": chat_identifier}
                    current_chat_messages = []

                current_chat_messages.append({
                    "text": text,
                    "attributedBody": body,
                    "date": date,
                    "is_from_me": bool(is_from_me),
                    "sender": sender
                })

            if current_chat_messages:
                yield create_document_from_chat(current_chat_data, current_chat_messages)

        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def load_from_state(self) -> Iterator[Document]:  # Updated return type annotation
        yield from self._load_messages()

    def poll_source(self, start: float, end: float) -> Iterator[Document]:  # Updated return type annotation
        yield from self._load_messages(start_time=start, end_time=end)
