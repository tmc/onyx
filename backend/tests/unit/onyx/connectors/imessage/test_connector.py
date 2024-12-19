import os
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from onyx.configs.constants import DocumentSource
from onyx.connectors.imessage.connector import IMessageConnector
from onyx.connectors.models import Document

from tests.unit.onyx.connectors.imessage.consts_and_utils import MOCK_MESSAGE_DATA, create_mock_chat_db

def test_message_to_document():
    """Test that messages are correctly converted to documents."""
    with patch('sqlite3.connect') as mock_connect:
        # Setup mock cursor with properly joined data
        mock_cursor = MagicMock()
        mock_cursor.execute = MagicMock()

        # Create joined mock data matching the SQL query format
        joined_data = []
        for msg in MOCK_MESSAGE_DATA["message"]:
            chat = MOCK_MESSAGE_DATA["chat"][0]  # We have one chat
            handle = MOCK_MESSAGE_DATA["handle"][0]  # We have one handle
            joined_data.append((
                chat["ROWID"],  # chat_id
                chat["chat_identifier"],
                msg["ROWID"],
                msg["text"],
                msg["attributedBody"],
                msg["date"],
                msg["is_from_me"],
                handle["id"]  # sender
            ))

        mock_cursor.fetchall.return_value = joined_data
        mock_connect.return_value.cursor.return_value = mock_cursor

        connector = IMessageConnector()
        connector.db_path = "mock_path"  # Set db_path to avoid None check
        docs = list(connector.load_from_state())

        assert len(docs) == 1  # We expect one document per chat
        doc = docs[0]

        # Verify document structure
        assert isinstance(doc, Document)
        assert doc.source == DocumentSource.IMESSAGE
        assert "chat123" in doc.semantic_identifier
        assert "Hello world" in str(doc.sections[0].text)
        assert "How are you?" in str(doc.sections[1].text)

def test_time_filter():
    """Test that time-based filtering works correctly."""
    with patch('sqlite3.connect') as mock_connect:
        # Setup mock cursor with properly joined data
        mock_cursor = MagicMock()
        mock_cursor.execute = MagicMock()

        # Create joined mock data matching the SQL query format
        chat = MOCK_MESSAGE_DATA["chat"][0]
        handle = MOCK_MESSAGE_DATA["handle"][0]
        # Only include the second message (after filter time)
        msg = MOCK_MESSAGE_DATA["message"][1]
        joined_data = [(
            chat["ROWID"],
            chat["chat_identifier"],
            msg["ROWID"],
            msg["text"],
            msg["attributedBody"],
            msg["date"],
            msg["is_from_me"],
            handle["id"]
        )]

        mock_cursor.fetchall.return_value = joined_data
        mock_connect.return_value.cursor.return_value = mock_cursor

        # Set after_time to filter out first message
        after_time = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
        connector = IMessageConnector()
        connector.db_path = "mock_path"
        docs = list(connector.poll_source(start=after_time.timestamp(), end=None))

        assert len(docs) == 1  # Only second message should pass filter
        doc = docs[0]
        assert "How are you?" in str(doc.sections[0].text)

def test_database_not_found():
    """Test graceful handling when chat.db is not accessible."""
    with patch('sqlite3.connect', side_effect=Exception("Database not found")):
        connector = IMessageConnector()
        docs = list(connector.load_from_state())
        assert len(docs) == 0

def test_invalid_message_data():
    """Test handling of invalid message data."""
    with patch('sqlite3.connect') as mock_connect:
        # Setup mock cursor with invalid data
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [{"ROWID": 1, "invalid": "data"}],  # Invalid message
            MOCK_MESSAGE_DATA["chat"],
            MOCK_MESSAGE_DATA["handle"],
        ]
        mock_connect.return_value.cursor.return_value = mock_cursor

        connector = IMessageConnector()
        docs = list(connector.load_from_state())
        assert len(docs) == 0  # Invalid messages should be skipped
