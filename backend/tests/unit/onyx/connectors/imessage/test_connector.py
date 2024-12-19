import os
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from onyx.configs.constants import DocumentSource
from onyx.connectors.imessage.connector import IMessageConnector
from onyx.connectors.models import Document

from .consts_and_utils import MOCK_MESSAGE_DATA, create_mock_chat_db

def test_message_to_document():
    """Test that messages are correctly converted to documents."""
    with patch('sqlite3.connect') as mock_connect:
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            MOCK_MESSAGE_DATA["message"],  # For messages query
            MOCK_MESSAGE_DATA["chat"],     # For chat query
            MOCK_MESSAGE_DATA["handle"],   # For handle query
        ]
        mock_connect.return_value.cursor.return_value = mock_cursor

        connector = IMessageConnector()
        docs = list(connector.load_source())

        assert len(docs) == 2  # We have 2 messages in mock data
        doc = docs[0]

        # Verify document structure
        assert isinstance(doc, Document)
        assert doc.source == DocumentSource.IMESSAGE
        assert doc.semantic_identifier == "Test Chat"
        assert "Hello world" in doc.text
        assert doc.metadata["is_from_me"] is False
        assert doc.metadata["handle_id"] == "+1234567890"
        assert doc.metadata["service"] == "iMessage"

def test_time_filter():
    """Test that time-based filtering works correctly."""
    with patch('sqlite3.connect') as mock_connect:
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            MOCK_MESSAGE_DATA["message"],
            MOCK_MESSAGE_DATA["chat"],
            MOCK_MESSAGE_DATA["handle"],
        ]
        mock_connect.return_value.cursor.return_value = mock_cursor

        # Set after_time to filter out first message
        after_time = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
        connector = IMessageConnector()
        docs = list(connector.load_source(after_time=after_time))

        assert len(docs) == 1  # Only second message should pass filter
        doc = docs[0]
        assert "How are you?" in doc.text
        assert doc.metadata["is_from_me"] is True

def test_database_not_found():
    """Test graceful handling when chat.db is not accessible."""
    with patch('sqlite3.connect', side_effect=Exception("Database not found")):
        connector = IMessageConnector()
        docs = list(connector.load_source())
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
        docs = list(connector.load_source())
        assert len(docs) == 0  # Invalid messages should be skipped
