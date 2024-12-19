import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from onyx.configs.constants import DocumentSource
from onyx.connectors.imessage.connector import IMessageConnector
from onyx.connectors.models import Document

from ....unit.onyx.connectors.imessage.consts_and_utils import create_mock_chat_db, MOCK_MESSAGE_DATA

def test_load_messages():
    """Test loading messages from a mock chat.db file."""
    with tempfile.NamedTemporaryFile() as temp_db:
        with patch('os.path.expanduser') as mock_expand:
            mock_expand.return_value = temp_db.name

            # Create mock database
            create_mock_chat_db(temp_db.name)

            # Test loading all messages
            connector = IMessageConnector()
            docs = list(connector.load_source())

            assert len(docs) == 2  # Should match number of messages in mock data

            # Verify first document
            doc = docs[0]
            assert isinstance(doc, Document)
            assert doc.source == DocumentSource.IMESSAGE
            assert doc.semantic_identifier == "Test Chat"
            assert "Hello world" in doc.text
            assert doc.metadata["is_from_me"] is False
            assert doc.metadata["handle_id"] == "+1234567890"

            # Test time-based filtering
            after_time = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
            filtered_docs = list(connector.load_source(after_time=after_time))
            assert len(filtered_docs) == 1
            assert "How are you?" in filtered_docs[0].text

def test_database_permissions():
    """Test handling of permission errors when accessing chat.db."""
    with tempfile.NamedTemporaryFile() as temp_db:
        with patch('os.path.expanduser') as mock_expand:
            mock_expand.return_value = temp_db.name

            # Create mock database but make it unreadable
            create_mock_chat_db(temp_db.name)
            os.chmod(temp_db.name, 0o000)

            connector = IMessageConnector()
            docs = list(connector.load_source())
            assert len(docs) == 0  # Should handle permission error gracefully

def test_poll_for_updates():
    """Test polling for new messages."""
    with tempfile.NamedTemporaryFile() as temp_db:
        with patch('os.path.expanduser') as mock_expand:
            mock_expand.return_value = temp_db.name

            # Create initial database
            create_mock_chat_db(temp_db.name)

            connector = IMessageConnector()
            initial_docs = list(connector.load_source())
            assert len(initial_docs) == 2

            # Simulate new message by recreating database with additional message
            new_message = {
                "ROWID": 3,
                "text": "New message",
                "attributedBody": None,
                "date": int(datetime(2024, 1, 1, 2, tzinfo=timezone.utc).timestamp()),
                "is_from_me": 0,
                "handle_id": 1,
                "cache_has_attachments": 0,
                "service": "iMessage"
            }
            MOCK_MESSAGE_DATA["message"].append(new_message)
            MOCK_MESSAGE_DATA["chat_message_join"].append({"chat_id": 1, "message_id": 3})

            create_mock_chat_db(temp_db.name)

            # Test polling with time filter
            poll_time = datetime(2024, 1, 1, 1, 30, tzinfo=timezone.utc)
            new_docs = list(connector.load_source(after_time=poll_time))
            assert len(new_docs) == 1
            assert "New message" in new_docs[0].text
