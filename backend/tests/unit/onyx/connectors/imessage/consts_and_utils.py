from datetime import datetime, timezone
from typing import Dict, List, Any

# Mock data representing the structure of chat.db tables
# Based on reverse engineering of the Messages app database schema
MOCK_MESSAGE_DATA: Dict[str, List[Dict[str, Any]]] = {
    "message": [
        {
            "ROWID": 1,
            "text": "Hello world",
            "attributedBody": None,  # Will be populated with hex-encoded data in actual db
            "date": int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
            "is_from_me": 0,
            "handle_id": 1,
            "cache_has_attachments": 0,
            "service": "iMessage"
        },
        {
            "ROWID": 2,
            "text": "How are you?",
            "attributedBody": None,
            "date": int(datetime(2024, 1, 1, 1, tzinfo=timezone.utc).timestamp()),
            "is_from_me": 1,
            "handle_id": 1,
            "cache_has_attachments": 0,
            "service": "iMessage"
        }
    ],
    "handle": [
        {
            "ROWID": 1,
            "id": "+1234567890",
            "service": "iMessage",
            "uncanonicalized_id": "+1234567890"
        }
    ],
    "chat": [
        {
            "ROWID": 1,
            "guid": "iMessage;-;chat123",
            "chat_identifier": "chat123",
            "display_name": "Test Chat",
            "style": 45  # Individual chat
        }
    ],
    "chat_message_join": [
        {
            "chat_id": 1,
            "message_id": 1
        },
        {
            "chat_id": 1,
            "message_id": 2
        }
    ]
}

def create_mock_chat_db(db_path: str) -> None:
    """Creates a mock chat.db file with test data for integration tests."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
    CREATE TABLE message (
        ROWID INTEGER PRIMARY KEY,
        text TEXT,
        attributedBody BLOB,
        date INTEGER,
        is_from_me INTEGER,
        handle_id INTEGER,
        cache_has_attachments INTEGER,
        service TEXT
    )""")

    cursor.execute("""
    CREATE TABLE handle (
        ROWID INTEGER PRIMARY KEY,
        id TEXT,
        service TEXT,
        uncanonicalized_id TEXT
    )""")

    cursor.execute("""
    CREATE TABLE chat (
        ROWID INTEGER PRIMARY KEY,
        guid TEXT,
        chat_identifier TEXT,
        display_name TEXT,
        style INTEGER
    )""")

    cursor.execute("""
    CREATE TABLE chat_message_join (
        chat_id INTEGER,
        message_id INTEGER,
        PRIMARY KEY (chat_id, message_id)
    )""")

    # Insert mock data
    cursor.executemany(
        "INSERT INTO message VALUES (:ROWID, :text, :attributedBody, :date, :is_from_me, :handle_id, :cache_has_attachments, :service)",
        MOCK_MESSAGE_DATA["message"]
    )

    cursor.executemany(
        "INSERT INTO handle VALUES (:ROWID, :id, :service, :uncanonicalized_id)",
        MOCK_MESSAGE_DATA["handle"]
    )

    cursor.executemany(
        "INSERT INTO chat VALUES (:ROWID, :guid, :chat_identifier, :display_name, :style)",
        MOCK_MESSAGE_DATA["chat"]
    )

    cursor.executemany(
        "INSERT INTO chat_message_join VALUES (:chat_id, :message_id)",
        MOCK_MESSAGE_DATA["chat_message_join"]
    )

    conn.commit()
    conn.close()
