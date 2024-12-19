"""
Utility functions for iMessage connector.
Handles message decoding and data conversion operations.
"""
import binascii
from datetime import datetime
from typing import Any

from onyx.connectors.models import Document, Section
from onyx.configs.constants import DocumentSource


def decode_attributed_body(hex_blob: str | None) -> str:
    if not hex_blob:
        return ""

    try:
        clean_hex = ''.join(c for c in hex_blob if c in '0123456789abcdefABCDEF')
        decoded = binascii.unhexlify(clean_hex).decode('utf-8', errors='ignore')
        message_start = decoded.find("NSString")
        if message_start != -1:
            content_start = decoded.find('"', message_start)
            content_end = decoded.find('"', content_start + 1)
            if content_start != -1 and content_end != -1:
                return decoded[content_start + 1:content_end]

        return decoded.strip()
    except (binascii.Error, UnicodeDecodeError):
        return ""


def create_document_from_chat(chat_data: dict[str, Any], messages: list[dict[str, Any]]) -> Document:
    chat_id = chat_data["chat_identifier"]

    sections = []
    for msg in messages:
        # First try to get text from attributedBody, fall back to text field
        text = decode_attributed_body(msg.get("attributedBody")) or msg.get("text", "")
        sender = "Me" if msg.get("is_from_me") else msg.get("sender", "Unknown")
        timestamp = datetime.fromtimestamp(msg.get("date", 0) / 1e9)  # Convert nanoseconds to seconds

        section_text = f"{sender} ({timestamp.strftime('%Y-%m-%d %H:%M:%S')}): {text}"
        sections.append(Section(text=section_text, link=None))

    return Document(
        id=f"imessage-chat-{chat_id}",
        sections=sections,
        source=DocumentSource.IMESSAGE,
        semantic_identifier=f"iMessage Chat: {chat_id}",
        metadata={
            "chat_identifier": chat_id,
            "participant_count": str(len(set(m.get("sender", "") for m in messages))),
        },
        doc_updated_at=datetime.fromtimestamp(
            max(m.get("date", 0) for m in messages) / 1e9  # Convert nanoseconds to seconds
        ) if messages else None
    )
