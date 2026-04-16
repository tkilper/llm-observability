"""
Pytest configuration and shared fixtures.

Sets up sys.path so tests can import from sibling package directories,
and stubs out confluent_kafka so producer.py can be imported without
a running broker or the native librdkafka library.
"""

import os
import sys
from unittest.mock import MagicMock

# Make the producer package importable from tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

# Stub confluent_kafka — tests never need a real broker
sys.modules.setdefault("confluent_kafka", MagicMock())
