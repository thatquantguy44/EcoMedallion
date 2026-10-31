"""Executable contracts for the required offline pytest tier."""

from __future__ import annotations

import socket

import pytest


def test_offline_tests_are_classified_as_unit(request):
    assert request.node.get_closest_marker("unit") is not None


def test_offline_tests_cannot_open_network_connections():
    with pytest.raises(RuntimeError, match="network access is disabled"):
        socket.create_connection(("127.0.0.1", 9), timeout=0.1)
