"""Tests for signal_generator._trader_tracks_market."""

import pytest

from app.services.signal_generator import _trader_tracks_market


class _FakeTrader:
    """Minimal stand-in for TrackedTrader with only top_markets set."""

    def __init__(self, top_markets=None):
        self.top_markets = top_markets


class TestTraderTracksMarket:
    def test_none_top_markets_returns_true(self):
        """None means no filter yet — trader is candidate for all markets."""
        trader = _FakeTrader(top_markets=None)
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_empty_string_returns_true(self):
        trader = _FakeTrader(top_markets="")
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_empty_json_list_returns_true(self):
        trader = _FakeTrader(top_markets="[]")
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_matching_ticker_returns_true(self):
        trader = _FakeTrader(top_markets='["NASDAQ-CLOSE-24DEC31", "FED-RATE-24DEC18"]')
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_non_matching_ticker_returns_false(self):
        trader = _FakeTrader(top_markets='["NASDAQ-CLOSE-24DEC31"]')
        assert _trader_tracks_market(trader, "FED-RATE-24DEC18") is False

    def test_malformed_json_returns_true(self):
        """Malformed JSON should not exclude a trader."""
        trader = _FakeTrader(top_markets="{not valid json}")
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_case_sensitive_match(self):
        trader = _FakeTrader(top_markets='["nasdaq-close-24dec31"]')
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is False

    def test_multiple_markets_no_match(self):
        trader = _FakeTrader(top_markets='["A", "B", "C"]')
        assert _trader_tracks_market(trader, "D") is False
