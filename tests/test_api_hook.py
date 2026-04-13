import pytest
from unittest.mock import patch
from hooks.marketplace_api_hook import MarketplaceAPIHook


@patch("hooks.marketplace_api_hook.requests.get")
def test_get_orders(mock_get):

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "orders": [{"order_id": "O1"}, {"order_id": "O2"}]
    }

    hook = MarketplaceAPIHook()
    result = hook.get_orders("2026-04-13")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["order_id"] == "O1"