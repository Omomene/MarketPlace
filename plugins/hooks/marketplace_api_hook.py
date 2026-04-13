from airflow.hooks.base import BaseHook
import requests


class MarketplaceAPIHook(BaseHook):

    def __init__(self, base_url="http://api-marketplace:5000", token="formation-token-2026"):
        self.base_url = base_url
        self.token = token

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}"
        }

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    def get_orders(self, date):
        return self.get("/orders", params={"date": date})

    def get_sellers(self):
        return self.get("/sellers")

    def get_products(self):
        return self.get("/products")