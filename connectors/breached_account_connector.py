import requests

class BreachedAccountConnector:
    def __init__(self, base_url, api_key, user_agent):
        self.base_url = base_url
        self.api_key_header = api_key
        self.user_agent = user_agent

    def fetch(self, account: str):
        url = f"{self.base_url}/breachedaccount/{requests.utils.quote(account)}"
        headers = {
            "hibp-api-key": self.api_key_header,
            "User-Agent": self.user_agent
        }
        params = {"truncateResponse": "false"}
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            # No breaches for that account
            return []
        else:
            resp.raise_for_status()
