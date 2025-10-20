import os
from dotenv import load_dotenv
from utils.config_loader import load_config
from utils.logger_setup import get_logger
from connectors.breached_account_connector import BreachedAccountConnector
from connectors.paste_account_connector import PasteAccountConnector
# … import other connectors …

from storage.mongo_client import MongoClientWrapper

def main():
    load_dotenv()
    cfg = load_config()
    logger = get_logger(__name__)

    mongo = MongoClientWrapper(uri=cfg["mongo"]["uri"],
                               db_name=cfg["mongo"]["db_name"])
    hibp_cfg = cfg["hibp"]
    # Example: we could read a list of accounts/emails to process
    accounts = ["user1@example.com", "user2@example.com"]  # maybe from a file or DB

    for account in accounts:
        try:
            # 1. Breached account connector
            bc = BreachedAccountConnector(base_url=hibp_cfg["base_url"],
                                          api_key=os.getenv(hibp_cfg["api_key_env"]),
                                          user_agent=hibp_cfg["user_agent"])
            raw = bc.fetch(account=account)
            # 2. Transform
            from transformers.normalize_breach import transform_breach_results
            transformed = transform_breach_results(raw, account=account)
            # 3. Store
            mongo.collection("breached_accounts").insert_many(transformed)
            logger.info(f"Inserted breaches for {account}")
        except Exception as e:
            logger.error(f"Error processing account {account}: {e}")

    mongo.close()

if __name__ == "__main__":
    main()
