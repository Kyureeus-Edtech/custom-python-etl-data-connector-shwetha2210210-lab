from datetime import datetime

def transform_breach_results(raw_list, account: str):
    # raw_list is list of breach dicts from API
    transformed = []
    for item in raw_list:
        t = {
            "account": account,
            "breach_name": item.get("Name"),
            "title": item.get("Title"),
            "domain": item.get("Domain"),
            "breach_date": parse_date(item.get("BreachDate")),
            "added_date": parse_datetime(item.get("AddedDate")),
            "modified_date": parse_datetime(item.get("ModifiedDate")),
            "pwn_count": item.get("PwnCount"),
            "data_classes": item.get("DataClasses"),
            "is_verified": item.get("IsVerified"),
            "is_sensitive": item.get("IsSensitive"),
            "retrieved_at": datetime.utcnow()
        }
        transformed.append(t)
    return transformed

def parse_date(date_str):
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    return None

def parse_datetime(dt_str):
    if dt_str:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return None
