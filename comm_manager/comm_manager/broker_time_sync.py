from datetime import datetime, timezone


def _safe_text(value, fallback=""):
    text = str(value if value is not None else "").strip()
    return text or fallback


def get_nested_value(payload, field_path):
    if not field_path:
        return payload
    current = payload
    for part in str(field_path).split("."):
        key = part.strip()
        if not key:
            continue
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _as_list(value):
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _matches_id(value, expected_values):
    text = _safe_text(value).lower()
    return bool(text and text in {_safe_text(item).lower() for item in expected_values})


def select_timestamp_value(payload, config):
    """Select the configured timestamp from a broker time payload.

    Supports two practical shapes:
    1. list selector:
       {"timestamps": [{"id": "control", "timestamp": "..."}, {"id": "robot", "timestamp": "..."}]}
    2. direct field:
       {"control": {"timestamp": "..."}, "robot": {"timestamp": "..."}}
    """
    if not isinstance(payload, dict):
        return None, None

    selected_ids = _as_list(config.get("selected_id") or config.get("id_value") or "control")
    selected_ids.extend(_as_list(config.get("selected_ids")))
    id_fields = _as_list(config.get("id_field") or "id")
    timestamp_field = config.get("timestamp_field") or "timestamp"
    payload_mode = _safe_text(config.get("payload_mode")).lower()

    direct_path = config.get("selected_timestamp_field") or config.get("timestamp_path")
    if direct_path:
        value = get_nested_value(payload, direct_path)
        if value not in (None, ""):
            return value, {"mode": "direct_path", "path": direct_path}

    if payload_mode in {"header", "header_timestamp", "plain", "plain_timestamp"}:
        value = get_nested_value(payload, timestamp_field)
        if value not in (None, ""):
            return value, {"mode": payload_mode, "path": timestamp_field}

    list_paths = _as_list(config.get("list_path"))
    if not list_paths:
        list_paths = ["timestamps", "timeList", "timestampList"]

    for list_path in list_paths:
        items = get_nested_value(payload, list_path)
        if not isinstance(items, list):
            continue
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            if not any(_matches_id(get_nested_value(item, id_field), selected_ids) for id_field in id_fields):
                continue
            value = get_nested_value(item, timestamp_field)
            if value not in (None, ""):
                return value, {
                    "mode": "list_selector",
                    "listPath": list_path,
                    "index": index,
                    "selectedIds": selected_ids,
                    "timestampField": timestamp_field,
                }

    direct_object_field = config.get("direct_object_field") or "control"
    direct_object_path = f"{direct_object_field}.{timestamp_field}"
    value = get_nested_value(payload, direct_object_path)
    if value not in (None, ""):
        return value, {"mode": "direct_object", "path": direct_object_path}

    value = get_nested_value(payload, timestamp_field)
    if value not in (None, ""):
        return value, {"mode": "plain_timestamp", "path": timestamp_field}

    return None, None


def parse_timestamp_value(value, assume_timezone=timezone.utc):
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1_000_000_000_000:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    text = _safe_text(value)
    if not text:
        raise ValueError("timestamp is empty")

    if text.isdigit():
        seconds = float(text)
        if seconds > 1_000_000_000_000:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    normalized = text
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=assume_timezone)
    return parsed.astimezone(timezone.utc)
