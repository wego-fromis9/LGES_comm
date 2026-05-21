import json


class InboundPayloadValidator:
    """Validate Host -> robot MQTT payloads without test-only file side effects."""

    def __init__(self, logger=None):
        self.logger = logger
        self.last_error = ""

    def _log(self, message, level="info"):
        if not self.logger:
            return
        if level == "error":
            self.logger.error(message)
        elif level == "warn":
            self.logger.warning(message)
        else:
            self.logger.info(message)

    def _fail(self, message, level="warn"):
        self.last_error = str(message or "validation failed")
        self._log(self.last_error, level)
        return False

    def _validate_required_keys(self, data, required_keys):
        if not isinstance(data, dict):
            return self._fail("invalid payload format: payload must be a JSON object", "error")
        missing = [key for key in required_keys if key not in data]
        if missing:
            return self._fail(f"invalid payload format: missing required keys {missing}", "warn")
        return True

    def _normalize_action_type(self, value, allowed_action_types):
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        if raw in allowed_action_types:
            return raw
        compact = "".join(raw.split())
        if compact in allowed_action_types:
            return compact
        for action_type in allowed_action_types:
            compact_action_type = action_type.replace("_", "")
            if compact == action_type + action_type:
                return action_type
            if compact == compact_action_type:
                return action_type
            if compact == compact_action_type + compact_action_type:
                return action_type
        return raw

    def _validate_order(self, data):
        required = [
            "headerId",
            "timestamp",
            "version",
            "manufacturer",
            "serialNumber",
            "orderId",
            "orderUpdateId",
        ]
        if not self._validate_required_keys(data, required):
            return False

        if not str(data.get("orderId") or "").strip():
            return self._fail("invalid order format: orderId must not be empty", "warn")
        try:
            int(data.get("orderUpdateId"))
        except (TypeError, ValueError):
            return self._fail("invalid order format: orderUpdateId must be an integer", "warn")

        actions = data.get("actions", [])
        recipe_id = str(data.get("recipeId") or "").strip()
        if not recipe_id and not actions:
            return self._fail("invalid order format: either recipeId or actions is required", "warn")
        if actions is not None and not isinstance(actions, list):
            return self._fail("invalid order format: actions must be an array", "warn")

        for index, action in enumerate(actions or []):
            if not isinstance(action, dict):
                return self._fail(f"invalid order format: actions[{index}] must be an object", "warn")
            missing = [
                key for key in ("actionId", "actionSeqNo", "actionType")
                if key not in action
            ]
            if missing:
                return self._fail(f"invalid order format: actions[{index}] missing required keys {missing}", "warn")
            if "actionParameters" in action and not isinstance(action.get("actionParameters"), list):
                return self._fail(f"invalid order format: actions[{index}].actionParameters must be an array", "warn")
        return True

    def _validate_instant_actions(self, data):
        required = [
            "headerId",
            "timestamp",
            "version",
            "manufacturer",
            "serialNumber",
            "instantActions",
        ]
        if not self._validate_required_keys(data, required):
            return False

        allowed_action_types = {
            "start_pause",
            "stop_pause",
            "start_charge",
            "stop_charge",
            "cancel_order",
            "clear_instant_actions",
            "request_factsheet",
        }

        actions = data.get("instantActions")
        if not isinstance(actions, list) or not actions:
            return self._fail("invalid instantActions format: instantActions must be a non-empty array", "warn")

        for index, action in enumerate(actions):
            if not isinstance(action, dict):
                return self._fail(f"invalid instantActions format: instantActions[{index}] must be an object", "warn")
            missing = [
                key for key in ("actionId", "actionType")
                if key not in action
            ]
            if missing:
                return self._fail(f"invalid instantActions format: instantActions[{index}] missing required keys {missing}", "warn")
            action_type = self._normalize_action_type(action.get("actionType"), allowed_action_types)
            if action_type and action_type not in allowed_action_types:
                return self._fail(f"invalid instantActions format: unsupported actionType {action_type}", "warn")
        return True

    def process_result(self, topic, payload):
        self.last_error = ""
        if isinstance(payload, (str, bytes)):
            try:
                data = json.loads(payload)
            except (json.JSONDecodeError, ValueError) as exc:
                message = f"invalid JSON format: {exc}"
                self._fail(message, "error")
                return False, message, None
        elif isinstance(payload, dict):
            data = payload
        else:
            message = f"unsupported payload type: {type(payload)}"
            self._fail(message, "error")
            return False, message, None

        topic_lower = str(topic or "").lower()
        if topic_lower.endswith("/order"):
            if self._validate_order(data):
                return True, "", data
            return False, self.last_error or "invalid order format", data

        if topic_lower.endswith("/instantactions"):
            if self._validate_instant_actions(data):
                return True, "", data
            return False, self.last_error or "invalid instantActions format", data

        return False, f"unsupported topic: {topic}", data

    # Backward-compatible name for older call sites. It no longer writes YAML.
    def process_and_save_result(self, topic, payload):
        return self.process_result(topic, payload)

    def process_and_save(self, topic, payload):
        success, _message, _data = self.process_result(topic, payload)
        return success
