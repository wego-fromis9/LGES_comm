import json
import math
import base64
import hashlib
import threading
import time
import urllib.error
import urllib.request

from rclpy.clock import Clock, ClockType
from lges_recipe_interfaces.msg import RecipeExecutionState
from lges_recipe_interfaces.srv import QueryRecipeList
from std_msgs.msg import String

try:
    from mir_api_interfaces.srv import QueryJson
except Exception:  # pragma: no cover - optional when MiR package is not installed
    QueryJson = None


CONTROL_STATES = {"AUTO", "MANUAL"}
OPERATION_STATES = {"INIT", "IDLE", "ACTIVE", "PAUSED", "CHARGING", "ERROR", "EMO"}
ERROR_LEVELS = {"WARNING", "URGENT", "CRITICAL", "FATAL"}
ACTION_STATUSES = {"IDLE", "READY", "ACTIVE", "ERROR", "PAUSED"}
_MISSING = object()


def _safe_text(value, fallback=""):
    text = str(value if value is not None else "").strip()
    return text or fallback


def _number_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _round_or_none(value, digits=3):
    number = _number_or_none(value)
    if number is None:
        return None
    return round(number, digits)


def _json_from_string_msg(msg):
    raw = getattr(msg, "data", msg)
    if isinstance(raw, dict):
        return raw
    if raw is None:
        return None
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError):
        text = str(raw).strip()
        return text if text else None


def _topic_safe_id(value, fallback):
    text = _safe_text(value, fallback)
    safe = "".join(ch if ch.isalnum() or ch in "_.-" else "_" for ch in text)
    return safe[:64] or fallback


def _normalize_enum(value, allowed, fallback=None):
    text = _safe_text(value).upper()
    if not text:
        return fallback
    if text in allowed:
        return text
    if any(key in text for key in ("EMO", "E-STOP", "ESTOP", "EMERGENCY")) and "EMO" in allowed:
        return "EMO"
    if any(key in text for key in ("ERROR", "FAULT", "FAIL", "PROTECTIVE")) and "ERROR" in allowed:
        return "ERROR"
    if "PAUSE" in text and "PAUSED" in allowed:
        return "PAUSED"
    if any(key in text for key in ("CHARGE", "CHARGING", "DOCK")) and "CHARGING" in allowed:
        return "CHARGING"
    if any(key in text for key in ("CHARGE", "CHARGING", "DOCK")) and "ACTIVE" in allowed:
        return "ACTIVE"
    if any(key in text for key in ("RUN", "ACTIVE", "EXECUT", "MOVING", "MISSION", "START")) and "ACTIVE" in allowed:
        return "ACTIVE"
    if "IDLE" in text and "IDLE" in allowed:
        return "IDLE"
    if any(key in text for key in ("READY", "WAIT", "STANDBY")) and "READY" in allowed:
        return "READY"
    if any(key in text for key in ("READY", "WAIT", "STANDBY")) and "IDLE" in allowed:
        return "IDLE"
    if "MANUAL" in text and "MANUAL" in allowed:
        return "MANUAL"
    if "AUTO" in text and "AUTO" in allowed:
        return "AUTO"
    return fallback


def _is_empty_value(value):
    return value is None or value == "" or value == [] or value == {}


def _get_path(source, path, default=None):
    if source is None:
        return default
    if path in (None, ""):
        return source
    current = source
    for part in str(path).split("."):
        if part == "":
            continue
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
        if current is None:
            return default
    return current


def _first_path(source, paths, default=None):
    for path in paths or []:
        value = _get_path(source, path)
        if not _is_empty_value(value):
            return value
    return default


class BackendMqttPublisher:
    """Build LGES MQTT payloads from ROS state without depending on the UI."""

    def __init__(self, node, mqtt_core, config):
        self.node = node
        self.mqtt_core = mqtt_core
        self.config = config or {}
        self.outbound_config = self.config.get("outbound", {}) or {}
        owner = str(self.outbound_config.get("publisher_owner", "ui") or "ui").lower()
        self.publisher_config = self.outbound_config.get("backend_publisher", {}) or {}
        self.enabled = bool(self.publisher_config.get("enabled", owner == "backend"))

        self.mir_state = {}
        self.mir_errors = []
        self.waypoints = []
        self.current_waypoint = None
        self.current_waypoint_at = 0.0
        self.recipe_state = None
        self.recipe_action_list = []
        self.recipe_list_fetched_at = 0.0
        self.recipe_query_in_flight = False
        self.last_pose_sample = None
        self.last_state_header_id = None
        self.last_factsheet_signature = ""
        self.topic_values = {}
        self.topic_received_at = {}
        self.service_values = {}
        self.inbound_action_history = []
        self.inbound_action_history_lock = threading.Lock()
        self.reached_node_history = []
        self.reached_node_history_lock = threading.Lock()
        self.dynamic_service_clients = {}
        self.dynamic_service_in_flight = set()
        self.dynamic_enrich_jobs = {}
        self.mir_rest_cache = {}
        self.mir_rest_warning_logged = False
        self.query_json_missing_logged = False
        self.subscriptions = []
        self.time_sync_wait_logged = False
        self.recipe_list_loaded = False
        self.pending_factsheet_publish = False
        self.pending_factsheet_force = False
        self.pending_factsheet_reason = ""
        self.recipe_query_unavailable_logged = False

        self.state_pub = None
        self.recipe_query_client = None
        self.timers = []
        self.timer_clock = getattr(node, "steady_clock", None) or Clock(clock_type=ClockType.STEADY_TIME)

        if self.enabled:
            self.start()

    def start(self):
        self.subscribe_configured_topics()

        canonical_topic = self.publisher_config.get("canonical_state_topic", "/lges/state_json")
        self.state_pub = self.node.create_publisher(String, canonical_topic, 10)
        recipe_field = self.configured_message_fields("factsheet").get("recipes", {}) or {}
        self.recipe_query_service_name = (
            recipe_field.get("service")
            or self.publisher_config.get("recipe_query_service", "/recipe/query_list")
        )
        self.recipe_query_client = self.node.create_client(
            QueryRecipeList,
            self.recipe_query_service_name,
        )

        state_cfg = self.outbound_config.get("state", {}) or {}
        if bool(state_cfg.get("enabled", True)):
            self.timers.append(self.node.create_timer(
                float(state_cfg.get("period_sec", 1.0)),
                self.publish_state_tick,
                clock=self.timer_clock,
            ))

        visualization_cfg = self.outbound_config.get("visualization", {}) or {}
        if isinstance(visualization_cfg, dict) and bool(visualization_cfg.get("enabled", False)):
            self.timers.append(self.node.create_timer(
                float(visualization_cfg.get("period_sec", 1.0)),
                self.publish_visualization_tick,
                clock=self.timer_clock,
            ))

        factsheet_cfg = self.outbound_config.get("factsheet", {}) or {}
        if bool(factsheet_cfg.get("enabled", True)):
            self.timers.append(self.node.create_timer(
                float(factsheet_cfg.get("check_period_sec", 5.0)),
                self.publish_factsheet_tick,
                clock=self.timer_clock,
            ))
            if bool(factsheet_cfg.get("publish_on_start", True)):
                self.timers.append(self.node.create_timer(
                    float(factsheet_cfg.get("initial_delay_sec", 3.0)),
                    self.publish_initial_factsheet_once,
                    clock=self.timer_clock,
                ))

        self.node.get_logger().info(
            "Backend MQTT publisher enabled: state/factsheet/visualization are owned by comm_manager"
        )

    def message_config(self, message_type):
        messages = self.config.get("messages", {}) or {}
        config = messages.get(message_type)
        if isinstance(config, dict):
            return config
        return messages.get(str(message_type).lower(), {}) or {}

    def configured_message_fields(self, message_type):
        return (self.message_config(message_type).get("fields") or {})

    def collect_ros_topic_specs(self, value, found=None):
        if found is None:
            found = {}
        if isinstance(value, dict):
            topic = value.get("ros_topic")
            if topic:
                topic_text = str(topic)
                found.setdefault(topic_text, str(value.get("ros_type") or "std_msgs/msg/String"))
            for child in value.values():
                self.collect_ros_topic_specs(child, found)
        elif isinstance(value, list):
            for item in value:
                self.collect_ros_topic_specs(item, found)
        return found

    def subscribe_configured_topics(self):
        found = {}
        for message_type in ("state", "factsheet", "visualization"):
            self.collect_ros_topic_specs(self.configured_message_fields(message_type), found)

        topics = self.publisher_config.get("topics", {}) or {}
        default_types = {
            str(topics.get("mir_state", "/mir/state")): "std_msgs/msg/String",
            str(topics.get("mir_errors", "/mir/errors_json")): "std_msgs/msg/String",
            str(topics.get("mir_current_waypoint", "/mir/current_waypoint_json")): "std_msgs/msg/String",
            str(topics.get("mir_reached_waypoint", "/mir/reached_waypoint_json")): "std_msgs/msg/String",
            str(topics.get("system_safety_state", "/system/safety_state")): "std_msgs/msg/String",
            str(topics.get("recipe_state", "/recipe/state")): "lges_recipe_interfaces/msg/RecipeExecutionState",
        }
        mir_waypoints_topic = topics.get("mir_waypoints")
        if mir_waypoints_topic:
            default_types[str(mir_waypoints_topic)] = "std_msgs/msg/String"
        for topic, ros_type in default_types.items():
            found.setdefault(topic, ros_type)

        for topic, ros_type in sorted(found.items()):
            msg_type = RecipeExecutionState if "RecipeExecutionState" in ros_type else String
            self.subscriptions.append(self.node.create_subscription(
                msg_type,
                topic,
                lambda msg, topic=topic: self.on_configured_topic(topic, msg),
                10,
            ))
            self.node.get_logger().info(f"Configured source subscribed: {topic} ({ros_type})")

    def on_configured_topic(self, topic, msg):
        payload = _json_from_string_msg(msg) if hasattr(msg, "data") else msg
        self.topic_values[topic] = payload
        self.topic_received_at[topic] = time.monotonic()
        self.update_legacy_cache(topic, payload)

    def update_legacy_cache(self, topic, payload):
        topics = self.publisher_config.get("topics", {}) or {}
        if topic == topics.get("mir_state", "/mir/state") and isinstance(payload, dict):
            self.mir_state = payload
        elif topic == topics.get("mir_errors", "/mir/errors_json"):
            errors = payload.get("errors") if isinstance(payload, dict) else payload
            self.mir_errors = errors if isinstance(errors, list) else []
        elif topic == topics.get("mir_waypoints", "/mir/waypoints_json"):
            waypoints = payload
            if isinstance(payload, dict):
                waypoints = payload.get("waypoints") or payload.get("positions") or []
            self.waypoints = waypoints if isinstance(waypoints, list) else []
        elif topic == topics.get("mir_current_waypoint", "/mir/current_waypoint_json") and isinstance(payload, dict):
            self.current_waypoint = payload
            self.current_waypoint_at = time.monotonic()
        elif topic == topics.get("mir_reached_waypoint", "/mir/reached_waypoint_json"):
            self.record_reached_node(payload)
        elif topic == topics.get("recipe_state", "/recipe/state"):
            self.recipe_state = payload

    def can_publish_factsheet(self):
        factsheet_cfg = self.outbound_config.get("factsheet", {}) or {}
        return self.enabled and bool(factsheet_cfg.get("enabled", True))

    def publish_initial_factsheet_once(self):
        self.publish_factsheet(force=True, reason="startup")
        # rclpy Timer has no one-shot mode; cancel the timer that called us.
        for timer in list(self.timers):
            if abs(float(getattr(timer, "timer_period_ns", 0)) / 1e9 - float((self.outbound_config.get("factsheet", {}) or {}).get("initial_delay_sec", 3.0))) < 0.001:
                timer.cancel()
                self.timers.remove(timer)
                break

    def on_mir_state(self, msg):
        payload = _json_from_string_msg(msg)
        if isinstance(payload, dict):
            self.mir_state = payload

    def on_mir_errors(self, msg):
        payload = _json_from_string_msg(msg)
        errors = payload.get("errors") if isinstance(payload, dict) else payload
        self.mir_errors = errors if isinstance(errors, list) else []

    def on_mir_waypoints(self, msg):
        payload = _json_from_string_msg(msg)
        waypoints = payload
        if isinstance(payload, dict):
            waypoints = payload.get("waypoints") or payload.get("positions") or []
        self.waypoints = waypoints if isinstance(waypoints, list) else []

    def on_mir_current_waypoint(self, msg):
        payload = _json_from_string_msg(msg)
        if isinstance(payload, dict):
            self.current_waypoint = payload
            self.current_waypoint_at = time.monotonic()

    def on_recipe_state(self, msg):
        self.recipe_state = msg

    def publish_state_tick(self):
        if not self.should_publish_mqtt():
            return
        try:
            payload = self.build_state_payload()
            result = self.mqtt_core.publish_by_template("state", **payload)
            self.last_state_header_id = (result.get("payload") or {}).get("headerId")
            self.publish_canonical_state(result.get("payload") or payload)
        except Exception as exc:
            self.node.get_logger().warn(f"Backend state publish failed: {exc}")

    def publish_visualization_tick(self):
        if not self.should_publish_mqtt():
            return
        try:
            self.mqtt_core.publish_by_template("visualization", **self.build_visualization_payload())
        except Exception as exc:
            self.node.get_logger().warn(f"Backend visualization publish failed: {exc}")

    def publish_factsheet_tick(self):
        self.refresh_recipe_list_if_needed()
        self.publish_factsheet(force=False, reason="periodic")

    def publish_factsheet(self, force=False, reason="request"):
        if not self.can_publish_factsheet() or not self.should_publish_mqtt():
            return False, "backend factsheet publisher is not ready"

        if self.should_wait_recipe_list_before_factsheet():
            self.pending_factsheet_publish = True
            self.pending_factsheet_force = bool(self.pending_factsheet_force or force)
            self.pending_factsheet_reason = str(reason or self.pending_factsheet_reason or "recipe_list_ready")
            request_started = self.refresh_recipe_list_if_needed(force=True)
            if not request_started and not self.recipe_query_in_flight:
                self.pending_factsheet_publish = False
                self.pending_factsheet_force = False
                self.pending_factsheet_reason = ""
                self.node.get_logger().warn(
                    "Factsheet recipe list 조회를 시작하지 못했습니다. 현재 확보된 데이터만으로 발행합니다."
                )
                return self.publish_factsheet_now(force=force, reason=reason)
            self.node.get_logger().info("Factsheet 발행 대기: recipe list 조회 후 발행합니다.")
            return True, "factsheet pending recipe list"

        self.refresh_recipe_list_if_needed()
        return self.publish_factsheet_now(force=force, reason=reason)

    def should_wait_recipe_list_before_factsheet(self):
        if self.recipe_list_loaded:
            return False
        if bool(self.recipe_query_in_flight):
            return True
        return bool(self.publisher_config.get("require_recipe_list_before_factsheet", True))

    def publish_factsheet_now(self, force=False, reason="request"):
        try:
            payload = self.build_factsheet_payload()
            signature = json.dumps({
                "nodeCount": len(payload.get("nodes", [])),
                "recipeCount": len(payload.get("recipes", [])),
                "actionCount": len(payload.get("actions", [])),
                "mapId": (payload.get("mapInfo") or {}).get("mapId"),
                "mapDataSize": len(json.dumps((payload.get("mapInfo") or {}).get("mapData"), sort_keys=True, ensure_ascii=False)),
            }, sort_keys=True)
            if not force and signature == self.last_factsheet_signature:
                return True, "factsheet unchanged"
            result = self.mqtt_core.publish_by_template("factsheet", **payload)
            self.last_factsheet_signature = signature
            return True, f"factsheet published ({reason}) -> {result.get('topic')}"
        except Exception as exc:
            self.node.get_logger().warn(f"Backend factsheet publish failed: {exc}")
            return False, str(exc)

    def should_publish_mqtt(self):
        if bool(self.publisher_config.get("publish_when_mqtt_disconnected", False)):
            return True
        if not bool(getattr(self.mqtt_core, "connected", False)):
            return False

        time_sync_config = self.config.get("time_sync", {}) or {}
        broker_config = time_sync_config.get("broker_topic", {}) or {}
        require_time_sync = bool(
            broker_config.get(
                "require_before_outbound_publish",
                self.publisher_config.get("require_time_sync_before_publish", False)
            )
        )
        if not require_time_sync:
            self.time_sync_wait_logged = False
            return True

        source = str(time_sync_config.get("source", "") or "").strip().lower()
        broker_enabled = bool(
            broker_config.get("enabled", source in ("broker", "broker_topic", "mqtt"))
        )
        if not broker_enabled:
            self.time_sync_wait_logged = False
            return True

        time_ready = (
            self.mqtt_core.broker_time_ready_for_outbound()
            if hasattr(self.mqtt_core, "broker_time_ready_for_outbound")
            else self.mqtt_core.broker_time_is_synced()
        )
        if time_ready:
            self.time_sync_wait_logged = False
            return True

        if not self.time_sync_wait_logged:
            self.node.get_logger().info(
                "Backend MQTT publish 대기: broker sync 수신/OS 시간 설정 전입니다."
            )
            self.time_sync_wait_logged = True
        return False

    def publish_canonical_state(self, payload):
        if self.state_pub is None:
            return
        msg = String()
        msg.data = json.dumps(payload, ensure_ascii=False)
        self.state_pub.publish(msg)

    def build_state_payload(self):
        return self.build_payload_from_mapping("state")

    def build_visualization_payload(self):
        return self.build_payload_from_mapping("visualization")

    def build_factsheet_payload(self):
        return self.build_payload_from_mapping("factsheet")

    def template_payload_defaults(self, message_type):
        try:
            template = self.mqtt_core._load_template(message_type)
            payload = template.get("payload", {}) if isinstance(template, dict) else {}
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def build_payload_from_mapping(self, message_type):
        defaults = self.template_payload_defaults(message_type)
        fields = self.configured_message_fields(message_type)
        header_fields = set(self.mqtt_core._default_header_values().keys())
        payload = {}
        context = {}

        for field_name, default_value in defaults.items():
            if field_name in header_fields:
                continue
            spec = fields.get(field_name, {})
            value = self.resolve_field(message_type, field_name, spec, context)
            if value is _MISSING:
                value = default_value
            context[field_name] = value
            payload[field_name] = value

        for field_name, spec in fields.items():
            if field_name in payload or field_name in header_fields:
                continue
            value = self.resolve_field(message_type, field_name, spec, context)
            if value is not _MISSING:
                context[field_name] = value
                payload[field_name] = value
        return payload

    def resolve_field(self, message_type, field_name, spec, context=None, base=None, index=0):
        context = context if context is not None else {}
        spec = spec if isinstance(spec, dict) else {"value": spec}

        if spec.get("only_when") and not self.evaluate_condition(str(spec.get("only_when")), context):
            return spec.get("empty_value", _MISSING)

        if "value" in spec:
            return self.resolve_template_value(spec.get("value"), context)

        if "source_priority" in spec:
            for candidate in spec.get("source_priority") or []:
                value = self.resolve_field(message_type, field_name, candidate, context, base=base, index=index)
                if not _is_empty_value(value):
                    if field_name == "errors" and isinstance(value, list):
                        return self.normalize_errors(value)
                    return self.apply_transform(value, spec.get("transform"), spec, context)
            return self.config_default(spec, _MISSING)

        source = str(spec.get("source", "") or "").strip()
        if source == "same_as":
            target_message = str(spec.get("message") or message_type)
            target_field = str(spec.get("field") or field_name)
            target_spec = self.configured_message_fields(target_message).get(target_field, {})
            return self.resolve_field(target_message, target_field, target_spec, context, base=base, index=index)

        if source == "internal":
            return getattr(self, str(spec.get("internal_field") or field_name), self.config_default(spec, None))

        if source == "config":
            return self.config_default(spec, self.config_default(spec, None))

        if source == "not_available":
            return self.config_default(spec, spec.get("empty_value"))

        if source == "builder":
            return self.resolve_builder(field_name, spec, context)

        if source in ("derived", "") and any(key in spec for key in ("rule", "from_field", "from_fields", "input_field", "true_when_text_contains")):
            return self.resolve_derived_field(field_name, spec, context, base=base)

        if source == "mixed" or "object_fields" in spec:
            return self.resolve_object_fields(message_type, spec, context, base=base, index=index)

        if source == "ros_service":
            return self.resolve_service_field(message_type, field_name, spec, context)

        if source == "ros_service_dynamic":
            return self.resolve_dynamic_service_field(message_type, field_name, spec, context)

        if source == "mir_rest_api":
            return self.resolve_mir_rest_api_field(message_type, field_name, spec, context)

        if source == "derived" and spec.get("candidate_paths"):
            topic_payload = base
            if topic_payload is None and spec.get("ros_topic"):
                topic_payload = self.topic_values.get(str(spec.get("ros_topic") or ""))
            value = self.extract_value(topic_payload, spec)
            value = self.apply_transform(value, spec.get("transform"), spec, context)
            return self.config_default(spec, value)

        if source == "ros_topic" or "ros_topic" in spec:
            topic_name = str(spec.get("ros_topic") or "")
            if self.is_topic_value_expired(topic_name, spec):
                return self.config_default(spec, spec.get("empty_value"))
            topic_payload = self.topic_values.get(topic_name)
            if "list_paths" in spec or "list_path" in spec:
                return self.resolve_list_field(message_type, spec, context, topic_payload)
            value = self.extract_value(topic_payload, spec)
            value = self.apply_transform(value, spec.get("transform"), spec, context)
            if field_name == "currentNodeId":
                value = self.normalize_current_node_id(value, spec)
            return self.config_default(spec, value)

        if base is not None:
            if "list_paths" in spec or "list_path" in spec or "item_mapping" in spec:
                return self.resolve_list_field(message_type, spec, context, base)
            value = self.extract_value(base, spec)
            value = self.apply_transform(value, spec.get("transform"), spec, context)
            return self.config_default(spec, value)

        return self.config_default(spec, _MISSING)

    def resolve_object_fields(self, message_type, spec, context, base=None, index=0):
        topic_payload = base
        if topic_payload is None and spec.get("ros_topic"):
            topic_payload = self.topic_values.get(str(spec.get("ros_topic") or ""))
        output = {}
        for key, child_spec in (spec.get("object_fields") or {}).items():
            local_context = {**context, **output}
            if isinstance(child_spec, dict) and child_spec.get("only_when"):
                if not self.evaluate_condition(str(child_spec.get("only_when")), local_context):
                    output[key] = child_spec.get("empty_value")
                    continue
            if isinstance(child_spec, dict):
                child_base = topic_payload
                value = self.resolve_field(message_type, key, child_spec, local_context, base=child_base, index=index)
            else:
                value = _get_path(topic_payload, child_spec)
            if value is _MISSING:
                value = None
            output[key] = value
        required = spec.get("required_fields") or []
        for key in required:
            if _is_empty_value(_get_path(output, key)):
                return spec.get("empty_value")
        return output

    def resolve_list_field(self, message_type, spec, context, source_payload):
        paths = spec.get("list_paths") or [spec.get("list_path", "")]
        values = None
        if isinstance(source_payload, list):
            values = source_payload
        else:
            for path in paths:
                values = _get_path(source_payload, path)
                if isinstance(values, list):
                    break
        if not isinstance(values, list):
            return spec.get("empty_value", [])

        max_items = spec.get("max_items")
        max_config = spec.get("max_items_config")
        if max_config:
            max_items = self.get_config_path(max_config, max_items)
        if max_items:
            values = values[:int(max_items)]

        item_mapping = spec.get("item_mapping") or {}
        required = spec.get("required_item_fields") or []
        exclude_name_values = spec.get("exclude_item_names") or []
        exclude_config_path = spec.get("exclude_item_names_config")
        if not exclude_name_values and exclude_config_path:
            exclude_name_values = self.get_config_path(exclude_config_path, []) or []
        exclude_names = set(
            _safe_text(name).lower()
            for name in exclude_name_values
            if _safe_text(name)
        )
        exclude_rules = spec.get("exclude_item_rules") or []
        exclude_rules_config_path = spec.get("exclude_item_rules_config")
        if not exclude_rules and exclude_rules_config_path:
            exclude_rules = self.get_config_path(exclude_rules_config_path, []) or []
        output = []
        for idx, item in enumerate(values, start=1):
            if isinstance(item, dict) and (exclude_names or exclude_rules):
                item_name = _safe_text(item.get("name") or _get_path(item, "raw.name")).lower()
                if item_name in exclude_names:
                    continue
                if self.matches_exclude_item_rules(item, exclude_rules):
                    continue
            if not isinstance(item_mapping, dict) or not item_mapping:
                output.append(item)
                continue
            mapped = self.resolve_mapping_object(message_type, item_mapping, context, item, idx)
            if any(_is_empty_value(_get_path(mapped, path)) for path in required):
                continue
            output.append(mapped)
        return output

    def matches_exclude_item_rules(self, item, rules):
        if not isinstance(item, dict) or not isinstance(rules, list):
            return False
        for rule in rules:
            if self.matches_exclude_item_rule(item, rule):
                return True
        return False

    def matches_exclude_item_rule(self, item, rule):
        if not isinstance(rule, dict):
            return False
        aliases = {
            "name": ["name", "raw.name"],
            "guid": ["guid", "id", "raw.guid", "raw.id"],
            "id": ["id", "guid", "raw.id", "raw.guid"],
            "type_id": ["type_id", "typeId", "type", "raw.type_id", "raw.typeId", "raw.type"],
            "typeId": ["type_id", "typeId", "type", "raw.type_id", "raw.typeId", "raw.type"],
            "type": ["type", "type_id", "typeId", "raw.type", "raw.type_id", "raw.typeId"],
            "parent_id": ["parent_id", "parentId", "parent", "raw.parent_id", "raw.parentId", "raw.parent"],
            "parentId": ["parent_id", "parentId", "parent", "raw.parent_id", "raw.parentId", "raw.parent"],
        }
        has_criterion = False
        for key, expected in rule.items():
            if _is_empty_value(expected):
                continue
            has_criterion = True
            paths = aliases.get(key, [key])
            actual_values = [_get_path(item, path) for path in paths]
            if not any(self.value_matches_rule(actual, expected) for actual in actual_values):
                return False
        return has_criterion

    def value_matches_rule(self, actual, expected):
        if isinstance(expected, list):
            return any(self.value_matches_rule(actual, item) for item in expected)
        return _safe_text(actual).lower() == _safe_text(expected).lower()

    def resolve_mapping_object(self, message_type, mapping, context, base, index):
        output = {}
        for key, child_spec in mapping.items():
            if isinstance(child_spec, dict) and not self.looks_like_field_spec(child_spec):
                output[key] = self.resolve_mapping_object(message_type, child_spec, context, base, index)
                continue
            spec = dict(child_spec) if isinstance(child_spec, dict) else {"field_path": child_spec}
            if spec.get("fallback_pattern") and not spec.get("fallback"):
                spec["fallback"] = str(spec.get("fallback_pattern")).format(index=index)
            value = self.resolve_field(message_type, key, spec, context, base=base, index=index)
            if isinstance(value, str):
                value = value.format(index=index)
            output[key] = None if value is _MISSING else value
        return output

    def looks_like_field_spec(self, spec):
        field_keys = {
            "source", "field_path", "candidate_paths", "fallback", "fallback_pattern",
            "transform", "default", "value", "empty_value", "ros_topic", "config_path",
            "list_path", "list_paths", "item_mapping", "required_item_fields", "max_items",
            "max_items_config", "exclude_item_names", "exclude_item_names_config",
            "exclude_item_rules", "exclude_item_rules_config",
        }
        return any(key in spec for key in field_keys)

    def resolve_service_field(self, message_type, field_name, spec, context):
        service_name = str(spec.get("service") or "")
        service_payload = self.service_values.get(service_name, {})
        if field_name == "recipes" and not service_payload and self.recipe_action_list:
            service_payload = {"recipes": self.recipe_action_list}
        source_list = _get_path(service_payload, spec.get("response_list_path", "recipes"), [])
        if not isinstance(source_list, list):
            return spec.get("empty_value", [])
        mapped = []
        for idx, item in enumerate(source_list, start=1):
            source_item = {"recipeId": item, "name": item} if isinstance(item, str) else (item or {})
            mapped.append(self.resolve_mapping_object(
                message_type,
                spec.get("item_mapping") or {},
                context,
                source_item,
                idx,
            ))
        return mapped

    def resolve_dynamic_service_field(self, message_type, field_name, spec, context):
        service_name = str(spec.get("service") or "")
        request_spec = spec.get("request") or {}
        query = str(request_spec.get("query") or "")
        request_id = self.resolve_dynamic_request_id(request_spec, context)
        if not service_name or not query or not request_id:
            return spec.get("empty_value", _MISSING)

        cache_key = self.dynamic_service_cache_key(service_name, query, request_id)
        cached = self.service_values.get(cache_key, _MISSING)
        if cached is not _MISSING:
            value = _get_path(cached, spec.get("output_path", ""), spec.get("empty_value"))
            if "list_paths" in spec or "list_path" in spec or "item_mapping" in spec:
                return self.resolve_list_field(message_type, spec, context, value)
            return value

        self.request_dynamic_query_json(service_name, query, request_id, spec, cache_key)
        return spec.get("empty_value", _MISSING)

    def resolve_dynamic_request_id(self, request_spec, context):
        context_key = str(request_spec.get("id_from_context") or "")
        if context_key:
            value = context.get(context_key)
            if not _is_empty_value(value):
                return str(value)

        topic_spec = request_spec.get("id_from_topic")
        if isinstance(topic_spec, dict):
            payload = self.topic_values.get(str(topic_spec.get("ros_topic") or ""))
            value = self.extract_value(payload, topic_spec)
            if not _is_empty_value(value):
                return str(value)

        value = request_spec.get("id")
        return "" if _is_empty_value(value) else str(value)

    def dynamic_service_cache_key(self, service_name, query, request_id):
        return f"{service_name}:{query}:{request_id}"

    def request_dynamic_query_json(self, service_name, query, request_id, spec, cache_key):
        if cache_key in self.dynamic_service_in_flight:
            return False
        if QueryJson is None:
            if not self.query_json_missing_logged:
                self.node.get_logger().warn(
                    "mir_api_interfaces/srv/QueryJson is not available; factsheet mapData will stay empty."
                )
                self.query_json_missing_logged = True
            return False

        client = self.dynamic_service_clients.get(service_name)
        if client is None:
            client = self.node.create_client(QueryJson, service_name)
            self.dynamic_service_clients[service_name] = client

        wait_sec = float(spec.get("service_wait_sec", 0.05) or 0.0)
        if not client.wait_for_service(timeout_sec=wait_sec):
            return False

        request = QueryJson.Request()
        request.query = str(query)
        request.id = str(request_id)
        self.dynamic_service_in_flight.add(cache_key)
        future = client.call_async(request)
        future.add_done_callback(
            lambda future, cache_key=cache_key, spec=dict(spec): self.on_dynamic_query_json_response(
                future,
                cache_key,
                spec,
            )
        )
        return True

    def on_dynamic_query_json_response(self, future, cache_key, spec):
        try:
            result = future.result()
            if not getattr(result, "success", False):
                self.dynamic_service_in_flight.discard(cache_key)
                self.node.get_logger().warn(
                    f"Dynamic QueryJson failed ({cache_key}): {getattr(result, 'message', '')}"
                )
                return
            raw_json = getattr(result, str(spec.get("response_json_field") or "raw_json"), "") or "{}"
            parsed = json.loads(raw_json)
        except Exception as exc:
            self.dynamic_service_in_flight.discard(cache_key)
            self.node.get_logger().warn(f"Dynamic QueryJson parse failed ({cache_key}): {exc}")
            return

        if self.start_dynamic_enrich_if_needed(parsed, cache_key, spec):
            return

        self.dynamic_service_in_flight.discard(cache_key)
        self.service_values[cache_key] = parsed
        if bool(spec.get("publish_on_response", False)):
            self.publish_factsheet_now(force=True, reason=f"dynamic_query_ready:{cache_key}")

    def start_dynamic_enrich_if_needed(self, parsed, cache_key, spec):
        enrich_spec = spec.get("enrich_items")
        if not isinstance(enrich_spec, dict) or not isinstance(parsed, list):
            return False

        service_name, query, _request_id = self.parse_dynamic_cache_key(cache_key)
        detail_query = str(enrich_spec.get("query") or "")
        if not service_name or not detail_query:
            return False
        client = self.dynamic_service_clients.get(service_name)
        if client is None or QueryJson is None:
            return False

        items = [dict(item) if isinstance(item, dict) else {"value": item} for item in parsed]
        id_paths = enrich_spec.get("id_paths") or ["guid", "id"]
        required_paths = enrich_spec.get("required_paths") or []
        pending = []
        for index, item in enumerate(items):
            if required_paths and any(not _is_empty_value(_get_path(item, path)) for path in required_paths):
                continue
            item_id = _first_path(item, id_paths)
            if not _is_empty_value(item_id):
                pending.append((index, str(item_id)))

        if not pending:
            return False

        self.dynamic_enrich_jobs[cache_key] = {
            "items": items,
            "remaining": len(pending),
            "spec": dict(spec),
            "merge": bool(enrich_spec.get("merge", True)),
        }
        for index, item_id in pending:
            request = QueryJson.Request()
            request.query = detail_query
            request.id = item_id
            future = client.call_async(request)
            future.add_done_callback(
                lambda future, cache_key=cache_key, index=index: self.on_dynamic_enrich_response(
                    future,
                    cache_key,
                    index,
                )
            )
        return True

    def parse_dynamic_cache_key(self, cache_key):
        parts = str(cache_key).split(":", 2)
        if len(parts) != 3:
            return "", "", ""
        return parts[0], parts[1], parts[2]

    def on_dynamic_enrich_response(self, future, cache_key, index):
        job = self.dynamic_enrich_jobs.get(cache_key)
        if not job:
            return
        try:
            result = future.result()
            if getattr(result, "success", False):
                detail = json.loads(getattr(result, "raw_json", "") or "{}")
                if isinstance(detail, dict) and isinstance(job["items"][index], dict):
                    job["items"][index] = {**job["items"][index], **detail}
        except Exception as exc:
            self.node.get_logger().warn(f"Dynamic QueryJson item enrich failed ({cache_key}[{index}]): {exc}")

        job["remaining"] = int(job.get("remaining", 1)) - 1
        if job["remaining"] > 0:
            return

        self.dynamic_enrich_jobs.pop(cache_key, None)
        self.dynamic_service_in_flight.discard(cache_key)
        self.service_values[cache_key] = job["items"]
        spec = job.get("spec", {}) or {}
        if bool(spec.get("publish_on_response", False)):
            self.publish_factsheet_now(force=True, reason=f"dynamic_query_ready:{cache_key}")

    def resolve_mir_rest_api_field(self, message_type, field_name, spec, context):
        request_spec = spec.get("request") or {}
        path_template = str(request_spec.get("path_template") or request_spec.get("path") or "")
        request_id = self.resolve_dynamic_request_id(request_spec, context)
        if not path_template:
            return spec.get("empty_value", _MISSING)
        if "{id}" in path_template and not request_id:
            return spec.get("empty_value", _MISSING)

        api_path = path_template.format(id=request_id, mapId=request_id)
        payload = self.mir_rest_get_json(api_path, spec)
        if payload is _MISSING:
            return spec.get("empty_value", _MISSING)

        payload = self.enrich_mir_rest_items_if_needed(payload, spec)
        if "list_paths" in spec or "list_path" in spec or "item_mapping" in spec:
            return self.resolve_list_field(message_type, spec, context, payload)
        return payload

    def mir_rest_config(self):
        return self.publisher_config.get("mir_rest", {}) or {}

    def mir_rest_get_json(self, api_path, spec=None):
        rest = self.mir_rest_config()
        host = _safe_text(rest.get("host"))
        if not host:
            if not self.mir_rest_warning_logged:
                self.node.get_logger().warn("MiR REST API host is not configured; factsheet API fields will stay empty.")
                self.mir_rest_warning_logged = True
            return _MISSING

        cache_ttl = float(rest.get("cache_ttl_sec", 0.0) or 0.0)
        cache_key = str(api_path)
        cached = self.mir_rest_cache.get(cache_key)
        if cached and cache_ttl > 0 and time.monotonic() - cached.get("at", 0.0) <= cache_ttl:
            return cached.get("value")

        scheme_host = host if str(host).startswith(("http://", "https://")) else f"http://{host}"
        base_path = _safe_text(rest.get("base_path"), "/api/v2.0.0").rstrip("/")
        path = str(api_path or "")
        if not path.startswith("/"):
            path = f"/{path}"
        url = f"{scheme_host.rstrip('/')}{base_path}{path}"
        timeout = float(rest.get("timeout_sec", 3.0) or 3.0)
        request = urllib.request.Request(url, headers=self.mir_rest_headers(rest), method="GET")

        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
            value = json.loads(raw) if raw else {}
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
            self.node.get_logger().warn(f"MiR REST API read failed ({path}): {exc}")
            return _MISSING

        self.mir_rest_cache[cache_key] = {"at": time.monotonic(), "value": value}
        return value

    def mir_rest_headers(self, rest):
        username = _safe_text(rest.get("username"))
        password = _safe_text(rest.get("password"))
        if _safe_text(rest.get("password_hash_mode")).lower() == "sha256":
            password = hashlib.sha256(password.encode("utf-8")).hexdigest()
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return {
            "Content-Type": "application/json",
            "Accept-Language": "ko_KR.utf8",
            "Authorization": f"Basic {token}",
        }

    def enrich_mir_rest_items_if_needed(self, payload, spec):
        enrich_spec = spec.get("enrich_items")
        if not isinstance(enrich_spec, dict) or not isinstance(payload, list):
            return payload

        path_template = str(enrich_spec.get("path_template") or enrich_spec.get("path") or "")
        if not path_template:
            return payload

        id_paths = enrich_spec.get("id_paths") or ["guid", "id"]
        required_paths = enrich_spec.get("required_paths") or []
        merge = bool(enrich_spec.get("merge", True))
        output = []
        for item in payload:
            source = dict(item) if isinstance(item, dict) else {"value": item}
            if required_paths and any(not _is_empty_value(_get_path(source, path)) for path in required_paths):
                output.append(source)
                continue
            item_id = _first_path(source, id_paths)
            if _is_empty_value(item_id):
                output.append(source)
                continue
            detail = self.mir_rest_get_json(path_template.format(id=str(item_id)), spec)
            if isinstance(detail, dict):
                output.append({**source, **detail} if merge else detail)
            else:
                output.append(source)
        return output

    def resolve_derived_field(self, field_name, spec, context, base=None):
        if field_name == "lastActionId":
            recipe_action = _safe_text(getattr(self.recipe_state, "current_step_id", ""))
            if recipe_action:
                return _topic_safe_id(recipe_action, "ACT_RECIPE")
            return spec.get("empty_value")
        if field_name == "lastActionSequenceId":
            if not _safe_text(getattr(self.recipe_state, "current_step_id", "")):
                return spec.get("empty_value")
            recipe_index = getattr(self.recipe_state, "current_step_index", None)
            if recipe_index not in (None, ""):
                try:
                    index = int(recipe_index)
                    return index if index > 0 else 1
                except (TypeError, ValueError):
                    pass
            return spec.get("empty_value")
        if field_name == "currentActionState":
            if _safe_text(getattr(self.recipe_state, "current_step_id", "")):
                return self.build_current_action_state(context, spec)
            return spec.get("empty_value")
        if field_name == "lastNodeSeqNo":
            latest = self.latest_reached_node_history()
            return latest.get("sequenceId") if latest else spec.get("empty_value")
        if field_name == "lastNodeId":
            latest = self.latest_reached_node_history()
            return latest.get("nodeId") if latest else spec.get("empty_value")
        if field_name == "nodeStates":
            return self.build_node_states(context, spec)
        if field_name == "driving":
            if str(spec.get("rule") or "").strip() == "mir_state_text_in_list":
                topic_name = str(spec.get("ros_topic") or "")
                payload = self.topic_values.get(topic_name) if topic_name else None
                if not isinstance(payload, dict):
                    payload = self.mir_state

                state_text = _safe_text(_get_path(payload, spec.get("field_path", "state_text")), "")
                state_text = state_text.strip().lower()
                if not state_text:
                    return bool(spec.get("empty_value", False))

                exact_values = {
                    str(item).strip().lower()
                    for item in (spec.get("true_when_state_text_in") or [])
                    if str(item).strip()
                }
                if state_text in exact_values:
                    return True

                contains_values = [
                    str(item).strip().lower()
                    for item in (spec.get("true_when_text_contains") or [])
                    if str(item).strip()
                ]
                return any(item in state_text for item in contains_values)

            operation_state = context.get("operationState")
            if _is_empty_value(operation_state):
                operation_state = self.resolve_operation_state()
            return operation_state == "ACTIVE"
        if field_name == "paused":
            return self.resolve_paused_state(spec)
        if field_name == "velocity":
            enabled_config = spec.get("enabled_config")
            if enabled_config and not bool(self.get_config_path(enabled_config, False)):
                return spec.get("empty_value")
            return self.build_velocity()
        if field_name == "charging":
            text = " ".join(_safe_text(_get_path(base, path)) for path in spec.get("candidate_paths") or [])
            text = text.lower()
            return any(str(key).lower() in text for key in spec.get("true_when_text_contains") or [])
        return self.config_default(spec, spec.get("empty_value", _MISSING))

    def resolve_builder(self, field_name, spec, context):
        if field_name == "currentActionState":
            return self.build_current_action_state(context, spec)
        if field_name == "actionStates":
            operation_state = context.get("operationState") or self.resolve_operation_state()
            return self.build_action_states(operation_state)
        if field_name == "instantActionStates":
            return self.build_instant_action_states()
        if field_name == "nodeStates":
            return self.build_node_states(context, spec)
        return spec.get("empty_value", _MISSING)

    def extract_value(self, source_payload, spec):
        if source_payload is None:
            return None
        if spec.get("candidate_paths"):
            return _first_path(source_payload, spec.get("candidate_paths"))
        if "field_path" in spec:
            return _get_path(source_payload, spec.get("field_path"))
        if "list_path" in spec:
            return _get_path(source_payload, spec.get("list_path"))
        return source_payload

    def config_default(self, spec, value):
        if value is not None and value is not _MISSING:
            return value
        if spec.get("config_path"):
            return self.get_config_path(spec.get("config_path"), spec.get("default", spec.get("empty_value", value)))
        if spec.get("fallback_config"):
            return self.get_config_path(spec.get("fallback_config"), spec.get("default", spec.get("empty_value", value)))
        if "default" in spec:
            return spec.get("default")
        if "fallback" in spec:
            return spec.get("fallback")
        if "fallback_pattern" in spec:
            return str(spec.get("fallback_pattern")).format(index=1)
        if "empty_value" in spec:
            return spec.get("empty_value")
        return value

    def get_config_path(self, path, default=None):
        return _get_path(self.config, path, default)

    def is_topic_value_expired(self, topic_name, spec):
        max_age = spec.get("max_age_sec")
        if spec.get("max_age_sec_config"):
            max_age = self.get_config_path(spec.get("max_age_sec_config"), max_age)
        if max_age in (None, "", 0, 0.0):
            return False
        received_at = self.topic_received_at.get(str(topic_name or ""), 0.0)
        if received_at <= 0:
            return True
        return (time.monotonic() - received_at) > float(max_age)

    def evaluate_condition(self, condition, context):
        text = str(condition or "").strip()
        if text.endswith(" is available"):
            key = text[: -len(" is available")].strip()
            return not _is_empty_value(context.get(key))
        if text.startswith("not "):
            return _is_empty_value(context.get(text[4:].strip()))
        return bool(context.get(text))

    def apply_transform(self, value, transform, spec=None, context=None):
        if value is _MISSING:
            return value
        transform = str(transform or "").strip()
        if not transform:
            return value
        if transform == "round_3":
            return _round_or_none(value, 3)
        if transform == "round_1":
            return _round_or_none(value, 1)
        if transform == "number_or_null":
            return _number_or_none(value)
        if transform == "empty_to_null":
            return None if _is_empty_value(value) else value
        if transform == "degrees_to_radians_round_3":
            number = _number_or_none(value)
            return None if number is None else round(math.radians(number), 3)
        if transform == "angle_to_radians_round_3":
            number = _number_or_none(value)
            if number is None:
                return None
            if abs(number) > (math.pi * 2):
                number = math.radians(number)
            return round(number, 3)
        if transform == "uppercase":
            return _safe_text(value).upper()
        if transform == "topic_safe_id":
            fallback = "ID"
            if isinstance(spec, dict):
                fallback = spec.get("fallback") or str(spec.get("fallback_pattern", "ID")).format(index=1)
            return _topic_safe_id(value, fallback)
        if transform == "normalize_control_state":
            fallback = (
                self.get_config_path(spec.get("fallback_config"), spec.get("empty_value"))
                if isinstance(spec, dict) and spec.get("fallback_config")
                else (spec.get("fallback", spec.get("empty_value")) if isinstance(spec, dict) else None)
            )
            allowed = set(spec.get("allowed") or CONTROL_STATES) if isinstance(spec, dict) else CONTROL_STATES
            return _normalize_enum(value, allowed, fallback)
        if transform == "normalize_operation_state":
            fallback = (
                self.get_config_path(spec.get("fallback_config"), spec.get("empty_value"))
                if isinstance(spec, dict) and spec.get("fallback_config")
                else (spec.get("fallback", spec.get("empty_value")) if isinstance(spec, dict) else None)
            )
            allowed = set(spec.get("allowed") or OPERATION_STATES) if isinstance(spec, dict) else OPERATION_STATES
            return _normalize_enum(value, allowed, fallback)
        return value

    def resolve_template_value(self, value, context):
        if isinstance(value, dict):
            return {key: self.resolve_template_value(item, context) for key, item in value.items()}
        if isinstance(value, list):
            return [self.resolve_template_value(item, context) for item in value]
        if not isinstance(value, str):
            return value
        if value.startswith("{") and value.endswith("}") and "[]." in value:
            body = value.strip("{}")
            source_name, item_path = body.split("[].", 1)
            source_items = context.get(source_name, [])
            if not isinstance(source_items, list):
                return []
            return [
                _get_path(item, item_path)
                for item in source_items
                if not _is_empty_value(_get_path(item, item_path))
            ]
        try:
            return value.format(**context)
        except Exception:
            return value

    def resolve_paused_state(self, spec):
        topic_name = str(spec.get("ros_topic") or "/system/safety_state")
        payload = self.topic_values.get(topic_name)
        if not isinstance(payload, dict):
            return bool(spec.get("empty_value", False))

        if bool(spec.get("true_when_pause_requested", True)):
            pause_path = str(spec.get("pause_requested_field_path") or "pause_requested")
            if bool(_get_path(payload, pause_path, False)):
                return True

        state_path = str(spec.get("state_field_path") or "state")
        state = _safe_text(_get_path(payload, state_path), "").strip().lower()
        true_states = {
            str(item).strip().lower()
            for item in (spec.get("true_when_states") or [])
            if str(item).strip()
        }
        return bool(state and state in true_states)

    def normalize_current_node_id(self, value, spec):
        if _is_empty_value(value):
            return value
        if not bool(spec.get("null_when_same_as_last_reached", False)):
            return value

        latest = self.latest_reached_node_history()
        latest_node_id = latest.get("nodeId") if latest else None
        if latest_node_id == value:
            return spec.get("empty_value")
        return value

    def build_robot_position(self):
        position = self.mir_state.get("position") or {}
        x = _round_or_none(position.get("x"))
        y = _round_or_none(position.get("y"))
        if x is None or y is None:
            return None
        orientation = _round_or_none(position.get("orientation"), 3)
        return {
            "x": x,
            "y": y,
            "theta": None if orientation is None else round(math.radians(orientation), 3),
        }

    def build_velocity(self):
        if not bool(self.publisher_config.get("derive_velocity_from_pose", False)):
            return None
        position = self.build_robot_position()
        if not position:
            return None
        current = {**position, "at": time.monotonic()}
        previous = self.last_pose_sample
        self.last_pose_sample = current
        if not previous:
            return None
        dt = max(current["at"] - previous["at"], 0.001)
        omega = None
        if current.get("theta") is not None and previous.get("theta") is not None:
            omega = round((current["theta"] - previous["theta"]) / dt, 3)
        return {
            "vx": round((current["x"] - previous["x"]) / dt, 3),
            "vy": round((current["y"] - previous["y"]) / dt, 3),
            "omega": omega,
        }

    def build_battery_state(self):
        return {
            "batteryCharge": _round_or_none(self.mir_state.get("battery_percentage"), 1),
            "batteryVoltage": None,
            "batteryCurrent": None,
            "batteryHealth": None,
            "charging": self.is_charging(),
            "reach": _number_or_none(self.mir_state.get("battery_time_remaining")),
        }

    def resolve_operation_state(self):
        allowed = self.operation_allowed_states()
        fallback = (self.publisher_config.get("defaults", {}) or {}).get("operation_state", "INIT")
        recipe_state = _safe_text(getattr(self.recipe_state, "operation_state", ""))
        if recipe_state:
            return _normalize_enum(recipe_state, allowed, fallback)

        text = " ".join(_safe_text(self.mir_state.get(key)) for key in (
            "state_text",
            "state",
            "mission_text",
            "mode_text",
        )).lower()
        if self.mir_state.get("errors"):
            return "ERROR" if "ERROR" in allowed else fallback
        if "emergency" in text or "estop" in text or "e-stop" in text:
            return "EMO" if "EMO" in allowed else fallback
        if "error" in text or "fault" in text:
            return "ERROR" if "ERROR" in allowed else fallback
        if "pause" in text:
            return "PAUSED" if "PAUSED" in allowed else fallback
        if any(key in text for key in ("charge", "charging", "dock")):
            if "CHARGING" in allowed:
                return "CHARGING"
            return "ACTIVE" if "ACTIVE" in allowed else fallback
        if any(key in text for key in ("execut", "moving", "mission", "active", "running", "start")):
            return "ACTIVE" if "ACTIVE" in allowed else fallback
        if "idle" in text:
            if "IDLE" in allowed:
                return "IDLE"
            return "READY" if "READY" in allowed else fallback
        if any(key in text for key in ("ready", "wait", "standby")):
            if "READY" in allowed:
                return "READY"
            return "IDLE" if "IDLE" in allowed else fallback
        return fallback

    def operation_allowed_states(self):
        spec = self.configured_message_fields("state").get("operationState", {}) or {}
        allowed = spec.get("allowed") if isinstance(spec, dict) else None
        return set(allowed or OPERATION_STATES)

    def resolve_current_node_id(self):
        max_age = float(self.publisher_config.get("current_waypoint_max_age_sec", 3.5))
        if not self.current_waypoint or (time.monotonic() - self.current_waypoint_at) > max_age:
            return None
        return _topic_safe_id(
            self.current_waypoint.get("name") or self.current_waypoint.get("guid") or self.current_waypoint.get("id"),
            "NODE_CURRENT",
        )

    def inbound_action_history_snapshot(self):
        with self.inbound_action_history_lock:
            return [dict(item) for item in self.inbound_action_history]

    def latest_inbound_action_history(self):
        history = self.inbound_action_history_snapshot()
        return history[-1] if history else {}

    def inbound_action_history_by_kind(self, kind):
        expected = str(kind or "").strip()
        return [
            item for item in self.inbound_action_history_snapshot()
            if str(item.get("kind") or "").strip() == expected
        ]

    def reached_node_history_snapshot(self):
        with self.reached_node_history_lock:
            return [dict(item) for item in self.reached_node_history]

    def latest_reached_node_history(self):
        history = self.reached_node_history_snapshot()
        return history[-1] if history else {}

    def record_reached_node(self, payload):
        if isinstance(payload, list):
            for item in payload:
                self.record_reached_node(item)
            return
        if not isinstance(payload, dict):
            return

        node_id = self.waypoint_node_id(payload, "NODE_REACHED")
        if _is_empty_value(node_id):
            return

        max_items = int(self.publisher_config.get("max_node_history", 200) or 200)
        with self.reached_node_history_lock:
            if self.reached_node_history and self.reached_node_history[-1].get("nodeId") == node_id:
                return
            entry = {
                "nodeId": node_id,
                "sequenceId": len(self.reached_node_history) + 1,
            }
            self.reached_node_history.append(entry)
            if max_items > 0 and len(self.reached_node_history) > max_items:
                self.reached_node_history = self.reached_node_history[-max_items:]
                for index, item in enumerate(self.reached_node_history, start=1):
                    item["sequenceId"] = index

    def record_inbound_event(self, event):
        if not isinstance(event, dict):
            return
        action_id = _safe_text(
            event.get("actionId") or event.get("orderId") or event.get("id"),
            f"HIST_{int(time.time())}",
        )
        action_type = _safe_text(event.get("actionType") or event.get("orderType") or event.get("kind"), "UNKNOWN")
        entry = {
            "kind": _safe_text(event.get("kind"), "unknown"),
            "actionId": _topic_safe_id(action_id, "HIST_ACTION"),
            "actionType": action_type,
        }
        if not _is_empty_value(event.get("actionSeqNo")):
            try:
                entry["actionSeqNo"] = int(event.get("actionSeqNo"))
            except (TypeError, ValueError):
                entry["actionSeqNo"] = event.get("actionSeqNo")
        if not _is_empty_value(event.get("actionDescription")):
            entry["actionDescription"] = _safe_text(event.get("actionDescription"))
        if not _is_empty_value(event.get("actionStatus")):
            entry["actionStatus"] = _safe_text(event.get("actionStatus"))
        if not _is_empty_value(event.get("actionResult")):
            entry["actionResult"] = _safe_text(event.get("actionResult"))

        max_items = int(self.publisher_config.get("max_action_history", 50) or 50)
        with self.inbound_action_history_lock:
            self.inbound_action_history.append(entry)
            if max_items > 0 and len(self.inbound_action_history) > max_items:
                self.inbound_action_history = self.inbound_action_history[-max_items:]

    def iter_known_waypoints(self):
        for waypoint in self.waypoints or []:
            if isinstance(waypoint, dict):
                yield waypoint
        for key, value in self.service_values.items():
            if ":map_positions:" not in str(key) or not isinstance(value, list):
                continue
            for waypoint in value:
                if isinstance(waypoint, dict):
                    yield waypoint

    def waypoint_node_id(self, waypoint, fallback):
        return _topic_safe_id(
            waypoint.get("name") or waypoint.get("guid") or waypoint.get("id"),
            fallback,
        )

    def node_sequence_for(self, node_id):
        node_text = _safe_text(node_id)
        if not node_text:
            return None
        for index, waypoint in enumerate(self.iter_known_waypoints(), start=1):
            candidate = self.waypoint_node_id(waypoint, f"NODE_{index}")
            if candidate == node_text:
                return index
        return None

    def build_node_states(self, context, spec):
        if str(spec.get("builder") or "") == "build_reached_node_states":
            return self.reached_node_history_snapshot() or spec.get("empty_value", [])

        if bool(spec.get("include_known_waypoints", False)):
            states = []
            seen = set()
            max_items = spec.get("max_items")
            max_config = spec.get("max_items_config")
            if max_config:
                max_items = self.get_config_path(max_config, max_items)
            max_items = int(max_items or 0)
            for index, waypoint in enumerate(self.iter_known_waypoints(), start=1):
                node_id = self.waypoint_node_id(waypoint, f"NODE_{index}")
                if _is_empty_value(node_id) or node_id in seen:
                    continue
                seen.add(node_id)
                states.append({"nodeId": node_id, "sequenceId": index})
                if max_items > 0 and len(states) >= max_items:
                    break
            if states:
                return states

        fields = spec.get("from_fields") or [spec.get("from_field") or "lastNodeId"]
        states = []
        seen = set()
        for field in fields:
            node_id = context.get(str(field))
            if _is_empty_value(node_id) or node_id in seen:
                continue
            seen.add(node_id)
            sequence = self.node_sequence_for(node_id) or len(states) + 1
            states.append({"nodeId": node_id, "sequenceId": sequence})
        return states or spec.get("empty_value", [])

    def normalize_action_status(self, value, fallback="IDLE"):
        text = _safe_text(value, "").strip().upper()
        if not text:
            return fallback
        if text in ACTION_STATUSES:
            return text
        if text in {"ACCEPTED", "VALIDATING", "PREPARING", "PENDING"}:
            return "READY"
        if text in {"RUNNING", "STEP_RUNNING", "ACTIVE", "EXECUTING", "CHARGING"}:
            return "ACTIVE"
        if text in {"PAUSE", "PAUSED", "SAFETY_PAUSED"}:
            return "PAUSED"
        if text in {"REJECTED", "FAILED", "FAILURE", "ABORTED", "ERROR", "EMO", "MIR_ERROR", "SAFETY_ABORT", "SAFETY_ESTOP", "TIMEOUT"}:
            return "ERROR"
        if text in {"SUCCEEDED", "SUCCESS", "COMPLETED", "FINISHED", "DONE"}:
            return "IDLE"
        return fallback

    def action_state_from_history_entry(self, entry):
        if not isinstance(entry, dict):
            return None
        action_id = _safe_text(entry.get("actionId"))
        action_type = _safe_text(entry.get("actionType"))
        if not action_id and not action_type:
            return None
        return {
            "actionId": action_id or "UNKNOWN_ACTION",
            "actionType": action_type or "UNKNOWN",
            "actionStatus": self.normalize_action_status(entry.get("actionStatus"), "READY"),
            "actionResult": _safe_text(entry.get("actionResult"), ""),
        }

    def find_recipe_step(self, recipe_id, step_id):
        recipe_text = _safe_text(recipe_id)
        step_text = _safe_text(step_id)
        if not recipe_text or not step_text:
            return {}
        service_payload = self.service_values.get(str(getattr(self, "recipe_query_service_name", "/recipe/query_list")), {})
        source_list = _get_path(service_payload, "recipes", [])
        if not isinstance(source_list, list):
            return {}
        for recipe in source_list:
            if not isinstance(recipe, dict):
                continue
            candidate_recipe_id = _safe_text(
                recipe.get("recipe_id") or recipe.get("recipeId") or recipe.get("id") or recipe.get("name")
            )
            if candidate_recipe_id != recipe_text:
                continue
            for step in recipe.get("steps") or []:
                if not isinstance(step, dict):
                    continue
                candidate_step_id = _safe_text(step.get("id") or step.get("actionId") or step.get("action"))
                if candidate_step_id == step_text:
                    return step
        return {}

    def build_current_action_state(self, context=None, spec=None):
        recipe = self.recipe_state
        step_id = _safe_text(getattr(recipe, "current_step_id", ""))
        if not step_id:
            return None
        recipe_id = _safe_text(getattr(recipe, "recipe_id", ""))
        step = self.find_recipe_step(recipe_id, step_id)
        action_type = _safe_text(
            step.get("action") or step.get("type") or recipe_id,
            "RECIPE",
        )
        operation_state = (context or {}).get("operationState") or self.resolve_operation_state()
        status_source = getattr(recipe, "execution_state", "") or operation_state
        return {
            "actionId": _topic_safe_id(step_id, "ACT_RECIPE"),
            "actionType": action_type,
            "actionStatus": self.normalize_action_status(status_source, self.normalize_action_status(operation_state, "ACTIVE")),
            "actionResult": _safe_text(getattr(recipe, "message", ""), getattr(recipe, "result_code", "")),
        }

    def build_action_states(self, operation_state):
        states = []
        current = self.build_current_action_state({"operationState": operation_state})
        if current:
            states.append(current)
        for entry in self.inbound_action_history_by_kind("order"):
            state = self.action_state_from_history_entry(entry)
            if state:
                states.append(state)
        return states

    def build_instant_action_states(self):
        states = []
        for entry in self.inbound_action_history_by_kind("instantAction"):
            state = self.action_state_from_history_entry(entry)
            if state:
                states.append(state)
        return states

    def normalize_error_level(self, value):
        text = _safe_text(value, "").strip().upper()
        if text in ERROR_LEVELS:
            return text
        if text in {"INFO", "NOTICE", "DEBUG"}:
            return "WARNING"
        if text in {"WARN", "WARNING"}:
            return "WARNING"
        if text in {"URGENT", "ALARM"}:
            return "URGENT"
        if text in {"ERROR", "ERR", "CRITICAL", "FAIL", "FAILED", "FAULT"}:
            return "CRITICAL"
        if text in {"FATAL", "EMO", "ESTOP", "EMERGENCY"}:
            return "FATAL"
        return "CRITICAL"

    def normalize_errors(self, errors):
        if not isinstance(errors, list):
            return []
        normalized = []
        for index, error in enumerate(errors):
            if not isinstance(error, dict):
                error = {"message": str(error)}
            normalized.append({
                "errorType": _safe_text(error.get("type") or error.get("module") or error.get("code"), f"ERR_{index + 1}"),
                "errorReferences": error.get("references") if isinstance(error.get("references"), list) else [],
                "errorDescription": _safe_text(error.get("description") or error.get("message") or error.get("error"), "Unknown error"),
                "errorLevel": self.normalize_error_level(error.get("level") or error.get("severity")),
            })
        return normalized

    def is_charging(self):
        text = " ".join(_safe_text(self.mir_state.get(key)) for key in (
            "state_text",
            "mission_text",
            "mode_text",
        )).lower()
        return any(key in text for key in ("charge", "charging", "dock", "chg"))

    def build_nodes(self):
        nodes = []
        max_nodes = int(self.publisher_config.get("max_factsheet_nodes", 300))
        for index, waypoint in enumerate(self.waypoints[:max_nodes]):
            if not isinstance(waypoint, dict):
                continue
            x = _round_or_none(waypoint.get("x") or waypoint.get("pos_x"))
            y = _round_or_none(waypoint.get("y") or waypoint.get("pos_y"))
            if x is None or y is None:
                continue
            orientation = _round_or_none(waypoint.get("orientation"), 3) or 0.0
            nodes.append({
                "nodeId": _topic_safe_id(waypoint.get("name") or waypoint.get("guid"), f"NODE_{index + 1:03d}"),
                "nodePosition": {
                    "x": x,
                    "y": y,
                    "theta": round(math.radians(orientation), 3),
                },
            })
        return nodes

    def build_recipes(self):
        recipes = []
        service_payload = self.service_values.get(str(getattr(self, "recipe_query_service_name", "/recipe/query_list")), {})
        source_list = _get_path(service_payload, "recipes", [])
        if not isinstance(source_list, list):
            source_list = []
        for index, recipe in enumerate(source_list):
            source = {"recipe_id": recipe} if isinstance(recipe, str) else (recipe or {})
            recipe_id = source.get("recipe_id") or source.get("recipeId") or source.get("id") or source.get("name")
            steps = []
            for step in source.get("steps") or []:
                if not isinstance(step, dict):
                    continue
                steps.append({
                    "id": step.get("id"),
                    "seq": step.get("seq"),
                    "type": step.get("type"),
                    "action": step.get("action"),
                    "timeout_sec": _number_or_none(step.get("timeout_sec")),
                    "params": step.get("params") if isinstance(step.get("params"), dict) else {},
                })
            recipes.append({
                "recipeId": _topic_safe_id(recipe_id, f"RCP_{index + 1:03d}"),
                "recipeSteps": steps,
            })
        return recipes

    def build_actions(self, nodes, recipes):
        return []

    def refresh_recipe_list_if_needed(self, force=False):
        refresh_sec = float(self.publisher_config.get("recipe_list_refresh_sec", 5.0))
        if self.recipe_query_in_flight:
            return False
        if not force and time.monotonic() - self.recipe_list_fetched_at < refresh_sec:
            return False
        wait_sec = float(self.publisher_config.get("recipe_query_service_wait_sec", 0.2) or 0.0)
        service_ready = (
            self.recipe_query_client is not None
            and self.recipe_query_client.wait_for_service(timeout_sec=wait_sec)
        )
        if not service_ready:
            self.recipe_list_fetched_at = time.monotonic()
            if not self.recipe_query_unavailable_logged:
                self.node.get_logger().warn(
                    "Recipe list service is not ready; factsheet recipe list will stay pending."
                )
                self.recipe_query_unavailable_logged = True
            return False

        request = QueryRecipeList.Request()
        request.include_details = True
        self.recipe_query_in_flight = True
        self.recipe_query_unavailable_logged = False
        future = self.recipe_query_client.call_async(request)
        future.add_done_callback(self.on_recipe_list_response)
        return True

    def on_recipe_list_response(self, future):
        self.recipe_query_in_flight = False
        self.recipe_list_fetched_at = time.monotonic()
        try:
            result = future.result()
            if not getattr(result, "success", False):
                return
            parsed = json.loads(getattr(result, "recipes_json", "") or "{}")
        except Exception as exc:
            self.node.get_logger().warn(f"Recipe list parse failed: {exc}")
            return

        recipes = parsed.get("recipes") if isinstance(parsed, dict) else []
        if not isinstance(recipes, list):
            return
        self.service_values[str(self.recipe_query_service_name)] = parsed
        normalized = []
        for index, recipe in enumerate(recipes):
            source = {"recipeId": recipe, "name": recipe} if isinstance(recipe, str) else (recipe or {})
            recipe_id = _safe_text(
                source.get("recipe_id") or source.get("recipeId") or source.get("id") or source.get("name"),
                f"RCP_{index + 1:03d}",
            )
            label = _safe_text(source.get("name") or source.get("recipe_name") or source.get("description"), recipe_id)
            normalized.append({
                "actionId": _topic_safe_id(recipe_id, f"RCP_{index + 1:03d}"),
                "actionType": "RECIPE",
                "actionDescription": label,
            })
        self.recipe_action_list = normalized
        self.recipe_list_loaded = True
        self.node.get_logger().info(f"Recipe list loaded for factsheet: {len(normalized)} recipes")

        if self.pending_factsheet_publish:
            force = self.pending_factsheet_force
            reason = self.pending_factsheet_reason or "recipe_list_ready"
            self.pending_factsheet_publish = False
            self.pending_factsheet_force = False
            self.pending_factsheet_reason = ""
            self.publish_factsheet_now(force=force, reason=reason)
