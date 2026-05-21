import json
import math
import time

from rclpy.clock import Clock, ClockType
from lges_recipe_interfaces.msg import RecipeExecutionState
from lges_recipe_interfaces.srv import QueryRecipeList
from std_msgs.msg import String


CONTROL_STATES = {"AUTO", "MANUAL"}
OPERATION_STATES = {"INIT", "READY", "ACTIVE", "PAUSED", "ERROR", "EMO"}
ERROR_LEVELS = {"INFO", "WARNING", "ERROR", "FATAL"}


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
    if any(key in text for key in ("RUN", "ACTIVE", "EXECUT", "MOVING", "MISSION", "CHARGE", "DOCK")) and "ACTIVE" in allowed:
        return "ACTIVE"
    if any(key in text for key in ("READY", "WAIT", "STANDBY", "IDLE")) and "READY" in allowed:
        return "READY"
    if "MANUAL" in text and "MANUAL" in allowed:
        return "MANUAL"
    if "AUTO" in text and "AUTO" in allowed:
        return "AUTO"
    return fallback


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
        self.system_safety_state = ""
        self.last_pose_sample = None
        self.last_state_header_id = None
        self.last_factsheet_signature = ""
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
        topics = self.publisher_config.get("topics", {}) or {}
        self.node.create_subscription(
            String,
            topics.get("mir_state", "/mir/state"),
            self.on_mir_state,
            10,
        )
        self.node.create_subscription(
            String,
            topics.get("mir_errors", "/mir/errors_json"),
            self.on_mir_errors,
            10,
        )
        self.node.create_subscription(
            String,
            topics.get("mir_waypoints", "/mir/waypoints_json"),
            self.on_mir_waypoints,
            10,
        )
        self.node.create_subscription(
            String,
            topics.get("mir_current_waypoint", "/mir/current_waypoint_json"),
            self.on_mir_current_waypoint,
            10,
        )
        self.node.create_subscription(
            String,
            topics.get("system_safety_state", "/system/safety_state"),
            self.on_system_safety_state,
            10,
        )
        self.node.create_subscription(
            RecipeExecutionState,
            topics.get("recipe_state", "/recipe/state"),
            self.on_recipe_state,
            10,
        )

        canonical_topic = self.publisher_config.get("canonical_state_topic", "/lges/state_json")
        self.state_pub = self.node.create_publisher(String, canonical_topic, 10)
        self.recipe_query_client = self.node.create_client(
            QueryRecipeList,
            self.publisher_config.get("recipe_query_service", "/recipe/query_list"),
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

    def on_system_safety_state(self, msg):
        payload = _json_from_string_msg(msg)
        if isinstance(payload, dict):
            self.system_safety_state = _safe_text(
                payload.get("safetyState") or payload.get("safety_state") or payload.get("state")
            )
        else:
            self.system_safety_state = _safe_text(payload)

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
            self.refresh_recipe_list_if_needed(force=True)
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
                "Backend MQTT publish 대기: broker timeSync 수신/OS 시간 설정 전입니다."
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
        operation_state = self.resolve_operation_state()
        current_node_id = self.resolve_current_node_id()
        action_states = self.build_action_states(operation_state)
        recipe = self.recipe_state

        return {
            "orderId": _safe_text(getattr(recipe, "order_id", ""), "") or None,
            "orderUpdateId": int(getattr(recipe, "order_update_id", 0) or 0) or None,
            "lastNodeId": current_node_id,
            "lastNodeSeqNo": 1 if current_node_id else None,
            "nodeStates": [{"nodeId": current_node_id, "sequenceId": 1}] if current_node_id else [],
            "robotPosition": self.build_robot_position(),
            "velocity": self.build_velocity(),
            "driving": operation_state == "ACTIVE",
            "actionStates": action_states,
            "instantActionStates": [],
            "batteryState": self.build_battery_state(),
            "controlState": self.resolve_control_state(),
            "operationState": operation_state,
            "safetyState": self.resolve_safety_state(),
            "errors": self.normalize_errors(self.mir_errors or self.mir_state.get("errors")),
        }

    def build_visualization_payload(self):
        return {
            "referenceStateHeaderId": self.last_state_header_id,
            "robotPosition": self.build_robot_position(),
            "velocity": self.build_velocity(),
        }

    def build_factsheet_payload(self):
        nodes = self.build_nodes()
        recipes = self.build_recipes()
        actions = self.build_actions(nodes, recipes)
        return {
            "nodes": nodes,
            "recipes": recipes,
            "actions": actions,
            "mapInfo": {
                "mapId": _safe_text(self.mir_state.get("map_id"), "") or None,
                "mapVerNo": int(self.publisher_config.get("map_ver_no", 1)) if self.mir_state.get("map_id") else None,
                "mapData": [],
            },
        }

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

    def resolve_control_state(self):
        configured = (self.publisher_config.get("defaults", {}) or {}).get("control_state", "MANUAL")
        value = self.mir_state.get("mode_text") or self.mir_state.get("mode_id") or configured
        return _normalize_enum(value, CONTROL_STATES, configured)

    def resolve_operation_state(self):
        recipe_state = _safe_text(getattr(self.recipe_state, "operation_state", ""))
        if recipe_state:
            return _normalize_enum(recipe_state, OPERATION_STATES, "INIT")

        text = " ".join(_safe_text(self.mir_state.get(key)) for key in (
            "state_text",
            "state",
            "mission_text",
            "mode_text",
        )).lower()
        if self.mir_state.get("errors"):
            return "ERROR"
        if "emergency" in text or "estop" in text or "e-stop" in text:
            return "EMO"
        if "error" in text or "fault" in text:
            return "ERROR"
        if "pause" in text:
            return "PAUSED"
        if any(key in text for key in ("execut", "moving", "mission", "active", "running", "start", "charge", "dock")):
            return "ACTIVE"
        if any(key in text for key in ("ready", "wait", "standby", "idle")):
            return "READY"
        return (self.publisher_config.get("defaults", {}) or {}).get("operation_state", "INIT")

    def resolve_safety_state(self):
        default = (self.publisher_config.get("defaults", {}) or {}).get("safety_state", "NORMAL")
        value = _safe_text(self.system_safety_state or self.mir_state.get("safety_state") or self.mir_state.get("safetyState"), default)
        return value.upper()

    def resolve_current_node_id(self):
        max_age = float(self.publisher_config.get("current_waypoint_max_age_sec", 3.5))
        if not self.current_waypoint or (time.monotonic() - self.current_waypoint_at) > max_age:
            return None
        return _topic_safe_id(
            self.current_waypoint.get("name") or self.current_waypoint.get("guid") or self.current_waypoint.get("id"),
            "NODE_CURRENT",
        )

    def build_action_states(self, operation_state):
        action_list = list(self.recipe_action_list)
        recipe = self.recipe_state
        if recipe and _safe_text(getattr(recipe, "current_step_id", "")):
            return [{
                "actionList": action_list,
                "actionId": _topic_safe_id(getattr(recipe, "current_step_id", ""), "ACT_RECIPE"),
                "actionSeqNo": int(getattr(recipe, "current_step_index", 0) or 0) + 1,
                "actionType": _safe_text(getattr(recipe, "recipe_id", ""), "RECIPE"),
                "actionStatus": "ACTIVE" if operation_state == "ACTIVE" else operation_state,
                "actionResult": _safe_text(getattr(recipe, "message", ""), getattr(recipe, "execution_state", "")),
            }]
        if operation_state == "ACTIVE":
            return [{
                "actionList": action_list,
                "actionId": "ACT_MIR_NAVI",
                "actionSeqNo": 1,
                "actionType": "MIR_NAVI",
                "actionStatus": "ACTIVE",
                "actionResult": _safe_text(self.mir_state.get("mission_text"), "active"),
            }]
        if action_list:
            return [{"actionList": action_list}]
        return []

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
                "errorLevel": _normalize_enum(error.get("level") or error.get("severity"), ERROR_LEVELS, "ERROR"),
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
        for index, recipe in enumerate(self.recipe_action_list):
            recipe_id = recipe.get("actionId") if isinstance(recipe, dict) else str(recipe)
            recipes.append({
                "recipeId": _topic_safe_id(recipe_id, f"RCP_{index + 1:03d}"),
                "recipeSteps": [{"actionId": "ACT_MIR_NAVI", "actionSeqNo": 1}],
            })
        return recipes

    def build_actions(self, nodes, recipes):
        node_ids = [node.get("nodeId") for node in nodes if node.get("nodeId")]
        recipe_ids = [recipe.get("recipeId") for recipe in recipes if recipe.get("recipeId")]
        return [
            {
                "actionId": "ACT_MIR_NAVI",
                "actionType": "MIR_NAVI",
                "actionParamList": [
                    {"actionParamKey": "target_node", "actionParamType": "ENUM", "actionParamEnumList": node_ids},
                    {"actionParamKey": "mission", "actionParamType": "ENUM", "actionParamEnumList": recipe_ids},
                ],
            },
            {"actionId": "ACT_MIR_CLEAR_ERROR", "actionType": "MIR_CLEAR_ERROR", "actionParamList": []},
            {"actionId": "ACT_REQUEST_FACTSHEET", "actionType": "request_factsheet", "actionParamList": []},
        ]

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
