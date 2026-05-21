#!/usr/bin/env python3
import hashlib
import json
import fcntl
import os
import sys
import yaml
import time
import threading
import signal

import rclpy
from rclpy.clock import Clock, ClockType
from rclpy.node import Node
from rclpy.signals import SignalHandlerOptions
from std_srvs.srv import Trigger
from std_msgs.msg import String
from lges_recipe_interfaces.srv import RunRecipe, SendInstantAction

# 커스텀 인터페이스 로드
from comm_interfaces.msg import ConnectionState
from comm_interfaces.srv import GetMqttJsonTemplates, PublishMqttJson, TriggerReconnect

# 엔진(Core) 모듈 로드
from .wifi_core import WifiCore
from .mqtt_core import MqttCore
from .payload_validator import InboundPayloadValidator
from .backend_publisher import BackendMqttPublisher
from .config_loader import load_config_file

from ament_index_python.packages import get_package_share_directory

_INSTANCE_LOCK_FILE = None
RESPONSE_TYPES = {"ACCEPTED", "REJECTED", "ERROR"}
DEFAULT_INSTANT_ACTION_ROUTES = {
    "start_pause": {"action_id": "IA_START_PAUSE_001"},
    "stop_pause": {"action_id": "IA_STOP_PAUSE_001"},
    "start_charge": {"action_id": "IA_START_CHARGE_001"},
    "stop_charge": {"action_id": "IA_STOP_CHARGE_001"},
    "cancel_order": {"action_id": "IA_CANCEL_ORDER_001"},
    "clear_instant_actions": {"action_id": "IA_CLEAR_INSTANT_ACTIONS_001"},
    "request_factsheet": {"action_id": "IA_REQUEST_FACTSHEET_001", "mode": "publish_backend"},
}

def resolve_config_path():
    env_path = os.environ.get('COMM_MANAGER_CONFIG_PATH')
    if env_path:
        expanded = os.path.expanduser(env_path)
        if os.path.exists(expanded):
            return expanded

    package_share_dir = get_package_share_directory('comm_manager')
    workspace_root = os.path.abspath(os.path.join(package_share_dir, '..', '..', '..', '..'))
    source_path = os.path.join(workspace_root, 'src', 'comm_manager', 'config', 'config.yaml')
    share_path = os.path.join(package_share_dir, 'config', 'config.yaml')

    for path in (source_path, share_path):
        if os.path.exists(path):
            return path

    return share_path

def load_preinit_config():
    """Load config before rclpy.init for the single-instance lock."""
    try:
        path = resolve_config_path()
        return load_config_file(path)
    except Exception:
        return {}

def acquire_single_instance_lock(config):
    """Prevent multiple comm_node instances from fighting over one MQTT client id."""
    global _INSTANCE_LOCK_FILE

    robot_config = config.get('robot', {}) if isinstance(config, dict) else {}
    serial = str(robot_config.get('serial_number') or 'AMR-001')
    runtime_config = config.get('runtime', {}) if isinstance(config, dict) else {}
    lock_path = runtime_config.get('lock_file') or f"/tmp/lges_comm_node_{serial}.lock"

    lock_file = open(lock_path, 'w')
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(
            f"[comm_node] another comm_node instance is already running "
            f"for serial={serial}. lock={lock_path}",
            file=sys.stderr
        )
        lock_file.close()
        return False

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(f"{os.getpid()}\n")
    lock_file.flush()
    _INSTANCE_LOCK_FILE = lock_file
    return True

class CommNode(Node):
    def __init__(self):
        super().__init__('comm_node')
        self.steady_clock = Clock(clock_type=ClockType.STEADY_TIME)
        self.config_path = ""
        
        # [1] 설정 파일 로드
        self.config = self.load_config()
        
        # [2] 코어 모듈 초기화 (의존성 주입)
        self.wifi_core = WifiCore(self.config, self.get_logger())
        self.mqtt_core = MqttCore(self.config, self.get_logger())

        # [2-1] Host inbound payload 검증기 초기화. YAML 저장은 comm_debug_tools로 분리했다.
        self.payload_validator = InboundPayloadValidator(logger=self.get_logger())
        
        # [3] MQTT 콜백 연결
        self.mqtt_core.on_connect_callback = self.on_mqtt_connect
        self.mqtt_core.on_disconnect_callback = self.on_mqtt_disconnect
        self.mqtt_core.on_message_callback = self.on_mqtt_message
        self.mqtt_core.on_time_sync_callback = self.on_broker_time_sync
        
        # [4] ROS 2 인터페이스 세팅
        self.state_pub = self.create_publisher(ConnectionState, '/connection_state', 10)
        time_sync_topic = str(self.config.get('time_sync', {}).get('ros_state_topic', '/comm/time_sync_state'))
        self.time_sync_pub = self.create_publisher(String, time_sync_topic, 10)
        self.reconnect_srv = self.create_service(TriggerReconnect, '/trigger_reconnect', self.srv_reconnect_cb)
        self.publish_mqtt_srv = self.create_service(
            PublishMqttJson,
            '/comm/publish_mqtt_json',
            self.srv_publish_mqtt_json_cb
        )
        self.mqtt_templates_srv = self.create_service(
            GetMqttJsonTemplates,
            '/comm/get_mqtt_json_templates',
            self.srv_get_mqtt_json_templates_cb
        )
        
        # 상태 변수
        self.comm_state = "DISCONNECTED"
        self.active_ssid = ""
        self.signal_level = 0
        self.signal_status = "Disconnected"
        self.last_time_sync_state_payload = None
        self.time_sync_state_lock = threading.Lock()
        self.is_shutting_down = False
        self.connection_lock = threading.Lock()
        self.connection_thread = None
        self.suppress_next_disconnect_reconnect = False
        
        self.max_retries = self.config.get('mqtt', {}).get('max_retries', 5)
        self.retry_interval = self.config.get('mqtt', {}).get('retry_interval', 5)
        self.reconnect_backoff_config = self.config.get('mqtt', {}).get('reconnect_backoff', {}) or {}
        self.retry_backoff_multiplier = float(self.reconnect_backoff_config.get('multiplier', 1.5))
        self.retry_interval_max = float(self.reconnect_backoff_config.get('max_delay_sec', max(float(self.retry_interval), 30.0)))
        self.manage_wifi = self.config.get('network', {}).get('manage_wifi', True)
        self.publish_connection_on_mqtt_connect = self.config.get('mqtt', {}).get('publish_connection_on_mqtt_connect', True)
        self.inbound_config = self.config.get('inbound', {})
        self.inbound_auto_response = self.inbound_config.get('auto_response', True)
        self.inbound_service_timeout_sec = float(self.inbound_config.get('service_timeout_sec', 3.0))
        self.inbound_service_wait_timeout_sec = float(self.inbound_config.get('service_wait_timeout_sec', 0.5))
        self.inbound_triggers = self.inbound_config.get('triggers', {}) or {}
        self.response_config = self.inbound_config.get('response', {}) or {}
        self.inbound_trigger_clients = self.create_inbound_trigger_clients()
        self.order_config = self.inbound_config.get('order', {}) or {}
        self.order_enabled = bool(self.order_config.get('enabled', True))
        self.order_service_name = str(self.order_config.get('service', '/recipe/run'))
        self.order_timeout_sec = float(self.order_config.get('timeout_sec', 0.0))
        self.order_request_mapping = self.order_config.get('request_mapping') or {}
        self.order_sequence_config = self.order_config.get('sequence', {}) or {}
        self.order_require_monotonic_update_id = bool(
            self.order_sequence_config.get('require_monotonic_update_id', True)
        )
        self.order_deduplicate = bool(self.order_sequence_config.get('deduplicate', True))
        self.order_sequence_lock = threading.Lock()
        self.order_history = {}
        self.order_inflight = {}
        self.order_client = self.create_client(RunRecipe, self.order_service_name)
        self.instant_action_config = self.inbound_config.get('instant_actions', {}) or {}
        self.instant_action_enabled = bool(self.instant_action_config.get('enabled', True))
        self.instant_action_service_name = str(
            self.instant_action_config.get('service', '/recipe/instant_action')
        )
        self.instant_action_default_input_json = str(
            self.instant_action_config.get('default_input_json', '{}')
        )
        self.instant_action_action_id_source = str(
            self.instant_action_config.get('action_id_source', 'route') or 'route'
        ).strip().lower()
        self.instant_action_request_mapping = self.instant_action_config.get('request_mapping') or {}
        configured_routes = self.instant_action_config.get('routes') or {}
        self.instant_action_routes = self.normalize_instant_action_routes(configured_routes)
        self.instant_action_fail_fast = bool(self.instant_action_config.get('fail_fast', True))
        self.instant_action_clients = {}
        self.outbound_config = self.config.get('outbound', {}) or {}
        self.visualization_skip_logged = False
        self.visualization_retained_clear_sent = False
        self.backend_publisher = BackendMqttPublisher(self, self.mqtt_core, self.config)
        self.retained_policy_config = self.config.get('mqtt', {}).get('retained', {}) or {}
        time_sync_config = self.config.get('time_sync', {}) or {}
        self.time_sync_ros_state_publish_period_sec = float(
            time_sync_config.get('ros_state_publish_period_sec', 1.0) or 0.0
        )
        
        # [5] UI용 상태 퍼블리시 타이머 (1초 주기)
        self.timer = self.create_timer(1.0, self.publish_ros_state, clock=self.steady_clock)
        self.time_sync_state_timer = None
        if self.time_sync_ros_state_publish_period_sec > 0:
            self.time_sync_state_timer = self.create_timer(
                self.time_sync_ros_state_publish_period_sec,
                self.publish_last_time_sync_state,
                clock=self.steady_clock
            )
        
        # [6] 메인 백그라운드 스레드 시작
        threading.Thread(target=self.main_control_loop, daemon=True).start()

    def load_config(self):
        try:
            path = resolve_config_path()
            self.config_path = path
            config = load_config_file(path)
            self.get_logger().info(f"Config 로드: {path}")
            includes = config.get("_loaded_includes")
            if includes:
                self.get_logger().info(f"Config include 로드: {includes}")
            return config
        except Exception as e:
            self.get_logger().error(f"Config 로드 실패, 기본값 사용: {e}")
            return {
                "network": {"ssid": "DEFAULT", "password": "", "min_signal_threshold": 40},
                "mqtt": {"broker_ip": "127.0.0.1", "port": 1883, "max_retries": 5},
                "robot": {"manufacturer": "MANU", "serial_number": "AMR-001", "version": "2.0.0"}
            }

    # ==========================================
    # 메인 제어 및 연결 프로세스
    # ==========================================
    def main_control_loop(self):
        self.get_logger().info("▶ [1] 통신 매니저 부팅 완료. 연결 프로세스 시작.")
        self.connect_full_process()
        
        while rclpy.ok() and not self.is_shutting_down and self.comm_state != "CONNECTION FAILED":
            if not self.manage_wifi:
                self.active_ssid = self.config.get('network', {}).get('ssid', 'UNMANAGED')
                self.signal_level = 100
                self.signal_status = "Unmanaged"
                time.sleep(5.0)
                continue

            # [수정] 4개의 값을 받도록 status_str 변수 추가
            wifi_ok, ssid, sig, status_str = self.wifi_core.monitor_and_roam()
            
            self.active_ssid = ssid
            self.signal_level = sig
            self.signal_status = status_str  # <--- 새로 추가 (UI 전송용)
            
            if not wifi_ok:
                self.comm_state = "DISCONNECTED"
                
            time.sleep(5.0)

    def start_connection_thread(self, reason=""):
        if self.is_shutting_down:
            return False

        if self.connection_thread and self.connection_thread.is_alive():
            self.get_logger().warn(f"MQTT 연결 작업이 이미 진행 중입니다. 요청 무시: {reason}")
            return False

        self.connection_thread = threading.Thread(
            target=self.connect_full_process,
            kwargs={"reason": reason},
            daemon=True,
            name="comm-mqtt-connect"
        )
        self.connection_thread.start()
        return True

    def connect_full_process(self, reason=""):
        if self.is_shutting_down:
            return

        if not self.connection_lock.acquire(blocking=False):
            self.get_logger().warn("MQTT 연결/재연결이 이미 진행 중입니다.")
            return

        try:
            self.comm_state = "CONNECTING"
            
            if self.manage_wifi:
                wifi_ok = self.wifi_core.connect_initial()
                if not wifi_ok:
                    self.comm_state = "CONNECTION FAILED"
                    return
            else:
                self.get_logger().info("Wi-Fi 관리 비활성화: 현재 네트워크로 MQTT 접속만 시도합니다.")
                
            retry_count = 0
            while retry_count < self.max_retries and not self.is_shutting_down:
                self.get_logger().info(f"▶ [2] MQTT 서버 접속 시도... ({retry_count + 1}/{self.max_retries})")
                
                if self.mqtt_core.connect():
                    return 
                    
                retry_count += 1
                if retry_count < self.max_retries and not self.is_shutting_down:
                    delay = min(
                        self.retry_interval_max,
                        float(self.retry_interval) * (self.retry_backoff_multiplier ** max(retry_count - 1, 0))
                    )
                    self.get_logger().info(f"MQTT 재접속 backoff 대기: {delay:.1f}s")
                    time.sleep(delay)

            if self.is_shutting_down:
                return
                    
            self.comm_state = "CONNECTION FAILED"
            self.get_logger().fatal("최대 재시도 횟수 초과. 통신 영구 실패.")
        finally:
            self.connection_lock.release()

    # ==========================================
    # [업그레이드 1] Graceful Shutdown (노드 종료 시 단절 알림)
    # ==========================================
    def destroy_node(self):
        self.get_logger().info("노드 종료 절차 시작: DISCONNECTED 상태 전파")
        self.is_shutting_down = True
        self.comm_state = "DISCONNECTED"
        
        try:
            try:
                self.timer.cancel()
            except Exception:
                pass

            # 1. ROS 2 UI용 상태 토픽 발행
            msg = ConnectionState()
            msg.state = "DISCONNECTED"
            msg.active_ssid = self.active_ssid
            msg.signal_status = self.signal_status
            msg.signal_strength = self.signal_level
            self.state_pub.publish(msg)
            
            # 2. 종료 정책에 따라 retained payload 정리 후 MQTT (Host)로 OFFLINE 전송
            self.clear_retained_topics("clear_on_normal_shutdown")
            self.mqtt_core.disconnect(normal=True)
            
            # [핵심] 메시지가 실제로 네트워크로 나갈 수 있도록 아주 잠깐 대기
            # ROS 2 버퍼가 비워질 시간을 줍니다.
            time.sleep(0.5) 

            if self.connection_thread and self.connection_thread.is_alive():
                self.connection_thread.join(timeout=1.0)
            
        except Exception as e:
            self.get_logger().error(f"종료 절차 중 오류 발생: {e}")

        # 3. 부모 클래스의 소멸자 호출
        super().destroy_node()

    def clear_retained_topics(self, policy_key):
        topics = self.retained_policy_config.get(policy_key, []) if isinstance(self.retained_policy_config, dict) else []
        if isinstance(topics, str):
            topics = [topics]
        if not isinstance(topics, list):
            return
        qos = int(self.retained_policy_config.get('clear_qos', 1) or 0)
        for topic_suffix in topics:
            suffix = str(topic_suffix or '').strip().strip('/')
            if not suffix:
                continue
            try:
                self.mqtt_core.clear_retained(suffix, qos=qos)
            except Exception as e:
                self.get_logger().warn(f"Retained 삭제 실패 [{suffix}]: {e}")

    # ==========================================
    # ROS 2 ↔ UI 브릿지 역할
    # ==========================================
    def publish_ros_state(self):
        msg = ConnectionState()
        msg.state = self.comm_state
        msg.active_ssid = self.active_ssid
        msg.signal_status = self.signal_status
        msg.signal_strength = self.signal_level
        self.state_pub.publish(msg)
        
        if self.comm_state == "CONNECTION FAILED":
            self.timer.cancel() 

    def on_broker_time_sync(self, state_payload):
        with self.time_sync_state_lock:
            self.last_time_sync_state_payload = dict(state_payload or {})
        self.publish_time_sync_state_payload(state_payload)

    def publish_last_time_sync_state(self):
        with self.time_sync_state_lock:
            payload = dict(self.last_time_sync_state_payload or {})
        if not payload:
            return
        self.publish_time_sync_state_payload(payload)

    def publish_time_sync_state_payload(self, state_payload):
        msg = String()
        msg.data = json.dumps(state_payload or {}, ensure_ascii=False)
        self.time_sync_pub.publish(msg)

    def create_inbound_trigger_clients(self):
        clients = {}
        for key, service_name in self.inbound_triggers.items():
            if not service_name:
                continue
            clients[key] = self.create_client(Trigger, str(service_name))
        return clients

    def inbound_message_type_from_topic(self, topic):
        topic_lower = str(topic or '').lower()
        if topic_lower.endswith('/order'):
            return 'order'
        if topic_lower.endswith('/instantactions'):
            return 'instantActions'
        return ''

    def get_inbound_trigger_client(self, message_type):
        service_name = (
            self.inbound_triggers.get(message_type)
            or self.inbound_triggers.get(str(message_type).lower())
        )
        if not service_name:
            return None, ''

        client = (
            self.inbound_trigger_clients.get(message_type)
            or self.inbound_trigger_clients.get(str(message_type).lower())
        )
        if client is None:
            client = self.create_client(Trigger, str(service_name))
            self.inbound_trigger_clients[message_type] = client
        return client, str(service_name)

    def classify_trigger_failure(self, message):
        text = str(message or '').lower()
        patterns = self.response_config.get('service_error_message_patterns', []) or []
        return any(str(pattern).lower() in text for pattern in patterns)

    def call_inbound_route(self, message_type, payload):
        if message_type == 'order' and self.order_enabled:
            return self.call_order_service(payload)
        if message_type == 'instantActions' and self.instant_action_enabled:
            return self.call_instant_actions_service(payload)
        return self.call_inbound_trigger(message_type)

    def record_backend_event(self, event):
        publisher = getattr(self, "backend_publisher", None)
        if publisher is None or not hasattr(publisher, "record_inbound_event"):
            return
        try:
            publisher.record_inbound_event(event)
        except Exception as exc:
            self.get_logger().warn(f"Inbound history record failed: {exc}")

    def record_order_history_event(self, payload, success, message, reason, request=None):
        payload = payload if isinstance(payload, dict) else {}
        order_id = (
            getattr(request, "order_id", None)
            if request is not None
            else payload.get("orderId")
        )
        order_type = (
            getattr(request, "order_type", None)
            if request is not None
            else payload.get("orderType")
        )
        recipe_id = (
            getattr(request, "recipe_id", None)
            if request is not None
            else payload.get("recipeId")
        )
        update_id = (
            getattr(request, "order_update_id", None)
            if request is not None
            else payload.get("orderUpdateId")
        )
        reason_text = str(reason or "")
        error_reasons = {"validation_error", "service_error", "service_timeout", "service_exception"}
        status = "ACCEPTED" if success else ("ERROR" if reason_text in error_reasons else "REJECTED")
        description_parts = [
            _part for _part in (str(order_type or "").upper(), str(recipe_id or "")) if _part
        ]
        self.record_backend_event({
            "kind": "order",
            "actionId": order_id,
            "actionSeqNo": update_id,
            "actionType": f"ORDER_{str(order_type or 'UNKNOWN').upper()}",
            "actionDescription": " ".join(description_parts),
            "actionStatus": status,
            "actionResult": str(message or ""),
        })

    def record_instant_action_history_event(self, action, action_id, action_type, success, message, reason):
        reason_text = str(reason or "")
        error_reasons = {"validation_error", "service_error", "service_timeout", "service_exception"}
        status = "ACCEPTED" if success else ("ERROR" if reason_text in error_reasons else "REJECTED")
        self.record_backend_event({
            "kind": "instantAction",
            "actionId": action_id or (action or {}).get("actionId"),
            "actionSeqNo": (action or {}).get("actionSeqNo"),
            "actionType": action_type or (action or {}).get("actionType"),
            "actionDescription": str((action or {}).get("actionDescription") or ""),
            "actionStatus": status,
            "actionResult": str(message or ""),
        })

    def run_recipe_result_reason(self, result, message):
        response_type = str(getattr(result, 'response_type', '') or '').lower()
        result_code = str(getattr(result, 'result_code', '') or '').lower()
        if bool(getattr(result, 'accepted', False)):
            return "service_accepted"
        if response_type == "error" or self.classify_trigger_failure(f"{result_code} {message}"):
            return "service_error"
        return "service_rejected"

    def first_payload_value(self, payload, fields, default=None):
        for field in fields or []:
            current = payload
            for part in str(field).split("."):
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    current = None
                if current is None:
                    break
            if current not in (None, ""):
                return current
        return default

    def config_path_value(self, path, default=None):
        current = self.config
        for part in str(path or "").split("."):
            if not part:
                continue
            if not isinstance(current, dict):
                return default
            current = current.get(part)
            if current is None:
                return default
        return current

    def transform_mapped_value(self, value, transform):
        transform = str(transform or "").strip().lower()
        if not transform:
            return value
        if transform == "int":
            return int(value or 0)
        if transform == "float":
            return float(value or 0.0)
        if transform == "upper":
            return str(value or "").upper()
        if transform == "lower":
            return str(value or "").lower()
        if transform == "string":
            return str(value if value is not None else "")
        if transform == "json_dumps":
            return json.dumps(value, ensure_ascii=False)
        return value

    def normalize_instant_action_type(self, value):
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        if raw in self.instant_action_routes:
            return raw
        compact = "".join(raw.split())
        if compact in self.instant_action_routes:
            return compact
        for action_type in self.instant_action_routes:
            compact_action_type = action_type.replace("_", "")
            if compact == action_type + action_type:
                return action_type
            if compact == compact_action_type:
                return action_type
            if compact == compact_action_type + compact_action_type:
                return action_type
        return raw

    def resolve_request_mapping_value(self, spec, payload, context=None):
        context = context or {}
        spec = spec if isinstance(spec, dict) else {"source": "literal", "value": spec}
        source = str(spec.get("source", "payload") or "payload").strip()
        default = spec.get("default")

        if source == "payload":
            value = self.first_payload_value(payload, spec.get("fields") or spec.get("payload_fields") or [], default)
        elif source == "payload_json":
            value = json.dumps(payload or {}, ensure_ascii=False)
        elif source == "config":
            value = self.config_path_value(spec.get("path"), default)
        elif source == "route_or_payload":
            route = context.get("route") if isinstance(context.get("route"), dict) else {}
            if self.instant_action_action_id_source == "payload":
                value = self.first_payload_value(payload, spec.get("payload_fields") or [], None)
                if value in (None, ""):
                    value = route.get(spec.get("route_field", "action_id"))
            else:
                value = route.get(spec.get("route_field", "action_id"))
                if value in (None, ""):
                    value = self.first_payload_value(payload, spec.get("payload_fields") or [], None)
            if value in (None, ""):
                value = default
        elif source == "instant_action_input_json":
            value = self.build_instant_action_input_json(payload)
        elif source == "literal":
            value = spec.get("value", default)
        else:
            value = default

        return self.transform_mapped_value(value, spec.get("transform"))

    def apply_service_request_mapping(self, request, mapping, payload, context=None):
        for request_field, spec in (mapping or {}).items():
            if not hasattr(request, request_field):
                self.get_logger().warn(f"Service request field not found, skipped: {request_field}")
                continue
            try:
                setattr(request, request_field, self.resolve_request_mapping_value(spec, payload, context))
            except Exception as exc:
                raise ValueError(f"failed to map service request field '{request_field}': {exc}") from exc

    def call_order_service(self, payload):
        sequence_ok, sequence_message, duplicate = self.validate_order_sequence(payload)
        if not sequence_ok:
            self.record_order_history_event(payload, False, sequence_message, "service_rejected")
            return False, sequence_message, "service_rejected"
        if duplicate:
            return True, sequence_message, "service_accepted"

        if self.order_client is None:
            self.record_order_history_event(payload, False, "order service is not configured", "service_not_configured")
            return False, "order service is not configured", "service_not_configured"
        if not self.order_client.wait_for_service(timeout_sec=self.inbound_service_wait_timeout_sec):
            message = f"service unavailable: {self.order_service_name}"
            self.record_order_history_event(payload, False, message, "service_unavailable")
            return False, message, "service_unavailable"

        request = RunRecipe.Request()
        mapping = self.order_request_mapping or {
            "execution_id": {"source": "payload", "fields": ["executionId", "execution_id"], "default": ""},
            "order_id": {"source": "payload", "fields": ["orderId"], "default": ""},
            "order_update_id": {"source": "payload", "fields": ["orderUpdateId"], "transform": "int", "default": 0},
            "order_type": {"source": "payload", "fields": ["orderType"], "transform": "upper", "default": "AUTO"},
            "recipe_id": {"source": "payload", "fields": ["recipeId"], "default": ""},
            "input_json": {"source": "payload_json"},
            "timeout_sec": {"source": "literal", "value": self.order_timeout_sec},
        }
        self.apply_service_request_mapping(request, mapping, payload)

        self.get_logger().info(
            "Order service call -> "
            f"{self.order_service_name}: "
            f"order_id={request.order_id}, update_id={request.order_update_id}, "
            f"order_type={request.order_type}, recipe_id={request.recipe_id}"
        )

        done = threading.Event()
        future = self.order_client.call_async(request)
        future.add_done_callback(lambda _future: done.set())

        if not done.wait(timeout=self.inbound_service_timeout_sec):
            self.clear_order_inflight(payload)
            message = f"service timeout: {self.order_service_name}"
            self.record_order_history_event(payload, False, message, "service_timeout", request=request)
            return False, message, "service_timeout"

        try:
            result = future.result()
        except Exception as e:
            self.clear_order_inflight(payload)
            message = f"service exception: {e}"
            self.record_order_history_event(payload, False, message, "service_exception", request=request)
            return False, message, "service_exception"

        accepted = bool(getattr(result, 'accepted', False))
        response_type = str(getattr(result, 'response_type', '') or '')
        result_code = str(getattr(result, 'result_code', '') or '')
        message = str(getattr(result, 'message', '') or '')
        summary = f"{request.order_id}: {response_type or ('accepted' if accepted else 'rejected')}"
        if result_code:
            summary += f" ({result_code})"
        if message:
            summary += f" - {message}"
        reason = self.run_recipe_result_reason(result, message)
        if accepted:
            self.remember_order(payload)
        self.clear_order_inflight(payload)
        self.record_order_history_event(payload, accepted, summary, reason, request=request)
        return accepted, summary, reason

    def order_payload_hash(self, payload):
        body = {
            key: value
            for key, value in (payload or {}).items()
            if key not in ("headerId", "timestamp")
        }
        raw = json.dumps(body, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    def validate_order_sequence(self, payload):
        if not self.order_deduplicate and not self.order_require_monotonic_update_id:
            return True, "", False

        order_id = str(payload.get("orderId") or "").strip()
        try:
            update_id = int(payload.get("orderUpdateId"))
        except (TypeError, ValueError):
            return False, "invalid orderUpdateId: must be an integer", False

        if not order_id:
            return False, "invalid orderId: must not be empty", False
        if update_id < 0:
            return False, "invalid orderUpdateId: must be >= 0", False

        payload_hash = self.order_payload_hash(payload)
        order_key = (order_id, update_id)
        with self.order_sequence_lock:
            inflight_hash = self.order_inflight.get(order_key)
            if inflight_hash:
                if self.order_deduplicate and payload_hash == inflight_hash:
                    return True, f"duplicate in-flight order ignored: orderId={order_id}, orderUpdateId={update_id}", True
                return False, (
                    f"conflicting in-flight orderUpdateId: orderId={order_id}, "
                    f"orderUpdateId={update_id}"
                ), False

            previous = self.order_history.get(order_id)
            if not previous:
                self.order_inflight[order_key] = payload_hash
                return True, "", False

            previous_update_id = int(previous.get("orderUpdateId", -1))
            previous_hash = previous.get("payloadHash")

            if update_id == previous_update_id:
                if self.order_deduplicate and payload_hash == previous_hash:
                    return True, f"duplicate order ignored: orderId={order_id}, orderUpdateId={update_id}", True
                return False, (
                    f"non-monotonic orderUpdateId: orderId={order_id}, "
                    f"received={update_id}, previous={previous_update_id}"
                ), False

            if self.order_require_monotonic_update_id and update_id < previous_update_id:
                return False, (
                    f"non-monotonic orderUpdateId: orderId={order_id}, "
                    f"received={update_id}, previous={previous_update_id}"
                ), False

            self.order_inflight[order_key] = payload_hash
            return True, "", False

    def remember_order(self, payload):
        order_id = str(payload.get("orderId") or "").strip()
        if not order_id:
            return
        try:
            update_id = int(payload.get("orderUpdateId"))
        except (TypeError, ValueError):
            return
        with self.order_sequence_lock:
            self.order_history[order_id] = {
                "orderUpdateId": update_id,
                "payloadHash": self.order_payload_hash(payload),
                "rememberedAt": time.time(),
            }

    def clear_order_inflight(self, payload):
        order_id = str(payload.get("orderId") or "").strip()
        try:
            update_id = int(payload.get("orderUpdateId"))
        except (TypeError, ValueError):
            return
        with self.order_sequence_lock:
            self.order_inflight.pop((order_id, update_id), None)

    def normalize_instant_action_routes(self, routes_config):
        routes = {}
        source = routes_config if isinstance(routes_config, dict) and routes_config else DEFAULT_INSTANT_ACTION_ROUTES
        for action_type, route in source.items():
            normalized_type = str(action_type or '').strip().lower()
            if not normalized_type:
                continue
            if isinstance(route, dict):
                normalized_route = dict(route)
            else:
                normalized_route = {"action_id": str(route)}
            normalized_route.setdefault("action_id", DEFAULT_INSTANT_ACTION_ROUTES.get(normalized_type, {}).get("action_id", ""))
            normalized_route.setdefault("service", self.instant_action_service_name)
            routes[normalized_type] = normalized_route
        return routes

    def get_instant_action_client(self, service_name):
        if not service_name:
            return None
        client = self.instant_action_clients.get(service_name)
        if client is None:
            client = self.create_client(SendInstantAction, service_name)
            self.instant_action_clients[service_name] = client
        return client

    def build_instant_action_input_json(self, action):
        direct_input = (
            action.get("input_json")
            if "input_json" in action
            else action.get("inputJson")
        )
        if direct_input is not None:
            if isinstance(direct_input, str):
                return direct_input.strip() or self.instant_action_default_input_json
            return json.dumps(direct_input, ensure_ascii=False)

        params = None
        for key in ("actionParameters", "actionParams", "parameters"):
            if key in action:
                params = action.get(key)
                break

        if params in (None, [], {}):
            return self.instant_action_default_input_json

        return json.dumps({"actionParameters": params}, ensure_ascii=False)

    def instant_action_result_reason(self, result, message):
        response_type = str(getattr(result, 'response_type', '') or '').lower()
        result_code = str(getattr(result, 'result_code', '') or '').lower()
        if bool(getattr(result, 'accepted', False)):
            return "service_accepted"
        if response_type == "error" or self.classify_trigger_failure(f"{result_code} {message}"):
            return "service_error"
        return "service_rejected"

    def call_one_instant_action_service(self, action, index):
        raw_action_type = action.get("actionType") or action.get("action_type") or ""
        action_type = self.normalize_instant_action_type(raw_action_type)
        mapped_action = dict(action or {})
        mapped_action["actionType"] = action_type
        mapped_action["action_type"] = action_type
        route = self.instant_action_routes.get(action_type, {})
        source_action_id = str(
            action.get("actionId")
            or action.get("action_id")
        ).strip()
        route_action_id = str(route.get("action_id") or "").strip()
        action_id = (
            source_action_id
            if self.instant_action_action_id_source == "payload"
            else (route_action_id or source_action_id)
        )
        blocking_type = str(
            action.get("blockingType")
            or action.get("blocking_type")
            or ""
        ).strip()
        service_name = str(route.get("service") or self.instant_action_service_name).strip()

        if not source_action_id:
            message = f"instantActions[{index}] missing actionId"
            self.record_instant_action_history_event(action, source_action_id, action_type, False, message, "service_rejected")
            return False, message, "service_rejected"
        if not action_type:
            message = f"instantActions[{index}] missing actionType"
            self.record_instant_action_history_event(action, source_action_id, action_type, False, message, "service_rejected")
            return False, message, "service_rejected"
        if action_type not in self.instant_action_routes:
            message = f"unsupported instant action type: {action_type}"
            self.record_instant_action_history_event(action, source_action_id, action_type, False, message, "service_rejected")
            return False, message, "service_rejected"
        route_mode = str(route.get("mode") or "").strip().lower()
        if action_type == "request_factsheet" or route_mode in ("publish_backend", "publish_factsheet"):
            success, message, reason = self.handle_request_factsheet(mapped_action, source_action_id, action_id, action_type)
            self.record_instant_action_history_event(mapped_action, action_id, action_type, success, message, reason)
            return success, message, reason

        client = self.get_instant_action_client(service_name)
        if client is None:
            message = "instant action service is not configured"
            self.record_instant_action_history_event(mapped_action, action_id, action_type, False, message, "service_not_configured")
            return False, message, "service_not_configured"
        if not client.wait_for_service(timeout_sec=self.inbound_service_wait_timeout_sec):
            message = f"service unavailable: {service_name}"
            self.record_instant_action_history_event(mapped_action, action_id, action_type, False, message, "service_unavailable")
            return False, message, "service_unavailable"

        request = SendInstantAction.Request()
        mapping = self.instant_action_request_mapping or {
            "action_id": {
                "source": "literal",
                "value": action_id,
            },
            "action_type": {
                "source": "payload",
                "fields": ["actionType", "action_type"],
                "transform": "lower",
                "default": action_type,
            },
            "blocking_type": {
                "source": "payload",
                "fields": ["blockingType", "blocking_type"],
                "default": blocking_type,
            },
            "input_json": {
                "source": "instant_action_input_json",
            },
        }
        self.apply_service_request_mapping(request, mapping, mapped_action, context={"route": route})
        action_id = request.action_id
        action_type = request.action_type

        self.get_logger().info(
            "InstantAction service call -> "
            f"{service_name}: "
            f"action_id={request.action_id}, action_type={request.action_type}, "
            f"blocking_type={request.blocking_type}, input_json={request.input_json}"
        )

        done = threading.Event()
        future = client.call_async(request)
        future.add_done_callback(lambda _future: done.set())

        if not done.wait(timeout=self.inbound_service_timeout_sec):
            message = f"service timeout: {service_name}"
            self.record_instant_action_history_event(mapped_action, action_id, action_type, False, message, "service_timeout")
            return False, message, "service_timeout"

        try:
            result = future.result()
        except Exception as e:
            message = f"service exception: {e}"
            self.record_instant_action_history_event(mapped_action, action_id, action_type, False, message, "service_exception")
            return False, message, "service_exception"

        accepted = bool(getattr(result, 'accepted', False))
        response_type = str(getattr(result, 'response_type', '') or '')
        result_code = str(getattr(result, 'result_code', '') or '')
        message = str(getattr(result, 'message', '') or '')
        summary = (
            f"{action_id}/{action_type}: "
            f"{response_type or ('accepted' if accepted else 'rejected')}"
        )
        if result_code:
            summary += f" ({result_code})"
        if message:
            summary += f" - {message}"

        reason = self.instant_action_result_reason(result, message)
        self.record_instant_action_history_event(mapped_action, action_id, action_type, accepted, summary, reason)
        return accepted, summary, reason

    def handle_request_factsheet(self, action, source_action_id, mapped_action_id, action_type):
        if not self.backend_publisher or not self.backend_publisher.can_publish_factsheet():
            return False, "backend factsheet publisher is not enabled", "service_not_configured"

        success, message = self.backend_publisher.publish_factsheet(
            force=True,
            reason=f"{mapped_action_id}/{action_type}",
        )
        return (
            success,
            message if message else f"{mapped_action_id}/{action_type}: factsheet published",
            "service_accepted" if success else "service_error",
        )

    def call_instant_actions_service(self, payload):
        actions = payload.get("instantActions") if isinstance(payload, dict) else None
        if not isinstance(actions, list) or not actions:
            return False, "instantActions must be a non-empty array", "service_rejected"

        messages = []
        final_reason = "service_accepted"
        all_success = True

        for index, action in enumerate(actions, start=1):
            if not isinstance(action, dict):
                all_success = False
                final_reason = "service_rejected"
                messages.append(f"instantActions[{index}] must be an object")
                if self.instant_action_fail_fast:
                    break
                continue

            success, message, reason = self.call_one_instant_action_service(action, index)
            messages.append(message)
            if not success:
                all_success = False
                final_reason = reason
                if self.instant_action_fail_fast:
                    break
            elif final_reason == "service_accepted":
                final_reason = reason

        return all_success, "; ".join(messages), final_reason

    def call_inbound_trigger(self, message_type):
        client, service_name = self.get_inbound_trigger_client(message_type)
        if client is None:
            return False, f"trigger service is not configured for {message_type}", "service_not_configured"

        if not client.wait_for_service(timeout_sec=self.inbound_service_wait_timeout_sec):
            return False, f"trigger service unavailable: {service_name}", "service_unavailable"

        done = threading.Event()
        future = client.call_async(Trigger.Request())
        future.add_done_callback(lambda _future: done.set())

        if not done.wait(timeout=self.inbound_service_timeout_sec):
            return False, f"trigger service timeout: {service_name}", "service_timeout"

        try:
            result = future.result()
        except Exception as e:
            return False, f"trigger service exception: {e}", "service_exception"

        success = bool(getattr(result, 'success', False))
        message = getattr(result, 'message', '') or (
            f"trigger accepted: {service_name}" if success else f"trigger rejected: {service_name}"
        )
        if success:
            return True, message, "service_accepted"
        if self.classify_trigger_failure(message):
            return False, message, "service_error"
        return False, message, "service_rejected"

    def normalize_response_actions(self, payload=None):
        if not isinstance(payload, dict):
            return []

        is_instant_payload = "instantActions" in payload
        source_actions = payload.get("actions") or payload.get("instantActions") or []
        if not isinstance(source_actions, list):
            return []

        action_config = self.response_config.get('actions', {}) or {}
        descriptor_fields = action_config.get('descriptor_source_fields', []) or []
        fallback_field = str(action_config.get('descriptor_fallback_field', '') or '').strip()
        descriptor_default = str(action_config.get('descriptor_default', '') or '')

        normalized = []
        for index, action in enumerate(source_actions, start=1):
            if not isinstance(action, dict):
                continue
            action_type = action.get("actionType") or action.get("action_type") or "UNKNOWN"
            if is_instant_payload:
                action_type = self.normalize_instant_action_type(action_type)
            route = self.instant_action_routes.get(str(action_type).strip().lower(), {}) if is_instant_payload else {}
            action_id = action.get("actionId") or action.get("action_id") or route.get("action_id") or f"ACT_{index:03d}"
            action_descriptor = self.resolve_action_descriptor(
                action,
                descriptor_fields,
                fallback_field,
                descriptor_default,
            )
            normalized.append({
                "actionId": action_id,
                "actionSeqNo": int(action.get("actionSeqNo") or index),
                "actionDescriptor": action_descriptor,
                "actionType": action_type
            })
        return normalized

    def resolve_action_descriptor(self, action, source_fields, fallback_field, default_value):
        for field in source_fields:
            value = action.get(field)
            if value is not None and str(value).strip():
                return str(value)
        if fallback_field:
            value = action.get(fallback_field)
            if value is not None and str(value).strip():
                return str(value)
        return default_value

    def response_type_for(self, key, default):
        response_types = self.response_config.get('response_types', {}) or {}
        value = str(response_types.get(key, default) or default).upper()
        return value if value in RESPONSE_TYPES else default

    def build_response_error(self, message_type, message, payload=None):
        references = []
        if isinstance(payload, dict):
            for key in ("orderId", "actionId"):
                value = payload.get(key)
                if value is not None:
                    references.append({"referenceKey": key, "referenceValue": str(value)})
            actions = payload.get("actions") or payload.get("instantActions") or []
            if isinstance(actions, list) and actions:
                action_id = actions[0].get("actionId") if isinstance(actions[0], dict) else None
                if action_id is not None:
                    references.append({"referenceKey": "actionId", "referenceValue": str(action_id)})

        return {
            "errorType": f"{str(message_type or 'message').upper()}_REJECTED",
            "errorReferences": references,
            "errorDescription": str(message or "request rejected"),
            "errorLevel": "WARNING"
        }

    def publish_inbound_response(self, message_type, success, message, payload=None, topic='', response_type=None, response_reason=''):
        if not self.inbound_auto_response:
            return

        if response_type is None:
            response_type = self.response_type_for(
                response_reason or ("service_accepted" if success else "service_rejected"),
                "ACCEPTED" if success else "REJECTED",
            )
        response_type = str(response_type).upper()
        if response_type not in RESPONSE_TYPES:
            response_type = "ERROR"
        is_instant_action_response = str(message_type or "").strip() == "instantActions"
        response_payload = {
            "orderId": payload.get("orderId") if isinstance(payload, dict) else None,
            "instantActionFlag": is_instant_action_response,
            "responseType": response_type,
            "actions": self.normalize_response_actions(payload),
            "reason": "" if success else str(message or "request rejected")
        }

        try:
            result = self.mqtt_core.publish_by_template('response', **response_payload)
            self.get_logger().info(
                f"Response 발행 완료 → {result.get('topic')}: "
                f"type={response_type}, source={topic}, message={message}"
            )
        except Exception as e:
            self.get_logger().error(f"Response 발행 실패: {e}")

    def srv_reconnect_cb(self, request, response):
        self.get_logger().info("UI로부터 수동 재연결 요청(Trigger) 수신: DISCONNECTED 발행 후 재접속합니다.")
        self.suppress_next_disconnect_reconnect = True
        self.comm_state = "DISCONNECTED"
        self.publish_ros_state()
        self.mqtt_core.disconnect(normal=True)

        delay_sec = float(self.config.get('mqtt', {}).get('manual_reconnect_delay', 2.0))
        if delay_sec > 0:
            time.sleep(delay_sec)

        if self.timer.is_canceled():
            self.timer = self.create_timer(1.0, self.publish_ros_state, clock=self.steady_clock)

        self.start_connection_thread("UI manual reconnect")

        response.success = True
        return response

    def srv_publish_mqtt_json_cb(self, request, response):
        """ROS bridge/UI에서 채운 JSON payload를 MQTT 사양 토픽으로 발행한다."""
        try:
            message_type = str(request.message_type or "").strip().strip("/")
            if message_type.lower() == "visualization" and not self.is_visualization_publish_enabled():
                self.clear_visualization_retained_if_disabled()
                response.success = True
                response.topic = ""
                response.message = "visualization publish disabled by comm_manager config"
                if hasattr(response, "payload_json"):
                    response.payload_json = ""
                if not self.visualization_skip_logged:
                    self.visualization_skip_logged = True
                    self.get_logger().info("Visualization MQTT 발행 비활성화: outbound.visualization.enabled=false")
                return response

            payload = json.loads(request.payload_json or "{}")
            if not isinstance(payload, dict):
                raise ValueError("payload_json must be a JSON object")

            qos = None if int(request.qos) < 0 else int(request.qos)
            retain = bool(request.retain) if request.override_retain else None

            result = self.mqtt_core.publish_json(
                message_type,
                payload=payload,
                qos=qos,
                retain=retain,
                use_template=bool(request.use_template)
            )
            response.success = True
            response.topic = result["topic"]
            response.message = "published"
            if hasattr(response, "payload_json"):
                response.payload_json = json.dumps(result.get("payload", {}), ensure_ascii=False)
        except Exception as e:
            response.success = False
            response.topic = ""
            response.message = str(e)
            if hasattr(response, "payload_json"):
                response.payload_json = ""
            self.get_logger().error(f"MQTT JSON 발행 서비스 실패: {e}")

        return response

    def srv_get_mqtt_json_templates_cb(self, request, response):
        """Expose comm_manager-owned MQTT JSON templates to UI/tools."""
        try:
            catalog = self.mqtt_core.get_template_catalog(request.message_type)
            response.success = True
            response.message = "ok"
            response.templates_json = json.dumps(catalog, ensure_ascii=False)
        except Exception as e:
            response.success = False
            response.message = str(e)
            response.templates_json = ""
            self.get_logger().error(f"MQTT 템플릿 목록 서비스 실패: {e}")
        return response

    def is_visualization_publish_enabled(self):
        visualization_config = self.outbound_config.get('visualization', True)
        if isinstance(visualization_config, dict):
            return bool(visualization_config.get('enabled', True))
        return bool(visualization_config)

    def clear_visualization_retained_if_disabled(self):
        visualization_config = self.outbound_config.get('visualization', True)
        clear_when_disabled = True
        if isinstance(visualization_config, dict):
            clear_when_disabled = bool(visualization_config.get('clear_retained_when_disabled', True))

        if self.is_visualization_publish_enabled() or not clear_when_disabled:
            return
        if self.visualization_retained_clear_sent:
            return

        try:
            self.mqtt_core.clear_retained('visualization', qos=0)
            self.visualization_retained_clear_sent = True
        except Exception as e:
            self.get_logger().warn(f"Visualization retained 삭제 실패: {e}")

    # ==========================================
    # MQTT ↔ Host (CIM) 브릿지 역할
    # ==========================================
    def on_mqtt_connect(self, rc):
        if rc == 0:
            self.comm_state = "CONNECTED"
            self.get_logger().info("▶ [3] MQTT 접속 성공.")

            if self.should_wait_initial_broker_time_sync():
                threading.Thread(
                    target=self.publish_connected_after_broker_time_sync,
                    daemon=True,
                    name="broker-time-initial-connection-publish",
                ).start()
                return

            self.publish_connected_payloads()
        else:
            self.get_logger().error(f"MQTT 접속 거부 (코드: {rc})")

    def publish_connected_payloads(self):
        if self.is_shutting_down or self.comm_state != "CONNECTED":
            return
        if not self.mqtt_core.connected:
            return
            
        # Host MQTT interface는 comm_node/backend가 소유한다.
        # UI는 표시/조작과 수동 publish 진단 경로만 유지한다.
        if self.publish_connection_on_mqtt_connect:
            self.mqtt_core.publish_by_template('connection', connectionState="CONNECTED")
        self.clear_retained_topics("clear_on_connect")
        self.clear_visualization_retained_if_disabled()

    def should_wait_initial_broker_time_sync(self):
        broker_cfg = self.config.get('time_sync', {}).get('broker_topic', {}) or {}
        if not bool(broker_cfg.get('enabled', False)):
            return False
        return bool(broker_cfg.get('wait_for_initial_sync_on_connect', False))

    def publish_connected_after_broker_time_sync(self):
        self.wait_for_initial_broker_time_sync()
        self.publish_connected_payloads()

    def wait_for_initial_broker_time_sync(self):
        broker_cfg = self.config.get('time_sync', {}).get('broker_topic', {}) or {}
        if not bool(broker_cfg.get('enabled', False)):
            return
        if not bool(broker_cfg.get('wait_for_initial_sync_on_connect', False)):
            return

        timeout_sec = float(broker_cfg.get('initial_sync_timeout_sec', 2.0) or 0.0)
        if timeout_sec <= 0:
            return

        if self.mqtt_core.wait_for_time_sync(timeout_sec):
            self.get_logger().info("Broker time sync ready before connection publish.")
        else:
            self.get_logger().warn(
                f"Broker time sync not received within {timeout_sec:.1f}s; "
                "connection payload will use local/system timestamp."
            )

    def on_mqtt_disconnect(self, rc):
        if self.is_shutting_down:
            self.get_logger().info("종료 중 MQTT 연결 단절 감지: 재연결 생략.")
            return

        if self.suppress_next_disconnect_reconnect:
            self.suppress_next_disconnect_reconnect = False
            self.get_logger().info("수동 재연결 절차 중 MQTT 단절 감지: 중복 재연결 생략.")
            return

        if self.comm_state != "CONNECTION FAILED":
            self.comm_state = "DISCONNECTED"
            self.visualization_retained_clear_sent = False
            self.get_logger().warn("▶ [4] MQTT 연결 단절 감지.")
            self.start_connection_thread("MQTT disconnect")

    def on_mqtt_message(self, topic, payload):
        """MQTT 메시지 수신 콜백. Order / InstantActions 를 검증하고 실제 ROS route로 위임."""
        self.get_logger().info(f"Host 명령 수신 [{topic}]: {payload}")

        message_type = self.inbound_message_type_from_topic(topic)
        if not message_type:
            return

        if isinstance(payload, dict) and payload.get("_comm_parse_error"):
            message = str(payload.get("_comm_parse_error") or "invalid JSON format")
            self.publish_inbound_response(
                message_type,
                False,
                message,
                {},
                topic,
                response_type=self.response_type_for("validation_error", "ERROR"),
                response_reason="validation_error",
            )
            return

        valid, validation_message, parsed_payload = self.payload_validator.process_result(topic, payload)
        if not valid:
            message = validation_message or f"invalid {message_type} format"
            self.get_logger().error(f"[InboundValidator] 처리 실패: {message}")
            self.publish_inbound_response(
                message_type,
                False,
                message,
                parsed_payload or payload,
                topic,
                response_type=self.response_type_for("validation_error", "ERROR"),
                response_reason="validation_error",
            )
            return

        self.get_logger().info("[InboundValidator] 처리 완료")
        trigger_success, trigger_message, trigger_reason = self.call_inbound_route(message_type, payload)
        self.get_logger().info(
            f"Inbound trigger result [{message_type}]: "
            f"success={trigger_success}, reason={trigger_reason}, message={trigger_message}"
        )
        self.publish_inbound_response(
            message_type,
            trigger_success,
            trigger_message,
            payload,
            topic,
            response_reason=trigger_reason,
        )

def main(args=None):
    preinit_config = load_preinit_config()
    if not acquire_single_instance_lock(preinit_config):
        return

    rclpy.init(args=args, signal_handler_options=SignalHandlerOptions.NO)

    def raise_keyboard_interrupt(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGINT, raise_keyboard_interrupt)
    signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

    node = CommNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt as exc:
        node.get_logger().info(f"종료 신호 수신({exc}). 우아한 종료 시작...")
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()
