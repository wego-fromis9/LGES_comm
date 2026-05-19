#!/usr/bin/env python3
import json
import fcntl
import os
import sys
import yaml
import time
import threading
import signal

import rclpy
from rclpy.node import Node
from rclpy.signals import SignalHandlerOptions
from std_srvs.srv import Trigger

# 커스텀 인터페이스 로드
from comm_interfaces.msg import ConnectionState
from comm_interfaces.srv import GetMqttJsonTemplates, PublishMqttJson, TriggerReconnect

# 엔진(Core) 모듈 로드
from .wifi_core import WifiCore
from .mqtt_core import MqttCore
from .task_handler import TaskHandler
from .ntp_monitor import NtpMonitor

from ament_index_python.packages import get_package_share_directory

_INSTANCE_LOCK_FILE = None
RESPONSE_TYPES = {"ACCEPTED", "REJECTED", "ERROR"}

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
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
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
        self.config_path = ""
        
        # [1] 설정 파일 로드
        self.config = self.load_config()
        
        # [2] 코어 모듈 초기화 (의존성 주입)
        self.wifi_core = WifiCore(self.config, self.get_logger())
        self.mqtt_core = MqttCore(self.config, self.get_logger())

        # [2-1] TaskHandler 초기화 (설정 없이 생성, 내부 기본값 사용)
        self.task_handler = TaskHandler(logger=self.get_logger())
        
        # [3] MQTT 콜백 연결
        self.mqtt_core.on_connect_callback = self.on_mqtt_connect
        self.mqtt_core.on_disconnect_callback = self.on_mqtt_disconnect
        self.mqtt_core.on_message_callback = self.on_mqtt_message
        
        # [4] ROS 2 인터페이스 세팅
        self.state_pub = self.create_publisher(ConnectionState, '/connection_state', 10)
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
        self.is_shutting_down = False
        self.connection_lock = threading.Lock()
        self.connection_thread = None
        self.suppress_next_disconnect_reconnect = False
        
        self.max_retries = self.config.get('mqtt', {}).get('max_retries', 5)
        self.retry_interval = self.config.get('mqtt', {}).get('retry_interval', 5)
        self.manage_wifi = self.config.get('network', {}).get('manage_wifi', True)
        self.publish_connection_on_mqtt_connect = self.config.get('mqtt', {}).get('publish_connection_on_mqtt_connect', True)
        self.inbound_config = self.config.get('inbound', {})
        self.inbound_auto_response = self.inbound_config.get('auto_response', True)
        self.inbound_service_timeout_sec = float(self.inbound_config.get('service_timeout_sec', 3.0))
        self.inbound_service_wait_timeout_sec = float(self.inbound_config.get('service_wait_timeout_sec', 0.5))
        self.inbound_triggers = self.inbound_config.get('triggers', {}) or {}
        self.response_config = self.inbound_config.get('response', {}) or {}
        self.inbound_trigger_clients = self.create_inbound_trigger_clients()
        self.outbound_config = self.config.get('outbound', {}) or {}
        self.visualization_skip_logged = False
        self.visualization_retained_clear_sent = False
        self.ntp_monitor = NtpMonitor(self.config, self.config_path, self.get_logger())
        self.ntp_monitor.start()
        
        # [5] UI용 상태 퍼블리시 타이머 (1초 주기)
        self.timer = self.create_timer(1.0, self.publish_ros_state)
        
        # [6] 메인 백그라운드 스레드 시작
        threading.Thread(target=self.main_control_loop, daemon=True).start()

    def load_config(self):
        try:
            path = resolve_config_path()
            self.config_path = path
            with open(path, 'r') as f:
                self.get_logger().info(f"Config 로드: {path}")
                return yaml.safe_load(f)
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
            daemon=True,
            name="comm-mqtt-connect"
        )
        self.connection_thread.start()
        return True

    def connect_full_process(self):
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
                    time.sleep(self.retry_interval)

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
            if hasattr(self, 'ntp_monitor') and self.ntp_monitor:
                self.ntp_monitor.stop()

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
            
            # 2. MQTT (Host)로 OFFLINE 전송
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
            action_id = action.get("actionId") or f"ACT_{index:03d}"
            action_type = action.get("actionType") or "UNKNOWN"
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
        response_payload = {
            "responseOrderHeaderId": payload.get("headerId") if isinstance(payload, dict) else None,
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
            self.timer = self.create_timer(1.0, self.publish_ros_state)

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
            
            # 네트워크 연결 상태는 comm_node가 소유한다.
            # factsheet/state/visualization payload는 UI가 서비스로 요청한다.
            if self.publish_connection_on_mqtt_connect:
                self.mqtt_core.publish_by_template('connection', connectionState="CONNECTED")
            self.clear_visualization_retained_if_disabled()
        else:
            self.get_logger().error(f"MQTT 접속 거부 (코드: {rc})")

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
        """MQTT 메시지 수신 콜백. Order / InstantActions 는 TaskHandler 로 위임."""
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

        save_success, save_message, parsed_payload = self.task_handler.process_and_save_result(topic, payload)
        if not save_success:
            message = save_message or f"invalid {message_type} format"
            self.get_logger().error(f"[TaskHandler] 처리 실패: {message}")
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

        self.get_logger().info("[TaskHandler] 처리 완료 → YAML 저장됨")
        trigger_success, trigger_message, trigger_reason = self.call_inbound_trigger(message_type)
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
