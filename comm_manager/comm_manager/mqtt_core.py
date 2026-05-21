import json
import os
import ssl
import subprocess
import time
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from ament_index_python.packages import get_package_share_directory
from .broker_time_sync import parse_timestamp_value, select_timestamp_value

class MqttCore:
    def __init__(self, config, logger):
        self.logger = logger
        self.mqtt_config = config.get('mqtt', {})
        self.robot_config = config.get('robot', {})
        
        self.manufacturer = self.robot_config.get('manufacturer', 'MANU')
        self.serial_number = self.robot_config.get('serial_number', 'AMR-001')
        self.base_topic = f"uagv/v2/{self.manufacturer}/{self.serial_number}"
        
        self.broker_ip = self.mqtt_config.get('broker_ip', '127.0.0.1')
        self.port = self.mqtt_config.get('port', 1883)
        self.keep_alive = self.mqtt_config.get('keep_alive', 20)
        self.client_id = self.mqtt_config.get('client_id') or self.serial_number
        self.require_publish_ack = bool(self.mqtt_config.get('require_publish_ack', True))
        self.publish_ack_timeout_sec = float(self.mqtt_config.get('publish_ack_timeout_sec', 5.0))
        self.inbound_qos = int(self.mqtt_config.get('inbound_qos', 1))
        self.session_expiry_interval_sec = int(self.mqtt_config.get('session_expiry_interval_sec', 0))
        self.clean_start = bool(self.mqtt_config.get('clean_start', True))
        self.version = self.robot_config.get('version', '2.0.0')
        self.timestamp_config = self.mqtt_config.get('timestamp', {}) or {}
        if not isinstance(self.timestamp_config, dict):
            self.timestamp_config = {'timezone': self.timestamp_config}
        self.timestamp_timezone = self._resolve_timestamp_timezone()
        self.timestamp_use_z_suffix_for_utc = bool(
            self.timestamp_config.get('use_z_suffix_for_utc', True)
        )
        self.time_sync_config = config.get('time_sync', {}) or {}
        self.time_sync_apply_config = self.time_sync_config.get('apply', {}) or {}
        self.broker_time_config = self.time_sync_config.get('broker_topic', {}) or {}
        self.time_sync_source = str(self.time_sync_config.get('source', 'system') or 'system').strip().lower()
        self.broker_time_sync_enabled = bool(
            self.broker_time_config.get(
                'enabled',
                self.time_sync_source in ('broker', 'broker_topic', 'mqtt')
            )
        )
        self.broker_time_topic = self._resolve_broker_time_topic()
        self.broker_time_timestamp_field = str(
            self.broker_time_config.get('timestamp_field', 'timestamp') or 'timestamp'
        )
        self.broker_time_qos = int(self.broker_time_config.get('qos', self.inbound_qos) or 0)
        self.broker_time_accept_retained = bool(self.broker_time_config.get('accept_retained', False))
        self.broker_time_max_offset_sec = float(self.broker_time_config.get('max_offset_sec', 86400.0))
        self.broker_time_stale_after_sec = float(self.broker_time_config.get('stale_after_sec', 300.0))
        self.broker_time_log_every_update = bool(self.broker_time_config.get('log_every_update', False))
        self.apply_mqtt_payload_timestamp = bool(
            self.time_sync_apply_config.get('mqtt_payload_timestamp', True)
        )
        self.system_clock_config = self.time_sync_config.get('system_clock', {}) or {}
        self.system_clock_enabled = bool(
            self.time_sync_apply_config.get(
                'system_clock',
                self.system_clock_config.get('enabled', False)
            )
        )
        self.system_clock_command = str(
            self.system_clock_config.get('command')
            or "sudo -n date -u --set '{timestamp}'"
        )
        self.system_clock_min_offset_sec = float(self.system_clock_config.get('min_offset_sec', 1.0))
        self.system_clock_max_offset_sec = float(
            self.system_clock_config.get('max_offset_sec', self.broker_time_max_offset_sec)
        )
        self.system_clock_cooldown_sec = float(self.system_clock_config.get('cooldown_sec', 30.0))
        self.system_clock_apply_once = bool(self.system_clock_config.get('apply_once', True))
        self.system_clock_timeout_sec = float(self.system_clock_config.get('timeout_sec', 5.0))
        self.system_clock_dry_run = bool(self.system_clock_config.get('dry_run', False))
        self.system_clock_last_apply_at = 0.0
        self.system_clock_applied = False
        self.system_clock_ready_for_outbound = not self.system_clock_enabled
        self.broker_time_offset_sec = 0.0
        self.broker_time_updated_at = 0.0
        self.broker_time_remote_timestamp = ""
        self.broker_time_last_state = ""
        self.broker_time_lock = threading.Lock()
        self.broker_time_event = threading.Event()
        
        self.header_id_counter = 1
        self.connected = False
        self.loop_started = False
        self.connection_lock = threading.Lock()
        
        # 템플릿 폴더 경로 설정 (ROS 2 share 디렉토리에서 동적으로 가져옴)
        try:
            package_share_dir = get_package_share_directory('comm_manager')
            self.template_dir = os.path.join(package_share_dir, 'json_templates')
        except Exception as e:
            self.logger.error(f"템플릿 경로를 찾을 수 없습니다: {e}")
            self.template_dir = ""
        
        self.client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv5)
        self._setup_auth_tls()
        self._setup_reconnect_backoff()
        
        # ... (이하 기존 코드와 완벽히 동일)
        self.on_connect_callback = None
        self.on_disconnect_callback = None
        self.on_message_callback = None
        self.on_time_sync_callback = None

        self.client.on_connect = self._internal_on_connect
        self.client.on_disconnect = self._internal_on_disconnect
        self.client.on_message = self._internal_on_message

        self._setup_lwt()

    def _setup_auth_tls(self):
        auth = self.mqtt_config.get('auth', {}) or {}
        username = auth.get('username')
        password = auth.get('password')
        password_env = auth.get('password_env')
        if password_env:
            password = os.environ.get(str(password_env), password)
        if username:
            self.client.username_pw_set(str(username), None if password is None else str(password))

        tls = self.mqtt_config.get('tls', {}) or {}
        if not bool(tls.get('enabled', False)):
            return

        ca_certs = tls.get('ca_certs') or tls.get('ca_cert')
        certfile = tls.get('certfile') or tls.get('cert_file')
        keyfile = tls.get('keyfile') or tls.get('key_file')
        cert_reqs = ssl.CERT_NONE if bool(tls.get('insecure', False)) else ssl.CERT_REQUIRED
        self.client.tls_set(
            ca_certs=os.path.expanduser(ca_certs) if ca_certs else None,
            certfile=os.path.expanduser(certfile) if certfile else None,
            keyfile=os.path.expanduser(keyfile) if keyfile else None,
            cert_reqs=cert_reqs,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        if bool(tls.get('insecure', False)):
            self.client.tls_insecure_set(True)

    def _setup_reconnect_backoff(self):
        backoff = self.mqtt_config.get('reconnect_backoff', {}) or {}
        min_delay = float(backoff.get('min_delay_sec', 1.0))
        max_delay = float(backoff.get('max_delay_sec', 30.0))
        try:
            self.client.reconnect_delay_set(min_delay=min_delay, max_delay=max_delay)
        except Exception as e:
            self.logger.warn(f"MQTT reconnect delay 설정 실패: {e}")

    def _connect_properties(self):
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = self.session_expiry_interval_sec
        return props

    def _will_properties(self):
        lwt_config = self.mqtt_config.get('lwt', {}) or {}
        delay = int(lwt_config.get('delay_interval_sec', 0) or 0)
        if delay <= 0:
            return None
        props = Properties(PacketTypes.WILLMESSAGE)
        props.WillDelayInterval = delay
        return props

    def _setup_lwt(self):
        """LWT(유언장)도 템플릿을 읽어서 동적으로 생성합니다."""
        lwt_config = self.mqtt_config.get('lwt', {}) or {}
        if not bool(lwt_config.get('enabled', True)):
            return
        lwt_payload, meta = self._build_from_template('connection', connectionState="DISCONNECTED")
        if lwt_payload and meta:
            lwt_topic = f"{self.base_topic}/{meta['topic_suffix']}"
            self.client.will_set(
                topic=lwt_topic,
                payload=json.dumps(lwt_payload),
                qos=int(lwt_config.get('qos', meta['qos'])),
                retain=bool(lwt_config.get('retain', meta['retain'])),
                properties=self._will_properties()
            )

    def _resolve_broker_time_topic(self):
        if not self.broker_time_sync_enabled:
            return ""

        explicit_topic = str(
            self.broker_time_config.get('topic')
            or self.broker_time_config.get('absolute_topic')
            or ''
        ).strip().strip('/')
        if explicit_topic:
            return explicit_topic

        suffix = str(self.broker_time_config.get('topic_suffix', 'timeSync') or 'timeSync')
        suffix = suffix.strip().strip('/')
        if not suffix:
            return ""
        return f"{self.base_topic}/{suffix}"

    def is_broker_time_topic(self, topic):
        if not self.broker_time_sync_enabled or not self.broker_time_topic:
            return False
        topic_text = str(topic or '').strip().strip('/')
        configured = str(self.broker_time_topic or '').strip().strip('/')
        if topic_text == configured:
            return True
        try:
            return mqtt.topic_matches_sub(configured, topic_text)
        except Exception:
            return False

    def subscribe_broker_time_topic(self, client):
        if not self.broker_time_sync_enabled or not self.broker_time_topic:
            return
        client.subscribe(self.broker_time_topic, qos=self.broker_time_qos)
        selector = {
            "mode": self.broker_time_config.get("payload_mode", "id_selector"),
            "list_path": self.broker_time_config.get("list_path", "timestamps"),
            "id_field": self.broker_time_config.get("id_field", "id"),
            "selected_id": self.broker_time_config.get("selected_id", "control"),
            "timestamp_field": self.broker_time_config.get("timestamp_field", "timestamp"),
        }
        self.logger.info(
            f"MQTT broker time sync 구독 완료: "
            f"[{self.broker_time_topic}], selector={selector}"
        )

    def reset_broker_time_sync_state(self):
        if not self.broker_time_sync_enabled:
            return
        with self.broker_time_lock:
            self.broker_time_offset_sec = 0.0
            self.broker_time_updated_at = 0.0
            self.broker_time_remote_timestamp = ""
            self.broker_time_event.clear()
        self.system_clock_ready_for_outbound = not self.system_clock_enabled

    def connect(self):
        with self.connection_lock:
            try:
                if self.connected and self.client.is_connected():
                    return True

                if self.loop_started:
                    self.client.reconnect()
                else:
                    try:
                        self.client.connect(
                            self.broker_ip,
                            self.port,
                            self.keep_alive,
                            clean_start=self.clean_start,
                            properties=self._connect_properties()
                        )
                    except TypeError:
                        self.client.connect(self.broker_ip, self.port, self.keep_alive)
                    self.client.loop_start()
                    self.loop_started = True
                return True
            except Exception as e:
                self.logger.error(f"MQTT 연결 실패: {e}")
                return False

    def disconnect(self, normal=True):
        with self.connection_lock:
            try:
                if normal and (self.connected or self.client.is_connected()):
                    self.publish_by_template('connection', connectionState="DISCONNECTED")
                    time.sleep(float(self.mqtt_config.get('disconnect_publish_wait_sec', 0.7)))
            except Exception as e:
                self.logger.warn(f"MQTT OFFLINE 발행 실패: {e}")

            try:
                if self.connected or self.client.is_connected():
                    self.client.disconnect()
            except Exception as e:
                self.logger.warn(f"MQTT disconnect 실패: {e}")

            try:
                if self.loop_started:
                    self.client.loop_stop()
            except Exception as e:
                self.logger.warn(f"MQTT loop_stop 실패: {e}")

            self.connected = False
            self.loop_started = False

    def _internal_on_connect(self, client, userdata, flags, rc, properties=None):
        self.connected = (rc == 0)

        # Host(관제 시스템) → 로봇 방향의 명령 토픽 구독
        # '+' 와일드카드: manufacturer, serial_number 값에 무관하게 수신
        if rc == 0:
            self.reset_broker_time_sync_state()
            client.subscribe(f"{self.base_topic}/order", qos=self.inbound_qos)
            client.subscribe(f"{self.base_topic}/instantActions", qos=self.inbound_qos)
            self.subscribe_broker_time_topic(client)
            self.logger.info(
                f"MQTT 명령 구독 완료: "
                f"[{self.base_topic}/order], "
                f"[{self.base_topic}/instantActions]"
            )

        if self.on_connect_callback:
            self.on_connect_callback(rc)

    def _internal_on_disconnect(self, client, userdata, rc, properties=None):
        self.connected = False
        if self.on_disconnect_callback: self.on_disconnect_callback(rc)
            
    def _internal_on_message(self, client, userdata, msg):
        try:
            raw_payload = msg.payload.decode('utf-8')
        except Exception:
            raw_payload = msg.payload.decode('utf-8', errors='replace')

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            if self.is_broker_time_topic(msg.topic):
                self.handle_broker_time_payload(
                    msg.topic,
                    {"_comm_parse_error": f"invalid JSON format: {raw_payload}"},
                    bool(getattr(msg, 'retain', False)),
                )
                return
            if self.on_message_callback:
                error_message = f"invalid JSON format: {raw_payload}"
                self.logger.warn(f"JSON 파싱 실패 (토픽: {msg.topic})")
                self._dispatch_message_callback(msg.topic, {
                    "_comm_parse_error": error_message
                })
            return

        if self.is_broker_time_topic(msg.topic):
            self.handle_broker_time_payload(msg.topic, payload, bool(getattr(msg, 'retain', False)))
            return

        if self.on_message_callback:
            self._dispatch_message_callback(msg.topic, payload)

    def _dispatch_message_callback(self, topic, payload):
        threading.Thread(
            target=self.on_message_callback,
            args=(topic, payload),
            daemon=True,
            name=f"mqtt-message-{str(topic).rstrip('/').split('/')[-1] or 'payload'}"
        ).start()

    def handle_broker_time_payload(self, topic, payload, retained=False):
        if retained and not self.broker_time_accept_retained:
            self.report_broker_time_state(
                "IGNORED_RETAINED",
                f"retained broker time message ignored: {topic}",
                topic=topic,
            )
            return False

        if not isinstance(payload, dict):
            self.report_broker_time_state(
                "ERROR",
                "broker time payload must be a JSON object",
                topic=topic,
            )
            return False

        if payload.get("_comm_parse_error"):
            self.report_broker_time_state(
                "ERROR",
                str(payload.get("_comm_parse_error")),
                topic=topic,
            )
            return False

        timestamp_value, selection = select_timestamp_value(payload, self.broker_time_config)
        if timestamp_value in (None, ""):
            self.report_broker_time_state(
                "ERROR",
                "broker time timestamp not found by configured selector",
                topic=topic,
                selection=selection,
            )
            return False

        local_received = datetime.now(timezone.utc)
        try:
            broker_time = parse_timestamp_value(timestamp_value)
        except Exception as exc:
            self.report_broker_time_state(
                "ERROR",
                f"broker time parse failed: {exc}",
                topic=topic,
            )
            return False

        offset_sec = (broker_time - local_received).total_seconds()
        if abs(offset_sec) > self.broker_time_max_offset_sec:
            self.report_broker_time_state(
                "REJECTED",
                (
                    f"broker time offset too large: offset={offset_sec:.3f}s, "
                    f"max={self.broker_time_max_offset_sec:.3f}s"
                ),
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        with self.broker_time_lock:
            self.broker_time_offset_sec = offset_sec
            self.broker_time_updated_at = time.monotonic()
            self.broker_time_remote_timestamp = broker_time.isoformat()
            self.broker_time_event.set()

        self.report_broker_time_state(
            "SYNCED",
            f"broker time synced: offset={offset_sec:.3f}s",
            topic=topic,
            broker_timestamp=broker_time.isoformat(),
            offset_sec=offset_sec,
            selection=selection,
        )
        if not self.system_clock_enabled:
            self.system_clock_ready_for_outbound = True
        self.apply_system_clock_if_configured(topic, broker_time, offset_sec, selection)
        return True

    def apply_system_clock_if_configured(self, topic, broker_time, offset_sec, selection=None):
        if not self.system_clock_enabled:
            self.system_clock_ready_for_outbound = True
            return False

        abs_offset = abs(float(offset_sec))
        if abs_offset < self.system_clock_min_offset_sec:
            self.system_clock_ready_for_outbound = True
            self.report_broker_time_state(
                "SYSTEM_CLOCK_SKIPPED",
                f"system clock apply skipped: offset {offset_sec:.3f}s < min {self.system_clock_min_offset_sec:.3f}s",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        if abs_offset > self.system_clock_max_offset_sec:
            self.system_clock_ready_for_outbound = False
            self.report_broker_time_state(
                "SYSTEM_CLOCK_REJECTED",
                f"system clock apply rejected: offset {offset_sec:.3f}s > max {self.system_clock_max_offset_sec:.3f}s",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        now_monotonic = time.monotonic()
        if self.system_clock_apply_once and self.system_clock_applied:
            self.system_clock_ready_for_outbound = True
            self.report_broker_time_state(
                "SYSTEM_CLOCK_SKIPPED",
                "system clock apply skipped: already applied once",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        if (
            self.system_clock_last_apply_at > 0
            and (now_monotonic - self.system_clock_last_apply_at) < self.system_clock_cooldown_sec
        ):
            self.system_clock_ready_for_outbound = True
            self.report_broker_time_state(
                "SYSTEM_CLOCK_SKIPPED",
                "system clock apply skipped: cooldown active",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        timestamp = broker_time.astimezone(timezone.utc).replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        command = self.system_clock_command.format(timestamp=timestamp)
        if self.system_clock_dry_run:
            self.system_clock_last_apply_at = now_monotonic
            self.system_clock_ready_for_outbound = True
            self.report_broker_time_state(
                "SYSTEM_CLOCK_DRY_RUN",
                f"system clock dry-run command: {command}",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return True

        try:
            result = subprocess.run(
                command,
                shell=True,
                text=True,
                capture_output=True,
                timeout=self.system_clock_timeout_sec,
                check=False,
            )
        except Exception as exc:
            self.system_clock_ready_for_outbound = False
            self.report_broker_time_state(
                "SYSTEM_CLOCK_ERROR",
                f"system clock command failed: {exc}",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        output = (result.stdout or result.stderr or "").strip()
        if result.returncode != 0:
            self.system_clock_ready_for_outbound = False
            self.report_broker_time_state(
                "SYSTEM_CLOCK_ERROR",
                f"system clock command returned {result.returncode}: {output}",
                topic=topic,
                broker_timestamp=broker_time.isoformat(),
                offset_sec=offset_sec,
                selection=selection,
            )
            return False

        self.system_clock_last_apply_at = now_monotonic
        self.system_clock_applied = True
        self.system_clock_ready_for_outbound = True
        with self.broker_time_lock:
            self.broker_time_offset_sec = 0.0
            self.broker_time_updated_at = time.monotonic()

        self.report_broker_time_state(
            "SYSTEM_CLOCK_SYNCED",
            f"system clock synced by broker timestamp: {timestamp}",
            topic=topic,
            broker_timestamp=broker_time.isoformat(),
            offset_sec=0.0,
            selection=selection,
        )
        return True

    def report_broker_time_state(self, state, message, **extra):
        state_key = str(state or "UNKNOWN").upper()
        if state_key != self.broker_time_last_state or self.broker_time_log_every_update:
            if state_key in {
                "SYNCED",
                "SYSTEM_CLOCK_SYNCED",
                "SYSTEM_CLOCK_SKIPPED",
                "SYSTEM_CLOCK_DRY_RUN",
            }:
                self.logger.info(message)
            elif state_key in {"ERROR", "REJECTED", "SYSTEM_CLOCK_ERROR", "SYSTEM_CLOCK_REJECTED"}:
                self.logger.error(message)
            else:
                self.logger.warn(message)
            self.broker_time_last_state = state_key

        if self.on_time_sync_callback:
            payload = {
                "source": "broker_topic",
                "state": state_key,
                "message": str(message or ""),
                "topic": extra.get("topic", self.broker_time_topic),
                "timestampField": self.broker_time_timestamp_field,
                "offsetSec": extra.get("offset_sec"),
                "brokerTimestamp": extra.get("broker_timestamp", ""),
                "selection": extra.get("selection"),
                "updatedAtMonotonic": self.broker_time_updated_at,
            }
            try:
                self.on_time_sync_callback(payload)
            except Exception as exc:
                self.logger.warn(f"broker time sync state callback failed: {exc}")

    def broker_time_is_synced(self):
        if not self.broker_time_sync_enabled:
            return False
        with self.broker_time_lock:
            updated_at = self.broker_time_updated_at
        if updated_at <= 0:
            return False
        if self.broker_time_stale_after_sec <= 0:
            return True
        return (time.monotonic() - updated_at) <= self.broker_time_stale_after_sec

    def broker_time_ready_for_outbound(self):
        if not self.broker_time_sync_enabled:
            return True
        if not self.broker_time_is_synced():
            return False
        if self.system_clock_enabled and not self.system_clock_ready_for_outbound:
            return False
        return True

    def wait_for_time_sync(self, timeout_sec):
        if not self.broker_time_sync_enabled:
            return True
        if self.broker_time_is_synced():
            return True
        return self.broker_time_event.wait(max(float(timeout_sec or 0), 0.0))

    def _resolve_timestamp_timezone(self):
        configured = str(self.timestamp_config.get('timezone', 'UTC') or 'UTC').strip()
        normalized = configured.upper()

        if normalized in ('UTC', 'Z'):
            return timezone.utc
        if normalized in ('KST', 'KOREA', 'SEOUL'):
            return ZoneInfo('Asia/Seoul')
        if normalized in ('LOCAL', 'SYSTEM'):
            return None

        try:
            return ZoneInfo(configured)
        except ZoneInfoNotFoundError:
            self.logger.warn(f"알 수 없는 timestamp timezone '{configured}', UTC를 사용합니다.")
            return timezone.utc

    def current_timestamp(self):
        if self.apply_mqtt_payload_timestamp and self.broker_time_is_synced():
            with self.broker_time_lock:
                offset_sec = self.broker_time_offset_sec
            now_utc = datetime.now(timezone.utc) + timedelta(seconds=offset_sec)
            if self.timestamp_timezone is None:
                now = now_utc.astimezone()
            else:
                now = now_utc.astimezone(self.timestamp_timezone)
        elif self.timestamp_timezone is None:
            now = datetime.now().astimezone()
        else:
            now = datetime.now(self.timestamp_timezone)
        now = now.replace(microsecond=0)

        if (
            self.timestamp_use_z_suffix_for_utc
            and now.utcoffset() == timedelta(0)
        ):
            return now.strftime('%Y-%m-%dT%H:%M:%SZ')
        return now.isoformat()

    # ==========================================
    # 템플릿 엔진 (마법이 일어나는 곳)
    # ==========================================
    def _template_path(self, template_name):
        return os.path.join(self.template_dir, f"{template_name}.json")

    def _load_template(self, template_name):
        filepath = self._template_path(template_name)
        with open(filepath, 'r') as f:
            return json.load(f)

    def get_template_catalog(self, message_type=""):
        requested = str(message_type or "").strip().strip("/")
        if requested:
            names = [requested]
        else:
            names = [
                os.path.splitext(name)[0]
                for name in sorted(os.listdir(self.template_dir))
                if name.endswith(".json")
            ] if self.template_dir and os.path.isdir(self.template_dir) else []

        templates = []
        for name in names:
            template = self._load_template(name)
            meta = template.get("_meta", {}) or {}
            templates.append({
                "messageType": name,
                "topicSuffix": meta.get("topic_suffix", name),
                "qos": int(meta.get("qos", 0)),
                "retain": bool(meta.get("retain", False)),
                "payloadTemplate": deepcopy(template.get("payload", {})),
                "headerFields": [
                    "headerId",
                    "timestamp",
                    "version",
                    "manufacturer",
                    "serialNumber"
                ]
            })

        return {
            "baseTopic": self.base_topic,
            "templates": templates
        }

    def _build_from_template(self, template_name, **kwargs):
        """JSON 템플릿을 읽고 동적 데이터(Header, 전달받은 kwargs)를 병합합니다."""
        try:
            template = self._load_template(template_name)
        except Exception as e:
            self.logger.error(f"템플릿 로드 실패 [{template_name}.json]: {e}")
            return None, None

        meta = template.get("_meta", {})
        payload = deepcopy(template.get("payload", {}))

        # 1. 공통 필수 Header 자동 채우기
        payload["headerId"] = kwargs.pop("headerId", self.header_id_counter)
        self.header_id_counter += 1
        payload["timestamp"] = kwargs.pop("timestamp", self.current_timestamp())
        payload["version"] = kwargs.pop("version", self.version)
        payload["manufacturer"] = kwargs.pop("manufacturer", self.manufacturer)
        payload["serialNumber"] = kwargs.pop("serialNumber", self.serial_number)

        # 2. 외부에서 주입된 데이터(kwargs)로 템플릿 덮어쓰기
        self._deep_merge(payload, kwargs)

        return payload, meta

    def _deep_merge(self, base, override):
        """딕셔너리는 재귀 병합하고, 리스트/스칼라는 덮어쓴다."""
        for key, value in override.items():
            if isinstance(base.get(key), dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    def _prune_nulls(self, value):
        """사양서 optional 필드는 값이 없을 때 전송하지 않도록 None을 제거한다."""
        if isinstance(value, dict):
            return {
                key: self._prune_nulls(item)
                for key, item in value.items()
                if item is not None
            }
        if isinstance(value, list):
            return [self._prune_nulls(item) for item in value]
        return value

    def _build_raw_payload(self, payload):
        payload = deepcopy(payload or {})
        payload.setdefault("headerId", self.header_id_counter)
        self.header_id_counter += 1
        payload.setdefault("timestamp", self.current_timestamp())
        payload.setdefault("version", self.version)
        payload.setdefault("manufacturer", self.manufacturer)
        payload.setdefault("serialNumber", self.serial_number)
        return payload

    def publish_by_template(self, template_name, **kwargs):
        """템플릿 이름과 데이터를 넘기면 알아서 조립 후 퍼블리쉬합니다."""
        return self.publish_json(template_name, kwargs, use_template=True)

    def clear_retained(self, topic_suffix, qos=0):
        """Clear a retained MQTT message by publishing an empty retained payload."""
        topic_suffix = str(topic_suffix or "").strip().strip("/")
        if not topic_suffix:
            raise ValueError("topic_suffix is required")

        topic = f"{self.base_topic}/{topic_suffix}"
        info = self.client.publish(topic, payload=b"", qos=int(qos), retain=True)
        self._wait_for_publish_ack(info, topic, int(qos))
        self.logger.info(f"MQTT retained 삭제 발행 완료 [{topic}] (QoS:{qos})")
        return {
            "topic": topic,
            "payload": None,
            "qos": int(qos),
            "retain": True,
            "mid": getattr(info, "mid", 0)
        }

    def publish_json(self, message_type, payload=None, qos=None, retain=None, use_template=True):
        """ROS service/UI에서 전달한 JSON을 MQTT 표준 토픽으로 발행한다."""
        message_type = (message_type or "").strip().strip("/")
        if not message_type:
            raise ValueError("message_type is required")

        if use_template:
            built_payload, meta = self._build_from_template(message_type, **(payload or {}))
            if not built_payload or not meta:
                raise ValueError(f"template not found or invalid: {message_type}")
            topic_suffix = meta.get("topic_suffix", message_type)
            qos_value = meta.get("qos", 0) if qos is None else qos
            retain_value = meta.get("retain", False) if retain is None else retain
        else:
            built_payload = self._build_raw_payload(payload or {})
            topic_suffix = message_type
            qos_value = 0 if qos is None else qos
            retain_value = False if retain is None else retain

        topic = f"{self.base_topic}/{topic_suffix}"
        built_payload = self._prune_nulls(built_payload)
        info = self.client.publish(
            topic,
            json.dumps(built_payload, ensure_ascii=False),
            qos=int(qos_value),
            retain=bool(retain_value)
        )
        self._wait_for_publish_ack(info, topic, int(qos_value))
        self.logger.info(f"MQTT 발행 완료 [{topic}] (QoS:{qos_value}, Retain:{retain_value})")
        return {
            "topic": topic,
            "payload": built_payload,
            "qos": int(qos_value),
            "retain": bool(retain_value),
            "mid": getattr(info, "mid", 0)
        }

    def _wait_for_publish_ack(self, info, topic, qos):
        if not self.require_publish_ack or qos <= 0:
            return
        thread_name = threading.current_thread().name.lower()
        if "paho-mqtt-client" in thread_name or thread_name.startswith("paho"):
            self.logger.debug(f"MQTT publish ack 대기 생략(콜백 스레드): {topic}")
            return
        try:
            info.wait_for_publish(timeout=self.publish_ack_timeout_sec)
        except RuntimeError as exc:
            raise TimeoutError(f"MQTT publish ack timeout: {topic}") from exc

        if hasattr(info, 'is_published') and not info.is_published():
            raise TimeoutError(f"MQTT publish ack timeout: {topic}")

        rc = getattr(info, 'rc', mqtt.MQTT_ERR_SUCCESS)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed: topic={topic}, rc={rc}")
