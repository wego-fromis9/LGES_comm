import json
import os
import ssl
import time
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from ament_index_python.packages import get_package_share_directory

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
        if self.on_connect_callback:
            self.on_connect_callback(rc)

        # Host(관제 시스템) → 로봇 방향의 명령 토픽 구독
        # '+' 와일드카드: manufacturer, serial_number 값에 무관하게 수신
        if rc == 0:
            client.subscribe(f"{self.base_topic}/order", qos=self.inbound_qos)
            client.subscribe(f"{self.base_topic}/instantActions", qos=self.inbound_qos)
            self.logger.info(
                f"MQTT 명령 구독 완료: "
                f"[{self.base_topic}/order], "
                f"[{self.base_topic}/instantActions]"
            )


    def _internal_on_disconnect(self, client, userdata, rc, properties=None):
        self.connected = False
        if self.on_disconnect_callback: self.on_disconnect_callback(rc)
            
    def _internal_on_message(self, client, userdata, msg):
        if self.on_message_callback:
            try:
                payload = json.loads(msg.payload.decode('utf-8'))
                self._dispatch_message_callback(msg.topic, payload)
            except json.JSONDecodeError:
                raw_payload = msg.payload.decode('utf-8', errors='replace')
                error_message = f"invalid JSON format: {raw_payload}"
                self.logger.warn(f"JSON 파싱 실패 (토픽: {msg.topic})")
                self._dispatch_message_callback(msg.topic, {
                    "_comm_parse_error": error_message
                })

    def _dispatch_message_callback(self, topic, payload):
        threading.Thread(
            target=self.on_message_callback,
            args=(topic, payload),
            daemon=True,
            name=f"mqtt-message-{str(topic).rstrip('/').split('/')[-1] or 'payload'}"
        ).start()

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
        if self.timestamp_timezone is None:
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
