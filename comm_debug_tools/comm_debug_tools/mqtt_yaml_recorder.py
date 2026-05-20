import json
import os
import signal
import sys
import threading

import paho.mqtt.client as mqtt
import rclpy
import yaml
from ament_index_python.packages import get_package_share_directory
from rclpy.node import Node

from .message_recorder import MessageRecorder


def _load_yaml(path):
    try:
        with open(os.path.expanduser(path), 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _resolve_debug_config_path():
    env_path = os.environ.get('COMM_DEBUG_TOOLS_CONFIG_PATH')
    if env_path and os.path.exists(os.path.expanduser(env_path)):
        return os.path.expanduser(env_path)
    try:
        share_dir = get_package_share_directory('comm_debug_tools')
        share_path = os.path.join(share_dir, 'config', 'mqtt_yaml_recorder.yaml')
        if os.path.exists(share_path):
            return share_path
    except Exception:
        pass
    return '/home/wego/LGES_ws/src/comm_debug_tools/config/mqtt_yaml_recorder.yaml'


class MqttYamlRecorderNode(Node):
    def __init__(self):
        super().__init__('mqtt_yaml_recorder')
        debug_config = _load_yaml(_resolve_debug_config_path()).get('mqtt_yaml_recorder', {}) or {}
        comm_config = _load_yaml(debug_config.get('config_path') or '/home/wego/LGES_ws/src/comm_manager/config/config.yaml')

        mqtt_config = comm_config.get('mqtt', {}) or {}
        robot_config = comm_config.get('robot', {}) or {}
        self.enabled = bool(debug_config.get('enabled', True))
        self.broker_ip = debug_config.get('broker_ip') or mqtt_config.get('broker_ip', '127.0.0.1')
        self.port = int(debug_config.get('port') or mqtt_config.get('port', 1883))
        manufacturer = robot_config.get('manufacturer', 'WEGO')
        serial_number = robot_config.get('serial_number', 'AMR-001')
        self.topic = debug_config.get('topic') or f'uagv/v2/{manufacturer}/{serial_number}/#'
        self.recorder = MessageRecorder(
            output_dir=debug_config.get('output_dir', '~/Documents/lges_mqtt_yaml'),
            max_files_per_day=debug_config.get('max_files_per_day', 3000),
            logger=self.get_logger(),
        )
        self.connected_event = threading.Event()

        self.client = mqtt.Client(client_id=f'{serial_number}_debug_recorder', protocol=mqtt.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def start(self):
        if not self.enabled:
            self.get_logger().info('MQTT YAML recorder disabled by config')
            return
        self.get_logger().info(f'MQTT YAML recorder connecting: {self.broker_ip}:{self.port}, topic={self.topic}')
        self.client.connect(self.broker_ip, self.port, 20)
        self.client.loop_start()

    def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def on_connect(self, client, _userdata, _flags, rc, _properties=None):
        if rc == 0:
            self.connected_event.set()
            client.subscribe(self.topic)
            self.get_logger().info(f'MQTT YAML recorder subscribed: {self.topic}')
        else:
            self.get_logger().error(f'MQTT YAML recorder connection rejected: rc={rc}')

    def on_disconnect(self, _client, _userdata, rc, _properties=None):
        self.connected_event.clear()
        self.get_logger().warn(f'MQTT YAML recorder disconnected: rc={rc}')

    def on_message(self, _client, _userdata, msg):
        raw_payload = msg.payload.decode('utf-8', errors='replace')
        try:
            payload = json.loads(raw_payload)
        except Exception:
            payload = raw_payload

        message_type = str(msg.topic or '').rstrip('/').split('/')[-1] or 'message'
        path = self.recorder.record(
            'mqtt',
            message_type,
            msg.topic,
            payload,
            qos=getattr(msg, 'qos', None),
            retain=getattr(msg, 'retain', None),
        )
        self.get_logger().debug(f'MQTT YAML saved: {path}')


def main(args=None):
    rclpy.init(args=args)
    node = MqttYamlRecorderNode()

    def _shutdown(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        node.start()
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info('MQTT YAML recorder shutdown requested')
    finally:
        node.stop()
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main(sys.argv)
