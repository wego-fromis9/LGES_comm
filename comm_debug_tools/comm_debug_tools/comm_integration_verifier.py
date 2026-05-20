import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass

import paho.mqtt.client as mqtt
import rclpy
import yaml
from ament_index_python.packages import get_package_share_directory
from lges_recipe_interfaces.srv import RunRecipe, SendInstantAction
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node


def load_yaml(path):
    try:
        with open(os.path.expanduser(path), 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def resolve_config_path():
    env_path = os.environ.get('COMM_INTEGRATION_VERIFIER_CONFIG_PATH')
    if env_path and os.path.exists(os.path.expanduser(env_path)):
        return os.path.expanduser(env_path)
    try:
        share_dir = get_package_share_directory('comm_debug_tools')
        path = os.path.join(share_dir, 'config', 'comm_integration_verifier.yaml')
        if os.path.exists(path):
            return path
    except Exception:
        pass
    return '/home/wego/LGES_ws/src/comm_debug_tools/config/comm_integration_verifier.yaml'


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ''


class StubRecipeServices(Node):
    def __init__(self):
        super().__init__('comm_integration_stub_recipe_services')
        self.order_calls = 0
        self.instant_calls = []
        self.create_service(RunRecipe, '/recipe/run', self.on_run_recipe)
        self.create_service(SendInstantAction, '/recipe/instant_action', self.on_instant_action)

    def on_run_recipe(self, request, response):
        self.order_calls += 1
        response.accepted = True
        response.response_type = 'accepted'
        response.result_code = 'accepted'
        response.message = f'accepted by verifier stub: {request.order_id}'
        response.response_json = json.dumps({
            'execution_id': request.execution_id or f'verifier_{self.order_calls}',
            'order_id': request.order_id,
            'recipe_id': request.recipe_id,
        })
        return response

    def on_instant_action(self, request, response):
        self.instant_calls.append({
            'action_id': request.action_id,
            'action_type': request.action_type,
            'blocking_type': request.blocking_type,
            'input_json': request.input_json,
        })
        response.accepted = True
        response.response_type = 'accepted'
        response.result_code = 'accepted'
        response.message = f'accepted by verifier stub: {request.action_type}'
        response.response_json = json.dumps(self.instant_calls[-1], ensure_ascii=False)
        return response


class MqttCapture:
    def __init__(self, broker_ip, port, base_topic):
        self.base_topic = base_topic.rstrip('/')
        self.messages = []
        self.condition = threading.Condition()
        self.client = mqtt.Client(client_id=f'verifier_{int(time.time() * 1000)}', protocol=mqtt.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(broker_ip, int(port), 20)
        self.client.loop_start()

    def on_connect(self, client, _userdata, _flags, rc, _properties=None):
        if rc == 0:
            client.subscribe(f'{self.base_topic}/#', qos=1)

    def on_message(self, _client, _userdata, msg):
        raw = msg.payload.decode('utf-8', errors='replace')
        try:
            payload = json.loads(raw) if raw else None
        except Exception:
            payload = raw
        entry = {
            'topic': msg.topic,
            'suffix': str(msg.topic).replace(f'{self.base_topic}/', '', 1),
            'payload': payload,
            'raw': raw,
            'retain': bool(getattr(msg, 'retain', False)),
        }
        with self.condition:
            self.messages.append(entry)
            self.condition.notify_all()

    def wait_for(self, predicate, timeout_sec, description):
        deadline = time.monotonic() + timeout_sec
        with self.condition:
            while time.monotonic() < deadline:
                for msg in self.messages:
                    if predicate(msg):
                        return msg
                self.condition.wait(timeout=max(0.05, deadline - time.monotonic()))
        raise TimeoutError(f'timeout waiting for {description}')

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()


class Verifier:
    def __init__(self, config):
        self.config = config
        self.comm_config = load_yaml(config.get('comm_config_path') or '/home/wego/LGES_ws/src/comm_manager/config/config.yaml')
        mqtt_config = self.comm_config.get('mqtt', {}) or {}
        robot_config = self.comm_config.get('robot', {}) or {}
        self.broker_ip = mqtt_config.get('broker_ip', '127.0.0.1')
        self.port = int(mqtt_config.get('port', 1883))
        self.manufacturer = robot_config.get('manufacturer', 'WEGO')
        self.serial_number = robot_config.get('serial_number', 'AMR-001')
        self.base_topic = f'uagv/v2/{self.manufacturer}/{self.serial_number}'
        self.timeout = float(config.get('scenario_timeout_sec', 12))
        self.results = []
        self.capture = None
        self.stub_node = None
        self.executor = None
        self.executor_thread = None
        self.comm_proc = None

    def add_result(self, name, ok, detail=''):
        self.results.append(CheckResult(name, ok, detail))
        mark = 'PASS' if ok else 'FAIL'
        print(f'[{mark}] {name}: {detail}')

    def start_stub_services(self):
        if not bool(self.config.get('use_stub_recipe_services', True)):
            return
        self.stub_node = StubRecipeServices()
        self.executor = MultiThreadedExecutor()
        self.executor.add_node(self.stub_node)
        self.executor_thread = threading.Thread(target=self.executor.spin, daemon=True)
        self.executor_thread.start()

    def start_comm_node(self):
        if not bool(self.config.get('start_comm_node', True)):
            return
        self.comm_proc = subprocess.Popen(
            ['ros2', 'run', 'comm_manager', 'comm_node'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=os.setsid,
        )

    def stop_comm_node(self, abnormal=False):
        if not self.comm_proc or self.comm_proc.poll() is not None:
            return
        if abnormal:
            os.killpg(os.getpgid(self.comm_proc.pid), signal.SIGKILL)
        else:
            os.killpg(os.getpgid(self.comm_proc.pid), signal.SIGINT)
        try:
            self.comm_proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.comm_proc.pid), signal.SIGKILL)
            self.comm_proc.wait(timeout=3)

    def publish(self, suffix, payload, retain=False, qos=1):
        client = mqtt.Client(client_id=f'verifier_pub_{time.time_ns()}', protocol=mqtt.MQTTv5)
        client.connect(self.broker_ip, self.port, 20)
        client.loop_start()
        info = client.publish(
            f'{self.base_topic}/{suffix}',
            json.dumps(payload, ensure_ascii=False) if payload is not None else b'',
            qos=qos,
            retain=retain,
        )
        info.wait_for_publish(timeout=5)
        client.loop_stop()
        client.disconnect()

    def clear_retained_probe(self):
        for suffix in ('connection', 'state', 'factsheet', 'response'):
            self.publish(suffix, None, retain=True, qos=1)
        if not bool(self.config.get('retained_visualization_probe', True)):
            return
        self.publish('visualization', {'probe': 'retained_visualization'}, retain=True, qos=1)

    def wait_response(self, header_id, expected_type):
        msg = self.capture.wait_for(
            lambda item: item['suffix'] == 'response'
            and isinstance(item['payload'], dict)
            and item['payload'].get('responseOrderHeaderId') == header_id,
            self.timeout,
            f'response headerId={header_id}',
        )
        actual = msg['payload'].get('responseType')
        if actual != expected_type:
            raise AssertionError(f'expected responseType={expected_type}, actual={actual}, payload={msg["payload"]}')
        return msg

    def run(self):
        self.clear_retained_probe()
        self.capture = MqttCapture(self.broker_ip, self.port, self.base_topic)
        self.start_stub_services()
        self.start_comm_node()

        try:
            self.capture.wait_for(
                lambda item: item['suffix'] == 'connection'
                and isinstance(item['payload'], dict)
                and item['payload'].get('connectionState') == 'CONNECTED',
                self.timeout,
                'CONNECTED connection payload',
            )
            self.add_result('정상 연결', True, 'connection=CONNECTED 수신')

            self.capture.wait_for(
                lambda item: item['suffix'] == 'state' and isinstance(item['payload'], dict),
                self.timeout,
                'state payload',
            )
            self.add_result('State 주기 발행', True, 'state payload 수신')

            self.capture.wait_for(
                lambda item: item['suffix'] == 'visualization' and item['raw'] == '',
                self.timeout,
                'visualization retained clear',
            )
            self.add_result('Retain 삭제 정책', True, 'visualization retained clear 수신')

            order = {
                'headerId': 12001,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                'version': '2.0.0',
                'manufacturer': self.manufacturer,
                'serialNumber': self.serial_number,
                'orderId': 'VERIFIER_ORDER_001',
                'orderUpdateId': 1,
                'orderType': 'AUTO',
                'recipeId': 'test_navigation_recipe',
                'actions': [],
            }
            self.publish('order', order)
            self.wait_response(12001, 'ACCEPTED')
            order_calls_after_first = self.stub_node.order_calls if self.stub_node else -1
            self.add_result('Order 실행', True, f'/recipe/run 호출 수={order_calls_after_first}')

            duplicate = dict(order)
            duplicate['headerId'] = 12002
            self.publish('order', duplicate)
            self.wait_response(12002, 'ACCEPTED')
            order_calls_after_duplicate = self.stub_node.order_calls if self.stub_node else -1
            duplicate_ok = order_calls_after_duplicate == order_calls_after_first
            self.add_result('Order 중복 처리', duplicate_ok, f'중복 후 /recipe/run 호출 수={order_calls_after_duplicate}')

            stale = dict(order)
            stale['headerId'] = 12003
            stale['recipeId'] = 'different_recipe_same_update_id'
            self.publish('order', stale)
            self.wait_response(12003, 'REJECTED')
            self.add_result('orderUpdateId 단조 증가 검증', True, '같은 updateId 다른 payload REJECTED')

            for header_id, action_type, label in [
                (12101, 'start_pause', 'Pause'),
                (12102, 'stop_pause', 'Resume'),
                (12103, 'cancel_order', 'Cancel'),
                (12104, 'clear_instant_actions', 'Error 복구'),
            ]:
                payload = {
                    'headerId': header_id,
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'version': '2.0.0',
                    'manufacturer': self.manufacturer,
                    'serialNumber': self.serial_number,
                    'instantActions': [{
                        'actionId': f'IA_VERIFIER_{action_type.upper()}',
                        'actionType': action_type,
                    }],
                }
                self.publish('instantActions', payload)
                self.wait_response(header_id, 'ACCEPTED')
                self.add_result(label, True, f'{action_type} ACCEPTED')

            factsheet_request = {
                'headerId': 12105,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                'version': '2.0.0',
                'manufacturer': self.manufacturer,
                'serialNumber': self.serial_number,
                'instantActions': [{
                    'actionId': 'IA_VERIFIER_REQUEST_FACTSHEET',
                    'actionType': 'request_factsheet',
                }],
            }
            self.publish('instantActions', factsheet_request)
            self.wait_response(12105, 'ACCEPTED')
            self.capture.wait_for(
                lambda item: item['suffix'] == 'factsheet' and isinstance(item['payload'], dict),
                self.timeout,
                'factsheet payload',
            )
            self.add_result('Factsheet 요청 처리', True, 'request_factsheet -> factsheet 발행')

            self.stop_comm_node(abnormal=False)
            self.capture.wait_for(
                lambda item: item['suffix'] == 'connection'
                and isinstance(item['payload'], dict)
                and item['payload'].get('connectionState') == 'DISCONNECTED',
                self.timeout,
                'normal DISCONNECTED payload',
            )
            self.add_result('정상 종료', True, 'connection=DISCONNECTED 수신')

            self.start_comm_node()
            self.capture.wait_for(
                lambda item: item['suffix'] == 'connection'
                and isinstance(item['payload'], dict)
                and item['payload'].get('connectionState') == 'CONNECTED',
                self.timeout,
                'second CONNECTED payload',
            )
            self.stop_comm_node(abnormal=True)
            self.capture.wait_for(
                lambda item: item['suffix'] == 'connection'
                and isinstance(item['payload'], dict)
                and item['payload'].get('connectionState') == 'DISCONNECTED',
                float(self.config.get('lwt_wait_sec', 8)),
                'abnormal LWT DISCONNECTED payload',
            )
            self.add_result('비정상 LWT', True, 'SIGKILL 후 LWT DISCONNECTED 수신')
        except Exception as exc:
            self.add_result('검증 중단', False, str(exc))
        finally:
            self.stop_comm_node(abnormal=True)
            if self.capture:
                self.capture.stop()
            if self.executor and self.stub_node:
                self.executor.remove_node(self.stub_node)
                self.stub_node.destroy_node()
                self.executor.shutdown()

        passed = sum(1 for result in self.results if result.ok)
        failed = sum(1 for result in self.results if not result.ok)
        print(f'\nSUMMARY passed={passed}, failed={failed}')
        return failed == 0


def main(args=None):
    rclpy.init(args=args)
    config = load_yaml(resolve_config_path()).get('comm_integration_verifier', {}) or {}
    verifier = Verifier(config)
    try:
        ok = verifier.run()
    finally:
        if rclpy.ok():
            rclpy.shutdown()
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
