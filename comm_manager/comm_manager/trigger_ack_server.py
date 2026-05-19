#!/usr/bin/env python3
import json

import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from std_srvs.srv import Trigger


class TriggerAckServer(Node):
    """ACK server for UI button tests and lightweight UR/System state topics.

    Real UR joint states, UR rosout, and camera topics are provided by their own
    runtime nodes. This server does not publish fake hardware data.
    """

    def __init__(self):
        super().__init__('comm_trigger_ack_server')
        self.ur_locked = True
        self.ur_freedrive = False
        self.ur_initialized = False
        self.gripper_calibrated = False
        self.gripper_initialized = False
        self.ur_last_action = None
        self.ur_last_action_status = 'IDLE'
        self.system_auto = False
        self.system_playing = False
        self.system_operation_state = 'INIT'
        self.system_last_action = None
        self.system_last_action_status = 'IDLE'
        self.system_state_pub = self.create_publisher(String, '/system/state', 10)
        self.ur_state_pub = self.create_publisher(String, '/ur/state', 10)

        self.create_service(Trigger, '/comm/order_received', self.handle_order)
        self.create_service(Trigger, '/comm/instant_actions_received', self.handle_instant_actions)

        self.create_service(Trigger, '/ur/toggle_lock', self.handle_ur_toggle_lock)
        self.create_service(Trigger, '/ur/home', self.handle_ur_home)
        self.create_service(Trigger, '/ur/clear_stop', self.handle_ur_clear_stop)
        self.create_service(Trigger, '/ur/freedrive', self.handle_ur_freedrive)
        self.create_service(Trigger, '/gripper/calibrate', self.handle_gripper_calibrate)
        self.create_service(Trigger, '/gripper/home', self.handle_gripper_home)

        self.create_service(Trigger, '/system/toggle_mode', self.handle_system_toggle_mode)
        self.create_service(Trigger, '/system/toggle_play', self.handle_system_toggle_play)
        self.create_service(Trigger, '/system/abort', self.handle_system_abort)
        self.create_service(Trigger, '/system/reset', self.handle_system_reset)
        self.create_service(Trigger, '/system/home', self.handle_system_home)
        self.create_service(Trigger, '/system/return', self.handle_system_return)
        self.create_service(Trigger, '/system/config_changed', self.handle_config_changed)

        self.create_timer(0.5, self.publish_state_topics)

        self.publish_state_topics()
        self.get_logger().info(
            'Trigger ACK services and state topics ready: /system/state, /ur/state'
        )

    def accept(self, response, message):
        response.success = True
        response.message = message
        self.get_logger().info(message)
        self.publish_state_topics()
        return response

    def publish_json(self, publisher, payload):
        msg = String()
        msg.data = json.dumps(payload, ensure_ascii=False)
        publisher.publish(msg)

    def ur_ready(self):
        return (
            not self.ur_locked
            and self.ur_initialized
            and self.gripper_calibrated
            and self.gripper_initialized
        )

    def compute_ur_mode(self):
        if self.ur_freedrive:
            return 'MANUAL'
        if self.ur_ready():
            return 'AUTO'
        return 'WAIT'

    def publish_state_topics(self):
        control_state = 'AUTO' if self.system_auto else 'MANUAL'
        self.publish_json(self.system_state_pub, {
            'controlState': control_state,
            'operationState': self.system_operation_state,
            'playing': self.system_playing,
            'lastAction': self.system_last_action,
            'lastActionStatus': self.system_last_action_status,
            'source': 'comm_trigger_ack_server',
            'stamp': self.get_clock().now().to_msg().sec,
        })
        self.publish_json(self.ur_state_pub, {
            'lockState': 'LOCK' if self.ur_locked else 'UNLOCK',
            'mode': self.compute_ur_mode(),
            'freedrive': self.ur_freedrive,
            'ready': self.ur_ready(),
            'steps': {
                'unlocked': not self.ur_locked,
                'urInitialized': self.ur_initialized,
                'gripperCalibrated': self.gripper_calibrated,
                'gripperInitialized': self.gripper_initialized,
            },
            'lastAction': self.ur_last_action,
            'lastActionStatus': self.ur_last_action_status,
            'source': 'comm_trigger_ack_server',
            'stamp': self.get_clock().now().to_msg().sec,
        })

    def handle_order(self, _request, response):
        self.system_playing = True
        self.system_operation_state = 'ACTIVE'
        self.system_last_action = 'order_received'
        self.system_last_action_status = 'ACTIVE'
        return self.accept(response, 'order trigger accepted')

    def handle_instant_actions(self, _request, response):
        self.system_last_action = 'instant_actions_received'
        self.system_last_action_status = 'FINISHED'
        return self.accept(response, 'instantActions trigger accepted')

    def handle_ur_toggle_lock(self, _request, response):
        self.ur_locked = not self.ur_locked
        if self.ur_locked:
            self.ur_freedrive = False
        self.ur_last_action = 'ur_unlock' if not self.ur_locked else 'ur_lock'
        self.ur_last_action_status = 'FINISHED'
        return self.accept(response, 'UR unlock accepted' if not self.ur_locked else 'UR lock accepted')

    def handle_ur_home(self, _request, response):
        self.ur_initialized = True
        self.ur_last_action = 'ur_init'
        self.ur_last_action_status = 'FINISHED'
        return self.accept(response, 'UR init accepted')

    def handle_ur_clear_stop(self, _request, response):
        self.ur_last_action = 'ur_clear_stop'
        self.ur_last_action_status = 'FINISHED'
        return self.accept(response, f'UR protective stop clear accepted: mode={self.compute_ur_mode()}')

    def handle_ur_freedrive(self, _request, response):
        self.ur_freedrive = not self.ur_freedrive
        self.ur_last_action = 'ur_freedrive'
        self.ur_last_action_status = 'ACTIVE' if self.ur_freedrive else 'FINISHED'
        return self.accept(response, 'UR manual/free drive accepted' if self.ur_freedrive else 'UR auto/wait accepted')

    def handle_gripper_calibrate(self, _request, response):
        self.gripper_calibrated = True
        self.ur_last_action = 'gripper_calibrate'
        self.ur_last_action_status = 'FINISHED'
        return self.accept(response, 'Gripper calibration accepted')

    def handle_gripper_home(self, _request, response):
        self.gripper_initialized = True
        self.ur_last_action = 'gripper_init'
        self.ur_last_action_status = 'FINISHED'
        return self.accept(response, 'Gripper init accepted')

    def handle_system_toggle_mode(self, _request, response):
        self.system_auto = not self.system_auto
        self.system_last_action = 'sys_toggle_mode'
        self.system_last_action_status = 'FINISHED'
        return self.accept(response, 'System mode accepted')

    def handle_system_toggle_play(self, _request, response):
        self.system_playing = not self.system_playing
        self.system_operation_state = 'ACTIVE' if self.system_playing else 'PAUSED'
        self.system_last_action = 'sys_toggle_play'
        self.system_last_action_status = 'ACTIVE' if self.system_playing else 'FINISHED'
        return self.accept(response, 'System play toggle accepted')

    def handle_system_abort(self, _request, response):
        self.system_playing = False
        self.system_operation_state = 'INIT'
        self.system_last_action = 'sys_abort'
        self.system_last_action_status = 'FINISHED'
        return self.accept(response, 'System abort accepted')

    def handle_system_reset(self, _request, response):
        self.system_playing = False
        self.system_operation_state = 'INIT'
        self.system_last_action = 'sys_reset'
        self.system_last_action_status = 'FINISHED'
        return self.accept(response, 'System reset accepted')

    def handle_system_home(self, _request, response):
        self.system_playing = False
        self.system_operation_state = 'INIT'
        self.system_last_action = 'sys_home'
        self.system_last_action_status = 'FINISHED'
        return self.accept(response, 'System home accepted')

    def handle_system_return(self, _request, response):
        self.system_playing = False
        self.system_operation_state = 'DOCK'
        self.system_last_action = 'sys_dock'
        self.system_last_action_status = 'ACTIVE'
        return self.accept(response, 'System dock accepted')

    def handle_config_changed(self, _request, response):
        self.system_last_action = 'config_changed'
        self.system_last_action_status = 'FINISHED'
        return self.accept(response, 'System config changed accepted')


def main(args=None):
    rclpy.init(args=args)
    node = TriggerAckServer()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
