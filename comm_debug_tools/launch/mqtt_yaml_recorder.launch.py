from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        Node(
            package='comm_debug_tools',
            executable='mqtt_yaml_recorder',
            name='mqtt_yaml_recorder',
            output='screen',
        )
    ])
