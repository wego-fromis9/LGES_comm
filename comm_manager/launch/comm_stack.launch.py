from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.conditions import IfCondition
from launch.launch_description_sources import AnyLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory

import os


def generate_launch_description():
    enable_ack_server = LaunchConfiguration("enable_ack_server")
    enable_rosbridge = LaunchConfiguration("enable_rosbridge")
    rosbridge_port = LaunchConfiguration("rosbridge_port")

    rosbridge_launch = os.path.join(
        get_package_share_directory("rosbridge_server"),
        "launch",
        "rosbridge_websocket_launch.xml",
    )

    return LaunchDescription([
        DeclareLaunchArgument("enable_ack_server", default_value="false"),
        DeclareLaunchArgument("enable_rosbridge", default_value="false"),
        DeclareLaunchArgument("rosbridge_port", default_value="9090"),

        Node(
            package="comm_manager",
            executable="comm_node",
            name="comm_node",
            output="screen",
        ),

        Node(
            package="comm_manager",
            executable="comm_trigger_ack_server",
            name="comm_trigger_ack_server",
            output="screen",
            condition=IfCondition(enable_ack_server),
        ),

        IncludeLaunchDescription(
            AnyLaunchDescriptionSource(rosbridge_launch),
            launch_arguments={"port": rosbridge_port}.items(),
            condition=IfCondition(enable_rosbridge),
        ),
    ])
