from setuptools import setup
import os
from glob import glob

package_name = 'comm_manager'

setup(
    name=package_name,
    version='1.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        # JSON 템플릿 등록
        (os.path.join('share', package_name, 'json_templates'), glob('comm_manager/json_templates/*.json')),
        # ⭐️ 추가: config 폴더 등록!
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools', 'paho-mqtt'],
    zip_safe=True,
    maintainer='Robot Engineer',
    maintainer_email='engineer@example.com',
    description='LGES 범용 로봇 MQTT 통신 매니저 패키지',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            # ⭐️ comm_node.py의 main 함수를 'comm_node'라는 이름으로 실행 가능하게 등록
            'comm_node = comm_manager.comm_node:main',
            'comm_trigger_ack_server = comm_manager.trigger_ack_server:main',
            'apply_host_setup = comm_manager.host_setup:main'
        ],
    },
)
