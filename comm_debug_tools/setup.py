from glob import glob
import os

from setuptools import setup


package_name = 'comm_debug_tools'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools', 'paho-mqtt', 'PyYAML'],
    zip_safe=True,
    maintainer='wego',
    maintainer_email='chohyunwoo23@gmail.com',
    description='Debug-only LGES MQTT payload recorder tools',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'mqtt_yaml_recorder = comm_debug_tools.mqtt_yaml_recorder:main',
            'comm_integration_verifier = comm_debug_tools.comm_integration_verifier:main',
        ],
    },
)
