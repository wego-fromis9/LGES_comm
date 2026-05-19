import os
import re
from datetime import datetime, timezone

import yaml


class MessageRecorder:
    """Persist MQTT payloads as YAML for pre-integration review."""

    def __init__(self, config, logger=None):
        cfg = (config or {}).get('message_recording', {}) or {}
        self.logger = logger
        self.enabled = bool(cfg.get('enabled', True))
        self.output_dir = os.path.expanduser(
            cfg.get('output_dir', '~/Documents/lges_mqtt_yaml')
        )
        self.max_files_per_direction = int(cfg.get('max_files_per_direction', 1000))

    def record(self, direction, message_type, topic, payload, qos=None, retain=None):
        if not self.enabled:
            return ''

        direction = self._safe_name(direction or 'unknown')
        message_type = self._safe_name(message_type or self._message_type_from_topic(topic))
        now = datetime.now(timezone.utc)
        date_dir = os.path.join(self.output_dir, now.strftime('%Y%m%d'), direction)
        os.makedirs(date_dir, exist_ok=True)

        header_id = ''
        if isinstance(payload, dict) and payload.get('headerId') is not None:
            header_id = f"_h{payload.get('headerId')}"

        filename = f"{now.strftime('%H%M%S_%f')[:-3]}{header_id}_{message_type}.yaml"
        path = os.path.join(date_dir, filename)
        document = {
            'saved_at': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'direction': direction,
            'message_type': message_type,
            'topic': topic,
            'qos': qos,
            'retain': retain,
            'payload': payload,
        }

        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(
                document,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )

        self._cleanup(date_dir)
        return path

    def _cleanup(self, directory):
        if self.max_files_per_direction <= 0:
            return
        try:
            files = [
                os.path.join(directory, name)
                for name in os.listdir(directory)
                if name.endswith(('.yaml', '.yml'))
            ]
            overflow = len(files) - self.max_files_per_direction
            if overflow <= 0:
                return
            files.sort(key=lambda item: os.path.getmtime(item))
            for path in files[:overflow]:
                os.remove(path)
        except Exception as exc:
            if self.logger:
                self.logger.warn(f"MQTT YAML recorder cleanup failed: {exc}")

    def _message_type_from_topic(self, topic):
        return str(topic or '').rstrip('/').split('/')[-1] or 'message'

    def _safe_name(self, value):
        safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(value or '').strip())
        return safe.strip('_') or 'message'
