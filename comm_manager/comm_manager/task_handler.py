import os
import json
import yaml
from datetime import datetime
from ament_index_python.packages import get_package_share_directory

class TaskHandler:
    def __init__(self, output_dir="~/Documents/tasks/received_yaml", logger=None):
        self.logger = logger  # rclpy Node 의 logger 를 외부에서 주입
        self.last_error = ""

        # [수정 1] ROS 2의 표준 설치 경로(share)에서 템플릿을 찾도록 변경
        try:
            package_share_dir = get_package_share_directory('comm_manager')
            self.template_dir = os.path.join(package_share_dir, 'json_templates')
        except Exception as e:
            self._log(f"템플릿 디렉토리 확인 실패: {e}", 'error')
            self.template_dir = ""

        # YAML 출력 폴더 (존재하지 않으면 자동 생성)
        self.output_dir = os.path.expanduser(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        # 검증용 템플릿 사전 로드
        self.order_template = self._load_template("order.json")
        self.instant_template = self._load_template("instantActions.json")
        if not self.instant_template:
            self.instant_template = self._load_template("instant_actions.json")

        self._log(f"TaskHandler 초기화 완료. YAML 저장 경로: {self.output_dir}")

    # ------------------------------------------------------------------
    # 내부 유틸리티
    # ------------------------------------------------------------------

    def _log(self, msg: str, level: str = 'info'):
        """[수정 2] rclpy 로거 버그 방지를 위해 명시적 조건문으로 변경"""
        if self.logger:
            if level == 'info':
                self.logger.info(msg)
            elif level == 'warn':
                self.logger.warning(msg)  # rclpy에서는 warning 사용 권장
            elif level == 'error':
                self.logger.error(msg)
            else:
                self.logger.debug(msg)
        else:
            prefix = {'info': 'ℹ', 'warn': '⚠', 'error': '✖', 'debug': '🔍'}.get(level, '')
            print(f"[TaskHandler] {prefix} {msg}")

    def _fail(self, msg: str, level: str = 'warn') -> bool:
        self.last_error = str(msg or "validation failed")
        self._log(self.last_error, level)
        return False

    def _load_template(self, filename: str) -> dict:
        """JSON 검증 템플릿을 로드합니다."""
        filepath = os.path.join(self.template_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self._log(f"템플릿 로드 실패 [{filename}]: {e}", 'error')
            return {}

    def _payload_template(self, template: dict) -> dict:
        if isinstance(template, dict) and isinstance(template.get("payload"), dict):
            return template["payload"]
        return template if isinstance(template, dict) else {}

    def _validate_required_keys(self, data: dict, required_keys: list) -> bool:
        if not isinstance(data, dict):
            return self._fail("invalid payload format: payload must be a JSON object", 'error')

        missing = [key for key in required_keys if key not in data]
        if missing:
            return self._fail(f"invalid payload format: missing required keys {missing}", 'warn')
        return True

    def _validate_order(self, data: dict) -> bool:
        required = [
            "headerId",
            "timestamp",
            "version",
            "manufacturer",
            "serialNumber",
            "orderId",
            "orderUpdateId"
        ]
        if not self._validate_required_keys(data, required):
            return False

        actions = data.get("actions", [])
        recipe_id = str(data.get("recipeId") or "").strip()
        if not recipe_id and not actions:
            return self._fail("invalid order format: either recipeId or actions is required", 'warn')

        if actions is not None and not isinstance(actions, list):
            return self._fail("invalid order format: actions must be an array", 'warn')

        for index, action in enumerate(actions or []):
            if not isinstance(action, dict):
                return self._fail(f"invalid order format: actions[{index}] must be an object", 'warn')
            missing = [
                key for key in ("actionId", "actionSeqNo", "actionType")
                if key not in action
            ]
            if missing:
                return self._fail(f"invalid order format: actions[{index}] missing required keys {missing}", 'warn')
            if "actionParameters" in action and not isinstance(action.get("actionParameters"), list):
                return self._fail(f"invalid order format: actions[{index}].actionParameters must be an array", 'warn')
        return True

    def _validate_instant_actions(self, data: dict) -> bool:
        required = [
            "headerId",
            "timestamp",
            "version",
            "manufacturer",
            "serialNumber",
            "instantActions"
        ]
        if not self._validate_required_keys(data, required):
            return False

        actions = data.get("instantActions")
        if not isinstance(actions, list) or not actions:
            return self._fail("invalid instantActions format: instantActions must be a non-empty array", 'warn')

        for index, action in enumerate(actions):
            if not isinstance(action, dict):
                return self._fail(f"invalid instantActions format: instantActions[{index}] must be an object", 'warn')
            missing = [
                key for key in ("actionId", "actionType")
                if key not in action
            ]
            if missing:
                return self._fail(f"invalid instantActions format: instantActions[{index}] missing required keys {missing}", 'warn')
        return True

    def _save_to_yaml(self, data: dict, prefix: str, doc_id: str = None):
        """검증 완료된 데이터를 타임스탬프 기반 YAML 파일로 저장합니다."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_id = str(doc_id).strip() if doc_id else timestamp
        filename = f"{prefix}_{safe_id}.yaml"
        filepath = os.path.join(self.output_dir, filename)

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False,
                          sort_keys=False, allow_unicode=True)
            self._log(f"YAML 저장 완료 → {filepath}")
            return True
        except Exception as e:
            return self._fail(f"YAML save failed: {e}", 'error')

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------

    def process_and_save_result(self, topic: str, payload):
        """MQTT 수신 페이로드를 검증하고 YAML로 저장합니다."""
        self.last_error = ""
        if isinstance(payload, (str, bytes)):
            try:
                data = json.loads(payload)
            except (json.JSONDecodeError, ValueError) as e:
                message = f"invalid JSON format: {e}"
                self._fail(message, 'error')
                return False, message, None
        elif isinstance(payload, dict):
            data = payload
        else:
            message = f"unsupported payload type: {type(payload)}"
            self._fail(message, 'error')
            return False, message, None

        topic_lower = topic.lower()

        if topic_lower.endswith('/order'):
            if self._validate_order(data):
                saved = self._save_to_yaml(data, "order", data.get("orderId"))
                return saved, "" if saved else self.last_error, data
            return False, self.last_error or "invalid order format", data

        if topic_lower.endswith('/instantactions'):
            if self._validate_instant_actions(data):
                saved = self._save_to_yaml(data, "instant_actions")
                return saved, "" if saved else self.last_error, data
            return False, self.last_error or "invalid instantActions format", data

        self._log(f"처리 대상이 아닌 토픽: {topic}", 'debug')
        return False, f"unsupported topic: {topic}", data

    def process_and_save(self, topic: str, payload) -> bool:
        success, _message, _data = self.process_and_save_result(topic, payload)
        return success
