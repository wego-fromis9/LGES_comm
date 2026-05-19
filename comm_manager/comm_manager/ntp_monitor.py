import re
import subprocess
import threading

from .host_setup import apply_ntp_client, resolve_ntp_server


class NtpMonitor:
    def __init__(self, config, config_path, logger):
        self.config = config or {}
        self.config_path = config_path
        self.logger = logger
        self.ntp_config = self.config.get("host_setup", {}).get("ntp_client", {}) or {}
        self.monitor_config = self.ntp_config.get("monitor", {}) or {}
        self.stop_event = threading.Event()
        self.thread = None
        self.last_state = None

    def enabled(self):
        return bool(self.ntp_config.get("enabled", False)) and bool(self.monitor_config.get("enabled", False))

    def start(self):
        if not self.enabled():
            return
        self.thread = threading.Thread(target=self._run, daemon=True, name="comm-ntp-monitor")
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)

    def _run(self):
        if self.ntp_config.get("apply_on_comm_start", False):
            self.apply_host_setup()

        if self.monitor_config.get("run_makestep_on_start", False):
            self.run_makestep("startup")

        if self.monitor_config.get("check_on_start", True):
            self.check_once()

        interval = max(float(self.monitor_config.get("check_interval_sec", 60)), 1.0)
        while not self.stop_event.wait(interval):
            self.check_once()

    def apply_host_setup(self):
        try:
            apply_ntp_client(config_path=self.config_path, dry_run=False, no_restart=False)
        except PermissionError as exc:
            self.logger.warn(f"NTP host setup skipped: root permission required ({exc})")
        except Exception as exc:
            self.logger.warn(f"NTP host setup failed: {exc}")

    def run_command(self, command):
        timeout = float(self.monitor_config.get("command_timeout_sec", 5))
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    def run_makestep(self, reason):
        result = self.run_command(["chronyc", "-a", "makestep"])
        if result.returncode == 0:
            self.logger.info(f"NTP makestep requested ({reason}).")
        else:
            stderr = (result.stderr or result.stdout or "").strip()
            self.logger.warn(f"NTP makestep failed ({reason}): {stderr}")

    def check_once(self):
        try:
            server = resolve_ntp_server(self.config, self.ntp_config)
            tracking = self.run_command(["chronyc", "tracking"])
            sources = self.run_command(["chronyc", "sources", "-v"])
        except Exception as exc:
            self.log_state("command_error", f"NTP monitor command failed: {exc}")
            return

        if tracking.returncode != 0:
            text = (tracking.stderr or tracking.stdout or "").strip()
            self.log_state("tracking_error", f"NTP tracking failed: {text}")
            return

        tracking_text = tracking.stdout or ""
        sources_text = sources.stdout or ""
        leap = self.extract_field(tracking_text, "Leap status")
        ref = self.extract_field(tracking_text, "Reference ID")
        offset = self.extract_system_offset(tracking_text)
        normal = str(leap).lower() == "normal"

        if not normal:
            self.log_state(
                "unsynchronised",
                f"NTP not synchronised. server={server}, reference={ref or 'none'}, leap={leap or 'unknown'}",
            )
            if self.monitor_config.get("run_makestep_when_unsynchronised", False):
                self.run_makestep("unsynchronised")
            return

        max_offset = float(self.monitor_config.get("max_offset_sec", 1.0))
        if offset is not None and abs(offset) > max_offset:
            self.log_state(
                "offset_high",
                f"NTP offset high. server={server}, reference={ref}, offset={offset:.6f}s",
            )
            return

        selected = self.extract_selected_source(sources_text)
        self.log_state(
            "normal",
            f"NTP synchronised. server={server}, selected={selected or ref or 'unknown'}, offset={offset if offset is not None else 'unknown'}s",
        )

    def log_state(self, state, message):
        log_every_check = bool(self.monitor_config.get("log_every_check", False))
        if state == self.last_state and not log_every_check:
            return
        self.last_state = state
        if state == "normal":
            self.logger.info(message)
        else:
            self.logger.warn(message)

    def extract_field(self, text, field):
        pattern = re.compile(rf"^{re.escape(field)}\s*:\s*(.+)$", re.MULTILINE)
        match = pattern.search(text or "")
        return match.group(1).strip() if match else ""

    def extract_system_offset(self, text):
        match = re.search(
            r"^System time\s*:\s*([+-]?\d+(?:\.\d+)?)\s+seconds\s+(fast|slow)",
            text or "",
            re.MULTILINE,
        )
        if not match:
            return None
        value = float(match.group(1))
        return -value if match.group(2) == "slow" else value

    def extract_selected_source(self, text):
        for line in (text or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("^*") or stripped.startswith("=*") or stripped.startswith("#*"):
                parts = stripped.split()
                return parts[1] if len(parts) > 1 else stripped
        return ""
