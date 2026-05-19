#!/usr/bin/env python3
import argparse
import os
import re
import shutil
import subprocess
from datetime import datetime

import yaml


BEGIN_MARKER = "# BEGIN LGES managed NTP client"
END_MARKER = "# END LGES managed NTP client"


def resolve_config_path():
    env_path = os.environ.get("COMM_MANAGER_CONFIG_PATH")
    if env_path:
        expanded = os.path.expanduser(env_path)
        if os.path.exists(expanded):
            return expanded

    try:
        from ament_index_python.packages import get_package_share_directory

        package_share_dir = get_package_share_directory("comm_manager")
        workspace_root = os.path.abspath(os.path.join(package_share_dir, "..", "..", "..", ".."))
        source_path = os.path.join(workspace_root, "src", "comm_manager", "config", "config.yaml")
        share_path = os.path.join(package_share_dir, "config", "config.yaml")

        for candidate in (source_path, share_path):
            if os.path.exists(candidate):
                return candidate
        return share_path
    except Exception:
        pass

    for candidate in (
        "/home/wego/LGES_ws/src/comm_manager/config/config.yaml",
        os.path.abspath(os.path.join(os.getcwd(), "src", "comm_manager", "config", "config.yaml")),
    ):
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError(
        "Could not resolve comm_manager config.yaml. "
        "Pass it explicitly with --config /home/wego/LGES_ws/src/comm_manager/config/config.yaml"
    )


def load_config(path):
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_ntp_server(config, ntp_config):
    explicit = ntp_config.get("server")
    if explicit:
        return str(explicit).strip()
    if ntp_config.get("server_from_mqtt_broker", False):
        return str(config.get("mqtt", {}).get("broker_ip") or "").strip()
    return ""


def validate_server(server):
    if not server:
        raise ValueError("NTP server is empty")
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", server):
        raise ValueError(f"NTP server contains unsupported characters: {server!r}")


def strip_managed_block(text):
    pattern = re.compile(
        rf"^{re.escape(BEGIN_MARKER)}$.*?^{re.escape(END_MARKER)}$\n?",
        re.MULTILINE | re.DOTALL,
    )
    return re.sub(pattern, "", text)


def comment_line(line, reason):
    if line.startswith("# LGES disabled:"):
        return line
    return f"# LGES disabled: {reason}: {line}"


def rewrite_chrony_conf(text, server, options):
    text = strip_managed_block(text)
    output = []
    server_re = re.compile(r"^\s*server\s+")
    pool_re = re.compile(r"^\s*pool\s+")
    dhcp_re = re.compile(r"^\s*sourcedir\s+/run/chrony-dhcp(?:\s+.*)?$")

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            output.append(line)
            continue
        if options.get("disable_public_pools", True) and pool_re.match(line):
            output.append(comment_line(line, "public NTP pool disabled"))
            continue
        if options.get("disable_dhcp_sources", True) and dhcp_re.match(line):
            output.append(comment_line(line, "DHCP NTP sources disabled"))
            continue
        if options.get("disable_other_servers", True) and server_re.match(line):
            output.append(comment_line(line, "managed server is configured below"))
            continue
        output.append(line)

    output.extend([
        "",
        BEGIN_MARKER,
        f"server {server} iburst prefer",
        END_MARKER,
        "",
    ])
    return "\n".join(output)


def run_command(command, check=True):
    print(f"+ {' '.join(command)}")
    return subprocess.run(command, check=check)


def apply_ntp_client(config_path, chrony_conf_override=None, dry_run=False, no_restart=False):
    config = load_config(config_path)
    ntp_config = config.get("host_setup", {}).get("ntp_client", {}) or {}
    if not ntp_config.get("enabled", False):
        print("host_setup.ntp_client.enabled is false; nothing to apply.")
        return 0

    server = resolve_ntp_server(config, ntp_config)
    validate_server(server)
    chrony_conf = chrony_conf_override or ntp_config.get("chrony_conf") or "/etc/chrony/chrony.conf"
    chrony_conf = os.path.expanduser(str(chrony_conf))

    if os.geteuid() != 0 and not dry_run:
        raise PermissionError(
            "Root permission is required to modify chrony/systemd settings. "
            "Run: sudo -E ros2 run comm_manager apply_host_setup"
        )

    with open(chrony_conf, "r", encoding="utf-8") as handle:
        current = handle.read()
    updated = rewrite_chrony_conf(current, server, ntp_config)

    print(f"Config file: {config_path}")
    print(f"Chrony conf: {chrony_conf}")
    print(f"NTP server : {server}")

    if dry_run:
        print("\n--- updated chrony.conf preview ---")
        print(updated)
        return 0

    if updated != current:
        backup = f"{chrony_conf}.lges.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(chrony_conf, backup)
        with open(chrony_conf, "w", encoding="utf-8") as handle:
            handle.write(updated)
        print(f"Updated chrony config. Backup: {backup}")
    else:
        print("Chrony config is already up to date.")

    if not no_restart and ntp_config.get("restart_chrony", True):
        run_command(["timedatectl", "set-ntp", "true"])
        run_command(["systemctl", "enable", "--now", "chrony"])
        run_command(["systemctl", "restart", "chrony"])
        if ntp_config.get("makestep", True):
            run_command(["chronyc", "-a", "makestep"], check=False)
        run_command(["chronyc", "sources", "-v"], check=False)
        run_command(["chronyc", "tracking"], check=False)

    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Apply LGES host-level setup from comm_manager config.")
    parser.add_argument("--config", default=None, help="Path to comm_manager config.yaml")
    parser.add_argument("--chrony-conf", default=None, help="Override chrony.conf path")
    parser.add_argument("--dry-run", action="store_true", help="Print the chrony.conf result without writing")
    parser.add_argument("--no-restart", action="store_true", help="Do not restart chrony after writing")
    args = parser.parse_args(argv)

    config_path = os.path.expanduser(args.config) if args.config else resolve_config_path()
    return apply_ntp_client(
        config_path=config_path,
        chrony_conf_override=args.chrony_conf,
        dry_run=args.dry_run,
        no_restart=args.no_restart,
    )


if __name__ == "__main__":
    raise SystemExit(main())
