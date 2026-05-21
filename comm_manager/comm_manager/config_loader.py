import copy
import glob
import os

import yaml


INCLUDE_KEYS = ("config_includes", "includes")


def deep_merge(base, override):
    result = copy.deepcopy(base) if isinstance(base, dict) else {}
    if not isinstance(override, dict):
        return result

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _load_yaml_file(path):
    with open(path, "r") as stream:
        return yaml.safe_load(stream) or {}


def load_config_file(path, _visited=None):
    path = os.path.abspath(os.path.expanduser(path))
    if _visited is None:
        _visited = set()
    if path in _visited:
        raise ValueError(f"recursive config include detected: {path}")
    _visited.add(path)

    current = _load_yaml_file(path)
    if not isinstance(current, dict):
        raise ValueError(f"config root must be a mapping: {path}")

    base_dir = os.path.dirname(path)
    merged = {}
    includes = []
    for key in INCLUDE_KEYS:
        includes.extend(_as_list(current.get(key)))

    for include in includes:
        pattern = os.path.expanduser(str(include))
        if not os.path.isabs(pattern):
            pattern = os.path.join(base_dir, pattern)
        matched = sorted(glob.glob(pattern, recursive=True))
        if not matched:
            raise FileNotFoundError(f"config include not found: {include} from {path}")
        for include_path in matched:
            if os.path.isdir(include_path):
                continue
            merged = deep_merge(merged, load_config_file(include_path, _visited))

    local = {
        key: value
        for key, value in current.items()
        if key not in INCLUDE_KEYS and not str(key).startswith("_")
    }
    merged = deep_merge(merged, local)
    _visited.remove(path)
    return merged
