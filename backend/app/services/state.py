"""状态后端管理业务规则：状态流转、快照清理与状态快照数据清单都收在这里。

快照清理的约束集中在本文件：

- 先读 STAT 状态快照与 CHEC 检查点引用，校验引用集合后再删除目录；
- 被恢复检查点（含仍停留在恢复待办里的已过期检查点）引用的存储路径整组保留；
- 同一路径由清理登记表保证只成功删除一次；
- 引用集合、删除结果、受保护大小与失败项在同一事务里写入状态快照数据清单；
- 失败项保留原目录，后续只从未完成路径继续。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

MODULE = "state"
REQUIRED_FIELDS = ["配置编号", "后端类型", "存储路径"]
STATUS_ORDER = ["草稿", "已启用", "已暂停", "已关闭"]
ACTION_RULES = {"启用后端": "草稿", "暂停后端": "已启用", "关闭后端": "已暂停"}
NEGATIVE_ACTIONS = []
CLEANUP_ACTIONS = {"清理快照", "清理过期快照", "批量清理快照", "批量清理过期快照", "清理"}

MANIFEST_MODULE = "_state_manifest"
CLEANUP_MODULE = "_state_cleanup"
CONSUMER_MODULE = "_state_manifest_consumer"
RESTORE_TODO_MODULE = "_state_restore_todo"
CHECKPOINT_MODULE = "checkpoint"

CHECKPOINT_STATUS_ORDER = ["已创建", "已恢复", "已过期", "已清理"]
RESTORABLE_CHECKPOINT_STATUSES = {"已创建", "已恢复"}
EXPIRED_STATUS = "已过期"
CLEANED_STATUS = "已清理"
FAILED_STATUS = "失败"

STATE_OPTIONAL_FIELDS = ["状态大小", "过期策略", "清理间隔", "快照频率"]
CHECKPOINT_OPTIONAL_FIELDS = ["状态大小", "保存路径", "创建时间", "恢复耗时"]

_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?B|字节)?\s*$", re.IGNORECASE)
_SIZE_UNITS = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4, "PB": 1024 ** 5}
_SPLIT_RE = re.compile(r"[,，、;；\s]+")


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _as_list(value: Any) -> list[Any]:
    """把单个编号、逗号分隔的编号或列表统一成列表，兼容历史提交口径。"""
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if item not in (None, "")]
    if isinstance(value, dict):
        return [value]
    return [item for item in _SPLIT_RE.split(str(value)) if item]


def _first(mapping: dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return default


def _normalize_path(value: Any) -> str:
    """存储路径按目录口径归一：去空白、补结尾斜杠，避免同一路径出现两种写法。"""
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.endswith("/") else f"{text}/"


def _path_overlaps(left: str, right: str) -> bool:
    """两个目录相同、或一方位于另一方之下，即视为同一组快照。"""
    if not left or not right:
        return False
    return left == right or left.startswith(right) or right.startswith(left)


def parse_size(value: Any) -> int | None:
    """把 1.2GB、300MB、0B 或纯字节数解析成字节；无法识别时返回 None。"""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return max(int(value), 0)
    text = str(value).strip()
    if not text:
        return None
    match = _SIZE_RE.match(text)
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "B").upper()
    if unit == "字节":
        unit = "B"
    factor = _SIZE_UNITS.get(unit)
    if factor is None:
        return None
    return max(int(round(number * factor)), 0)


def format_size(size: int) -> str:
    """把字节数格式化成便于核对的大小，保留一位小数、去掉多余的 .0。"""
    value = max(int(size), 0)
    if value == 0:
        return "0B"
    for unit in ["PB", "TB", "GB", "MB", "KB"]:
        factor = _SIZE_UNITS[unit]
        if value >= factor:
            formatted = f"{value / factor:.1f}".rstrip("0").rstrip(".")
            return f"{formatted}{unit}"
    return f"{value}B"


def _size_bytes(row: dict[str, Any]) -> int:
    stored = row.get("状态大小字节")
    if isinstance(stored, bool):
        stored = None
    if isinstance(stored, (int, float)):
        return max(int(stored), 0)
    parsed = parse_size(row.get("状态大小"))
    return parsed if parsed is not None else 0


def _record_id(row: dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = str(row.get(field) or "").strip()
        if value:
            return value
    return str(row.get("id", ""))


def _checkpoint_status(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "").strip()
    if status not in CHECKPOINT_STATUS_ORDER:
        status = str(row.get("记录状态") or "").strip()
    if status not in CHECKPOINT_STATUS_ORDER:
        status = CHECKPOINT_STATUS_ORDER[0]
    return status


def normalize_state_row(row: dict[str, Any]) -> dict[str, Any]:
    """修复历史数据里 status 与配置状态不一致、路径写法不一、大小字段缺失等问题。"""
    status = str(row.get("status") or "").strip()
    if status not in STATUS_ORDER:
        fallback = str(row.get("配置状态") or "").strip()
        status = fallback if fallback in STATUS_ORDER else STATUS_ORDER[0]
    row["status"] = status
    row["配置状态"] = status
    row["存储路径"] = _normalize_path(row.get("存储路径"))
    row["pending"] = bool(row.get("pending", status != STATUS_ORDER[-1]))
    if "abnormal" not in row:
        row["abnormal"] = False
    parsed = parse_size(row.get("状态大小"))
    if parsed is not None:
        row["状态大小字节"] = parsed
    elif not isinstance(row.get("状态大小字节"), (int, float)):
        row["状态大小字节"] = 0
        if not str(row.get("状态大小") or "").strip():
            row["状态大小"] = "0B"
    if not row.get("快照状态"):
        row["快照状态"] = "已删除" if row["状态大小字节"] == 0 and row.get("清理清单编号") else "正常"
    return row


def normalize_checkpoint_row(row: dict[str, Any]) -> dict[str, Any]:
    """修复检查点记录状态、保存路径与大小字段，保留既有展示字段。"""
    status = _checkpoint_status(row)
    row["status"] = status
    row["记录状态"] = status
    row["保存路径"] = _normalize_path(row.get("保存路径"))
    parsed = parse_size(row.get("状态大小"))
    if parsed is not None:
        row["状态大小字节"] = parsed
    elif not isinstance(row.get("状态大小字节"), (int, float)):
        row["状态大小字节"] = 0
        if not str(row.get("状态大小") or "").strip():
            row["状态大小"] = "0B"
    row.setdefault("可恢复", status in RESTORABLE_CHECKPOINT_STATUSES)
    return row


def _restore_todo_ids() -> set[str]:
    """恢复待办里的检查点即便已经过期，也仍然是可恢复引用，不能当作无引用目录删除。"""
    ids: set[str] = set()
    for row in store.rows(RESTORE_TODO_MODULE):
        todo_status = str(row.get("状态") or "").strip()
        if todo_status in {"已完成", "已取消", "已恢复"}:
            continue
        checkpoint_id = str(row.get("检查点编号") or "").strip()
        if checkpoint_id:
            ids.add(checkpoint_id)
    return ids


def _collect_references() -> list[dict[str, Any]]:
    """汇总当前所有可恢复检查点引用；已过期但仍在恢复待办中的也算引用。"""
    todo_ids = _restore_todo_ids()
    references: list[dict[str, Any]] = []
    for row in store.rows(CHECKPOINT_MODULE):
        normalize_checkpoint_row(row)
        status = _checkpoint_status(row)
        checkpoint_id = _record_id(row, "记录编号")
        restorable = status in RESTORABLE_CHECKPOINT_STATUSES
        if status == EXPIRED_STATUS and checkpoint_id in todo_ids:
            restorable = True
        if not restorable:
            continue
        path = _normalize_path(row.get("保存路径"))
        if not path:
            continue
        references.append({
            "检查点": checkpoint_id,
            "路径": path,
            "状态": status,
            "来源": "恢复待办" if status == EXPIRED_STATUS and checkpoint_id in todo_ids else "检查点恢复",
            "可恢复": True,
        })
    return references


def _references_for_path(path: str, references: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in references if _path_overlaps(path, item["路径"])]


def _manifest_paths(manifest: dict[str, Any]) -> list[str]:
    paths = [manifest.get("存储路径"), *_as_list(manifest.get("路径组"))]
    result: list[str] = []
    for path in paths:
        normalized = _normalize_path(path)
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _manifest_sort_key(manifest: dict[str, Any]) -> int:
    try:
        return int(manifest.get("id", 0))
    except (TypeError, ValueError):
        return 0


def _dedupe_records(records: list[dict[str, Any]], key: str = "路径") -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for record in records:
        marker = str(record.get(key) or "") or json.dumps(record, ensure_ascii=False, sort_keys=True)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(record)
    return result


def _normalize_manifest(row: dict[str, Any]) -> dict[str, Any]:
    """兼容历史清单：补齐缺失的进度、批量结果与版本字段，不覆盖已有删除结果。"""
    row["存储路径"] = _normalize_path(row.get("存储路径"))
    path_group = [_normalize_path(path) for path in _as_list(row.get("路径组"))]
    if row["存储路径"] and row["存储路径"] not in path_group:
        path_group.insert(0, row["存储路径"])
    row["路径组"] = [path for path in path_group if path]
    row["引用集合"] = [_normalize_path(path) for path in _as_list(row.get("引用集合")) if _normalize_path(path)]
    row["引用快照"] = [str(item) for item in _as_list(row.get("引用快照"))]
    row["处理记录"] = [str(item) for item in _as_list(row.get("处理记录"))]
    row["删除结果"] = [dict(item) if isinstance(item, dict) else {"路径": str(item), "结果": "已删除"}
                     for item in row.get("删除结果", []) if item]
    row["失败项"] = [dict(item) if isinstance(item, dict) else {"路径": str(item), "结果": FAILED_STATUS}
                   for item in row.get("失败项", []) if item]
    row["保护项"] = [dict(item) if isinstance(item, dict) else {"路径": str(item), "结果": "保留"}
                   for item in row.get("保护项", []) if item]
    batch_results = row.get("批量处理结果", [])
    row["批量处理结果"] = [dict(item) for item in batch_results if isinstance(item, dict)]
    row["批次记录"] = [dict(item) for item in row.get("批次记录", []) if isinstance(item, dict)]
    protected_bytes = row.get("受保护字节")
    if not isinstance(protected_bytes, (int, float)):
        protected_bytes = parse_size(row.get("受保护大小")) or 0
    row["受保护字节"] = max(int(protected_bytes), 0)
    row["受保护大小"] = format_size(row["受保护字节"])
    try:
        row["状态快照版本"] = max(int(row.get("状态快照版本", 1)), 1)
    except (TypeError, ValueError):
        row["状态快照版本"] = 1
    try:
        row["版本标识"] = max(int(row.get("版本标识", 1)), 1)
    except (TypeError, ValueError):
        row["版本标识"] = 1
    row.setdefault("状态", "已生成")
    row.setdefault("清单编号", f"SNAP-{int(row.get('id', 0)):04d}")
    row.setdefault("创建时间", _now())
    row.setdefault("更新时间", row["创建时间"])
    row.setdefault("请求指纹", "")
    row.setdefault("关联清单", {})
    row.setdefault("一致性", "引用集合与删除结果同事务落库")
    if not row.get("完成路径"):
        row["完成路径"] = sorted({
            _normalize_path(item.get("路径")) for item in
            [*row["删除结果"], *row["保护项"], *row["失败项"]]
            if isinstance(item, dict) and item.get("路径")
        })
    if not row.get("整组进度"):
        row["整组进度"] = {
            "总数": len(row["路径组"]),
            "已删除": len(row["删除结果"]),
            "已保留": len(row["保护项"]),
            "失败": len(row["失败项"]),
            "跳过": 0,
            "未完成": len(row["失败项"]),
            "完成": not row["失败项"],
        }
    return row


def _resolve_state_entries(values: dict[str, Any], explicit_ids: list[Any]) -> list[dict[str, Any]]:
    """按 id、配置编号或存储路径定位 STAT 状态快照；重复写法只保留一条记录。"""
    rows = store.rows(MODULE)
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    tokens: list[Any] = []
    for key in ["状态快照", "状态快照编号", "state_ids", "state_entries", "state_snapshots", "ids"]:
        tokens.extend(_as_list(values.get(key)))
    for item in _as_list(values.get("entries") or values.get("items") or values.get("records")):
        if isinstance(item, dict):
            kind = str(_first(item, ["类型", "type", "module", "模块"], "")).upper()
            if "CHEC" in kind or "checkpoint" in kind.lower():
                continue
            tokens.append(_first(item, ["配置编号", "编号", "id", "path", "存储路径"], item))
        else:
            tokens.append(item)
    tokens.extend(explicit_ids)
    for token in tokens:
        if isinstance(token, dict):
            path = _normalize_path(_first(token, ["存储路径", "path"], ""))
            config_id = str(_first(token, ["配置编号", "编号", "id"], "")).strip()
        else:
            text = str(token).strip()
            path = _normalize_path(text)
            config_id = text
        matched: dict[str, Any] | None = None
        if config_id.isdigit():
            matched = store.find(MODULE, int(config_id))
        if matched is None and config_id:
            matched = next((row for row in rows if str(row.get("配置编号") or "") == config_id), None)
        if matched is None:
            matched = next((row for row in rows if _normalize_path(row.get("存储路径")) == path), None)
        if matched is None:
            # 允许直接按存储路径提交：命中该路径下的状态快照。
            matched = next((row for row in rows if _path_overlaps(_normalize_path(row.get("存储路径")), path)), None)
        if matched is not None and int(matched.get("id", 0)) not in seen:
            seen.add(int(matched.get("id", 0)))
            result.append(normalize_state_row(matched))
    return result


def _resolve_checkpoint_entries(values: dict[str, Any], state_paths: list[str]) -> list[dict[str, Any]]:
    """按 id、记录编号、保存路径定位 CHEC 检查点；未显式提交时也读取关联引用。"""
    rows = store.rows(CHECKPOINT_MODULE)
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    tokens: list[Any] = []
    for key in ["检查点", "检查点编号", "checkpoint_ids", "checkpoints", "checkpoint_entries"]:
        tokens.extend(_as_list(values.get(key)))
    for item in _as_list(values.get("entries") or values.get("items") or values.get("records")):
        if isinstance(item, dict):
            kind = str(_first(item, ["类型", "type", "module", "模块"], "")).upper()
            if "STAT" in kind or "state" in kind.lower():
                continue
            tokens.append(_first(item, ["记录编号", "编号", "id", "path", "保存路径"], item))
        else:
            tokens.append(item)
    # 字符串形式的 "CHEC-0003" 即使落在 ids 里也能识别。
    for key in ["ids", "状态快照", "记录"]:
        for token in _as_list(values.get(key)):
            text = str(token).strip().upper()
            if text.startswith("CHEC") or text.startswith("CHECKPOINT"):
                tokens.append(token)
    for token in tokens:
        matched: dict[str, Any] | None = None
        if isinstance(token, dict):
            record_id = str(_first(token, ["记录编号", "编号", "id"], "")).strip()
            path = _normalize_path(_first(token, ["保存路径", "path"], ""))
        else:
            text = str(token).strip()
            record_id = text
            path = _normalize_path(text) if "://" in text else ""
        if record_id.isdigit():
            matched = store.find(CHECKPOINT_MODULE, int(record_id))
        if matched is None and record_id:
            matched = next((row for row in rows if str(row.get("记录编号") or "") == record_id), None)
        if matched is None and path:
            matched = next((row for row in rows if _normalize_path(row.get("保存路径")) == path), None)
        if matched is not None and int(matched.get("id", 0)) not in seen:
            seen.add(int(matched.get("id", 0)))
            result.append(normalize_checkpoint_row(matched))
    # 提交状态快照时，把落在这些路径下的检查点一并纳入引用校验与关联记录。
    for row in rows:
        normalize_checkpoint_row(row)
        path = _normalize_path(row.get("保存路径"))
        if not path or not any(_path_overlaps(path, state_path) for state_path in state_paths):
            continue
        if int(row.get("id", 0)) not in seen:
            seen.add(int(row.get("id", 0)))
            result.append(row)
    return result


def _failure_requests(values: dict[str, Any]) -> dict[str, str]:
    """失败注入：支持路径列表或 {路径: 原因}，用于验证失败项保留原目录。"""
    raw = _first(values, ["失败路径", "模拟失败路径", "fail_paths", "simulate_failures", "failures"], [])
    result: dict[str, str] = {}
    if isinstance(raw, dict):
        for path, reason in raw.items():
            result[_normalize_path(path)] = str(reason or "模拟删除失败")
        return result
    for item in _as_list(raw):
        if isinstance(item, dict):
            path = _normalize_path(_first(item, ["路径", "path"], ""))
            reason = str(_first(item, ["原因", "reason"], "模拟删除失败"))
            if path:
                result[path] = reason
        else:
            path = _normalize_path(item)
            if path:
                result[path] = "模拟删除失败"
    return result


def _find_ledger(path: str) -> dict[str, Any] | None:
    for row in store.rows(CLEANUP_MODULE):
        if _normalize_path(row.get("路径")) == path:
            return row
    return None


def _upsert_ledger(path: str, manifest_id: int, result: str, reason: str = "", checkpoint: str = "") -> dict[str, Any]:
    row = _find_ledger(path)
    if row is None:
        row = {"路径": path}
        store.rows(CLEANUP_MODULE).append(row)
    row.update({
        "路径": path,
        "清单编号": f"SNAP-{int(manifest_id):04d}",
        "结果": result,
        "原因": reason,
        "检查点": checkpoint,
    })
    return row


def _manifest_by_id(manifest_id: int) -> dict[str, Any] | None:
    for row in store.rows(MANIFEST_MODULE):
        try:
            if int(row.get("id", 0)) == int(manifest_id):
                return _normalize_manifest(row)
        except (TypeError, ValueError):
            continue
    return None


def _locate_manifests(path: str) -> list[dict[str, Any]]:
    """同一目录或父子目录都视为同一组快照，只允许被一份清单接管。"""
    matches: list[dict[str, Any]] = []
    for row in store.rows(MANIFEST_MODULE):
        _normalize_manifest(row)
        if any(_path_overlaps(path, candidate) for candidate in _manifest_paths(row)):
            matches.append(row)
    return sorted(matches, key=_manifest_sort_key)


def _protected_bytes(records: list[dict[str, Any]]) -> int:
    """受保护大小按目录组归并：父目录已计入时，子目录不再重复累加。"""
    sizes: dict[str, int] = {}
    for item in records:
        path = _normalize_path(item.get("路径"))
        if not path:
            continue
        sizes[path] = max(sizes.get(path, 0), int(item.get("字节", 0) or 0))
    total = 0
    for path, size in sizes.items():
        if any(other != path and path.startswith(other) for other in sizes):
            continue
        total += size
    return total


def _consistency_issue(path: str, references: list[dict[str, Any]]) -> str:
    """跨链路一致性：已登记删除的目录不能再被恢复待办引用，否则拒绝删除。

    已过期检查点仍出现在恢复待办时，它就是一个可恢复引用；如果清理登记或既有清单
    已经把这个目录记为删除，两条链路的依据冲突，只能保留目录并登记失败项。
    """
    relevant = _references_for_path(path, references)
    if not relevant:
        return ""
    for ledger in store.rows(CLEANUP_MODULE):
        ledger_path = _normalize_path(ledger.get("路径"))
        if ledger.get("结果") != "已删除" or not _path_overlaps(path, ledger_path):
            continue
        conflicting = [
            item for item in relevant
            if any(_path_overlaps(ledger_path, reference_path)
                   for reference_path in [item["路径"]])
        ]
        if conflicting:
            checkpoint_ids = "、".join(sorted({item["检查点"] for item in conflicting}))
            return f"跨链路冲突：{ledger_path} 已登记删除，但恢复待办仍引用 {checkpoint_ids}，保留原目录"
    for manifest in _locate_manifests(path):
        for record in manifest.get("删除结果", []):
            if _path_overlaps(_normalize_path(record.get("路径")), path):
                checkpoint_ids = "、".join(sorted({item["检查点"] for item in relevant}))
                return f"跨链路冲突：{manifest.get('清单编号')} 已删除 {path}，但恢复待办仍引用 {checkpoint_ids}"
    return ""


def _manifest_for_path(path: str, paths: list[str]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """一条已定位的状态快照数据清单：同一路径只允许被一份清单接管。"""
    matches = _locate_manifests(path)
    if len(matches) > 1:
        return None, matches
    if matches:
        return matches[0], matches
    rows = store.rows(MANIFEST_MODULE)
    manifest_id = max((_manifest_sort_key(row) for row in rows), default=0) + 1
    manifest = _normalize_manifest({
        "id": manifest_id,
        "清单编号": f"SNAP-{manifest_id:04d}",
        "存储路径": path,
        "路径组": paths,
        "后端配置编号": "",
        "后端配置编号列表": [],
        "状态快照版本": 1,
        "引用集合": [],
        "引用快照": [],
        "处理记录": [],
        "关联清单": {},
        "删除结果": [],
        "保护项": [],
        "受保护大小": "0B",
        "受保护字节": 0,
        "失败项": [],
        "批量处理结果": [],
        "批次记录": [],
        "整组进度": {},
        "状态": "已生成",
        "版本标识": 1,
        "请求指纹": "",
        "创建时间": _now(),
        "更新时间": _now(),
    })
    rows.append(manifest)
    return manifest, []


def _fingerprint(state_rows: list[dict[str, Any]], checkpoint_rows: list[dict[str, Any]], values: dict[str, Any]) -> str:
    provided = str(_first(values, ["提交编号", "request_id", "batch_id", "idempotency_key"], "") or "").strip()
    if provided:
        return f"request:{provided}"
    payload = {
        "state": sorted(str(row.get("配置编号") or row.get("id")) for row in state_rows),
        "checkpoint": sorted(str(row.get("记录编号") or row.get("id")) for row in checkpoint_rows),
        "fail": sorted(_failure_requests(values)),
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _expected_actions(
    state_rows: list[dict[str, Any]],
    references: list[dict[str, Any]],
    failure_requests: dict[str, str],
) -> dict[str, str]:
    """按当前引用集合推断每条快照应得到的动作，用于判断重复提交是否会产生新结果。

    已删除（跳过）与本次删除等价：目录都已经不在，重复提交不能再删一次。
    """
    expected: dict[str, str] = {}
    for row in state_rows:
        path = _normalize_path(row.get("存储路径"))
        if _consistency_issue(path, references):
            action = "失败"
        elif _references_for_path(path, references):
            action = "保留"
        else:
            ledger = _find_ledger(path)
            if ledger is not None and ledger.get("结果") == "已删除":
                action = "跳过"
            elif failure_requests.get(path) or not path:
                action = "失败"
            else:
                action = "删除"
        expected[path] = action
    return expected


def _same_actions(recorded: dict[str, str], expected: dict[str, str]) -> bool:
    if set(recorded) != set(expected):
        return False
    for path, action in expected.items():
        other = recorded[path]
        if action != other and {action, other} != {"删除", "跳过"}:
            return False
    return True


class StateService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [normalize_state_row(row) for row in store.rows(MODULE)]
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("配置编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        return normalize_state_row(entry) if entry is not None else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in STATE_OPTIONAL_FIELDS:
            if field in values:
                entry[field] = values.get(field)
        entry["存储路径"] = _normalize_path(entry.get("存储路径"))
        entry["status"] = STATUS_ORDER[0]
        entry["配置状态"] = STATUS_ORDER[0]
        entry["状态大小"] = entry.get("状态大小") or "0B"
        entry["状态大小字节"] = parse_size(entry.get("状态大小")) or 0
        entry["快照状态"] = "正常"
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"后端配置 {entry_id} 不存在或已归档"
        normalize_state_row(entry)
        if action in CLEANUP_ACTIONS:
            result = self.cleanup_snapshots({"state_ids": [entry_id]})
            if not result.get("ok"):
                return None, str(result.get("message") or "快照清理未完成")
            return result.get("entry"), str(result.get("message") or "快照清理已完成")
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于状态后端管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["配置状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"后端配置已{action}"

    # ------------------------------------------------------------------
    # 过期快照批量清理
    # ------------------------------------------------------------------
    def cleanup_snapshots(self, values: dict[str, Any]) -> dict[str, Any]:
        dry_run = bool(_first(values, ["预演", "dry_run", "preview"], False))
        with store.transaction(commit=not dry_run):
            return self._cleanup_snapshots(values, dry_run)

    def _cleanup_snapshots(self, values: dict[str, Any], dry_run: bool) -> dict[str, Any]:
        state_rows = _resolve_state_entries(values, _as_list(values.get("state_ids")))
        checkpoint_rows = _resolve_checkpoint_entries(
            values, [_normalize_path(row.get("存储路径")) for row in state_rows]
        )
        if not state_rows:
            return {"ok": False, "message": "未定位到可清理的 STAT 状态快照，请提交配置编号或存储路径"}
        fingerprint = _fingerprint(state_rows, checkpoint_rows, values)
        references = _collect_references()
        failure_requests = _failure_requests(values)
        duplicate = [
            row for row in store.rows(MANIFEST_MODULE)
            if _normalize_manifest(row).get("请求指纹") == fingerprint
            or any(item.get("请求指纹") == fingerprint for item in row.get("批次记录", []))
        ]
        if duplicate and self._is_duplicate(duplicate, fingerprint, state_rows, references, failure_requests):
            manifests = sorted((_normalize_manifest(row) for row in duplicate), key=_manifest_sort_key)
            return {
                "ok": True,
                "message": f"重复提交只接受一份记录，已返回既有状态快照数据清单 {len(manifests)} 份",
                "entry": self._batch_summary(manifests, duplicate=True),
                "重复提交": True,
                "manifests": manifests,
            }
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in state_rows:
            groups.setdefault(_normalize_path(row.get("存储路径")), []).append(row)

        manifests: list[dict[str, Any]] = []
        batch_results: list[dict[str, Any]] = []
        assigned: dict[str, dict[str, Any]] = {}
        touched: dict[str, list[str]] = {}
        for path, group_rows in groups.items():
            manifest, _conflicts = _manifest_for_path(path, [path])
            if manifest is None:
                reason = f"跨链路冲突：{path} 命中多条状态快照数据清单"
                for row in group_rows:
                    batch_results.append(self._result(row, path, "冲突", reason))
                _upsert_ledger(path, 0, FAILED_STATUS, reason)
                continue
            # 同一批次的多条存储路径共用一条已定位的清单，整组进度只记这一份。
            key = str(manifest.get("id"))
            shared = assigned.get(key)
            if shared is not None:
                manifest = shared
            assigned[key] = manifest
            touched.setdefault(key, []).append(path)
            # 跨链路冲突与失败注入统一在 _process_group 里判定，失败项保留原目录。
            results = self._process_group(
                manifest, path, group_rows, references, failure_requests, dry_run
            )
            batch_results.extend(results)
            self._apply_results(manifest, path, group_rows, checkpoint_rows, references, results)

        for key, manifest in assigned.items():
            self._finalize_manifest(
                manifest, touched.get(key, []), checkpoint_rows,
                references, fingerprint, batch_results
            )
            manifests.append(manifest)
        summary = self._batch_summary(manifests)
        summary["批量处理结果"] = batch_results
        summary["预演"] = dry_run
        summary["进度"] = summary.get("整组进度", {})
        deleted = sum(1 for item in batch_results if item.get("动作") == "删除")
        protected = sum(1 for item in batch_results if item.get("动作") == "保留")
        failed = sum(1 for item in batch_results if item.get("动作") in {"失败", "冲突"})
        prefix = "预演完成（未删除目录）" if dry_run else "快照清理完成"
        summary["message"] = (
            f"{prefix}：删除 {deleted} 个目录，保留 {protected} 个"
            f"（受保护 {format_size(summary.get('受保护字节', 0))}），失败 {failed} 项"
        )
        summary["ok"] = True
        # ActionResult.entry 携带整组进度与批量结果，避免只回传第一份清单。
        summary["entry"] = {
            "清单编号": summary.get("清单编号", ""),
            "清单列表": manifests,
            "引用集合": summary.get("引用集合", []),
            "引用快照": summary.get("引用快照", []),
            "删除结果": summary.get("删除结果", []),
            "保护项": summary.get("保护项", []),
            "失败项": summary.get("失败项", []),
            "受保护大小": summary.get("受保护大小", "0B"),
            "受保护字节": summary.get("受保护字节", 0),
            "整组进度": summary.get("整组进度", {}),
            "进度": summary.get("进度", {}),
            "批量处理结果": summary.get("批量处理结果", []),
            "版本标识": summary.get("版本标识", 0),
            "预演": dry_run,
            "重复提交": False,
        }
        return summary

    def _is_duplicate(
        self,
        duplicate: list[dict[str, Any]],
        fingerprint: str,
        state_rows: list[dict[str, Any]],
        references: list[dict[str, Any]],
        failure_requests: dict[str, str],
    ) -> bool:
        """内容与既有批次一致才算重复提交；引用集合变了就允许再清理一次。"""
        recorded: dict[str, str] = {}
        for row in duplicate:
            batches = list(row.get("批次记录", []))
            if not batches and row.get("请求指纹") == fingerprint:
                batches = [{"处理结果": row.get("批量处理结果", [])}]
            for batch in batches:
                if batch.get("请求指纹") not in (None, fingerprint):
                    continue
                for item in batch.get("处理结果", []):
                    path = _normalize_path(item.get("路径"))
                    if path:
                        recorded[path] = str(item.get("动作") or "")
        expected = _expected_actions(state_rows, references, failure_requests)
        if not recorded:
            return False
        return _same_actions(recorded, expected)

    def _result(self, row: dict[str, Any], path: str, action: str, reason: str = "",
                checkpoint: str = "", protected_bytes: int = 0) -> dict[str, Any]:
        return {
            "路径": path,
            "配置编号": str(row.get("配置编号") or ""),
            "后端配置编号": str(row.get("配置编号") or ""),
            "检查点": checkpoint,
            "动作": action,
            "结果": reason or ("已删除" if action == "删除" else "保留" if action == "保留" else action),
            "原因": reason,
            "大小": format_size(_size_bytes(row)),
            "字节": _size_bytes(row),
            "受保护字节": protected_bytes,
            "保留目录": action != "删除",
        }

    def _process_group(
        self,
        manifest: dict[str, Any],
        path: str,
        group_rows: list[dict[str, Any]],
        references: list[dict[str, Any]],
        failure_requests: dict[str, str],
        dry_run: bool,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        relevant_refs = _references_for_path(path, references)
        checkpoint_ids = "、".join(sorted({item["检查点"] for item in relevant_refs}))
        issue = _consistency_issue(path, references)
        for row in group_rows:
            row_path = _normalize_path(row.get("存储路径"))
            size = _size_bytes(row)
            if issue:
                # 跨链路依据冲突：不删除目录，登记失败项，整组进度保持未完成。
                results.append(self._result(row, row_path, "失败", issue, checkpoint_ids))
                if not dry_run:
                    _upsert_ledger(path, int(manifest.get("id", 0)), FAILED_STATUS, issue)
                    row["快照状态"] = "冲突保留"
                    row["abnormal"] = True
                continue
            if relevant_refs:
                results.append(self._result(row, row_path, "保留", "被恢复检查点引用，整组保留", checkpoint_ids, size))
                if not dry_run:
                    row["快照状态"] = "已保留"
                    row["受保护大小"] = format_size(size)
                    row["清理清单编号"] = manifest.get("清单编号")
                    row["abnormal"] = False
                continue
            ledger = _find_ledger(row_path)
            if ledger is not None and ledger.get("结果") == "已删除":
                results.append(self._result(row, row_path, "跳过", "同一路径已清理，重复清理只成功一次"))
                continue
            failure_reason = failure_requests.get(row_path)
            if not failure_reason:
                failure_reason = failure_requests.get(path)
            if failure_reason or not row_path:
                reason = failure_reason or "存储路径为空，无法清理"
                results.append(self._result(row, row_path, "失败", reason))
                if not dry_run:
                    _upsert_ledger(row_path or path, int(manifest.get("id", 0)), FAILED_STATUS, reason)
                    row["快照状态"] = "清理失败"
                    row["abnormal"] = True
                continue
            if dry_run:
                results.append(self._result(row, row_path, "删除", "预演：校验通过，未删除目录"))
                continue
            results.append(self._result(row, row_path, "删除", ""))
            _upsert_ledger(row_path, int(manifest.get("id", 0)), "已删除", "")
            row["状态大小"] = "0B"
            row["状态大小字节"] = 0
            row["快照状态"] = "已清理"
            row["清理清单编号"] = manifest.get("清单编号")
            row["abnormal"] = False
        return results

    def _apply_results(
        self,
        manifest: dict[str, Any],
        path: str,
        group_rows: list[dict[str, Any]],
        checkpoint_rows: list[dict[str, Any]],
        references: list[dict[str, Any]],
        results: list[dict[str, Any]],
    ) -> None:
        """把一个路径组的处理结果并入清单；同一路径的旧结果先移除，避免重复记录。"""
        _normalize_manifest(manifest)
        relevant_refs = _references_for_path(path, references)
        manifest["引用集合"] = sorted(set(manifest.get("引用集合", [])) | {item["路径"] for item in relevant_refs})
        manifest["引用快照"] = sorted(set(manifest.get("引用快照", [])) | {item["检查点"] for item in relevant_refs})
        manifest["后端配置编号列表"] = sorted(set(manifest.get("后端配置编号列表", [])) | {
            str(row.get("配置编号") or "") for row in group_rows
        })
        manifest["后端配置编号"] = "、".join(manifest["后端配置编号列表"])
        for item in relevant_refs:
            manifest["关联清单"][item["检查点"]] = manifest.get("清单编号")
        deleted_records = [dict(item) for item in manifest.get("删除结果", [])]
        protected_records = [dict(item) for item in manifest.get("保护项", [])]
        failed_records = [dict(item) for item in manifest.get("失败项", [])]
        completed = set(manifest.get("完成路径", []))
        for result in results:
            action = result.get("动作")
            if action not in {"删除", "保留", "失败", "冲突", "跳过"}:
                continue
            record_path = _normalize_path(result.get("路径"))
            if record_path:
                completed.add(record_path)
            deleted_records = [item for item in deleted_records if item.get("路径") != record_path]
            protected_records = [item for item in protected_records if item.get("路径") != record_path]
            failed_records = [item for item in failed_records if item.get("路径") != record_path]
            if action == "跳过":
                # 同一路径已由其他批次清理完成，不重复写入删除结果。
                continue
            record = {
                "路径": record_path,
                "后端配置编号": result.get("配置编号"),
                "检查点": result.get("检查点", ""),
                "结果": result.get("结果"),
                "字节": result.get("字节", 0),
                "大小": result.get("大小", "0B"),
                "原因": result.get("原因", ""),
                "保留目录": result.get("保留目录", action != "删除"),
            }
            if action == "删除":
                deleted_records.append(record)
            elif action == "保留":
                protected_records.append(record)
            else:
                failed_records.append(record)
        manifest["删除结果"] = _dedupe_records(deleted_records)
        manifest["保护项"] = _dedupe_records(protected_records)
        manifest["失败项"] = _dedupe_records(failed_records)
        manifest["完成路径"] = sorted(completed)
        manifest["批量处理结果"] = _dedupe_records([*manifest.get("批量处理结果", []), *results])

    def _finalize_manifest(
        self,
        manifest: dict[str, Any],
        touched_paths: list[str],
        checkpoint_rows: list[dict[str, Any]],
        references: list[dict[str, Any]],
        fingerprint: str,
        batch_results: list[dict[str, Any]],
    ) -> None:
        """整组落库：路径组、受保护大小、整组进度、批量结果与版本标识一次写完。"""
        _normalize_manifest(manifest)
        paths = _manifest_paths(manifest)
        for candidate in touched_paths:
            if candidate and candidate not in paths:
                paths.append(candidate)
        manifest["路径组"] = paths
        manifest["存储路径"] = manifest.get("存储路径") or (paths[0] if paths else "")
        relevant_refs = [item for item in references
                         if any(_path_overlaps(item["路径"], state_path) for state_path in paths)]
        manifest["引用集合"] = sorted(set(manifest.get("引用集合", [])) | {item["路径"] for item in relevant_refs})
        manifest["引用快照"] = sorted(set(manifest.get("引用快照", [])) | {item["检查点"] for item in relevant_refs})
        manifest["处理记录"] = sorted(set(manifest.get("处理记录", [])) | {
            str(_record_id(row, "记录编号")) for row in checkpoint_rows
            if any(_path_overlaps(_normalize_path(row.get("保存路径")), state_path) for state_path in paths)
        })
        for item in relevant_refs:
            manifest["关联清单"][item["检查点"]] = manifest.get("清单编号")
        protected_records = [
            item for item in manifest.get("保护项", [])
            if not item.get("路径") or any(_path_overlaps(item.get("路径"), state_path) for state_path in paths)
        ]
        protected_bytes = _protected_bytes(protected_records)
        manifest["受保护字节"] = protected_bytes
        manifest["受保护大小"] = format_size(protected_bytes)
        owned_results = [
            item for item in batch_results
            if any(_path_overlaps(item.get("路径"), state_path) for state_path in paths)
        ]
        batch = {
            "请求指纹": fingerprint,
            "提交编号": fingerprint[:12],
            "处理结果": owned_results,
            "时间": _now(),
        }
        signature = sorted((str(item.get("路径")), str(item.get("动作"))) for item in owned_results)
        if not any(
            item.get("请求指纹") == fingerprint
            and sorted((str(result.get("路径")), str(result.get("动作"))) for result in item.get("处理结果", []))
            == signature
            for item in manifest.get("批次记录", [])
        ):
            manifest["批次记录"].append(batch)
        manifest["整组进度"] = self._group_progress(paths, manifest)
        manifest["状态"] = self._manifest_status(manifest)
        manifest["版本标识"] = int(manifest.get("版本标识", 1)) + 1
        manifest["更新时间"] = _now()
        if not manifest.get("请求指纹"):
            manifest["请求指纹"] = fingerprint
        # 检查点关联结果：既能恢复，又必须能追溯到唯一一份清单。
        for row in checkpoint_rows:
            row_path = _normalize_path(row.get("保存路径"))
            if not any(_path_overlaps(row_path, state_path) for state_path in paths):
                continue
            checkpoint_id = _record_id(row, "记录编号")
            row["关联清单编号"] = manifest.get("清单编号")
            if any(item["检查点"] == checkpoint_id for item in relevant_refs):
                row["可恢复"] = True
                row["清理结果"] = "保留"
                row["abnormal"] = False
            elif _checkpoint_status(row) == EXPIRED_STATUS and checkpoint_id not in _restore_todo_ids():
                row["可恢复"] = False
                row["清理结果"] = "已删除"
            else:
                row["清理结果"] = "保留"
                row["可恢复"] = True

    def _group_progress(self, paths: list[str], manifest: dict[str, Any]) -> dict[str, Any]:
        """整组进度按路径统计：删除/保留/跳过都算完成，失败与未处理的算未完成。"""
        own_paths = {path for path in paths if path}
        deleted_paths = {_normalize_path(item.get("路径")) for item in manifest.get("删除结果", [])}
        protected_paths = {_normalize_path(item.get("路径")) for item in manifest.get("保护项", [])}
        failed_paths = {_normalize_path(item.get("路径")) for item in manifest.get("失败项", [])}
        completed_paths = {_normalize_path(path) for path in manifest.get("完成路径", [])}
        deleted = len(deleted_paths & own_paths)
        protected = len((protected_paths - deleted_paths) & own_paths)
        failed = len((failed_paths - deleted_paths - protected_paths) & own_paths)
        total = max(len(own_paths), deleted + protected + failed)
        done = len((completed_paths | deleted_paths | protected_paths | failed_paths) & own_paths)
        unhandled = max(total - done, 0)
        return {
            "总数": total,
            "已删除": deleted,
            "已保留": protected,
            "失败": failed,
            "跳过": max(len(manifest.get("批量处理结果", [])) - deleted - protected - failed, 0),
            "未完成": failed + unhandled,
            "完成": failed == 0 and unhandled == 0,
        }

    def _manifest_status(self, manifest: dict[str, Any]) -> str:
        if manifest.get("失败项"):
            return "部分完成"
        progress = manifest.get("整组进度") or {}
        if progress.get("完成"):
            return "已完成"
        return "部分完成"

    def _batch_summary(self, manifests: list[dict[str, Any]], duplicate: bool = False) -> dict[str, Any]:
        manifests = [_normalize_manifest(manifest) for manifest in manifests]
        primary = manifests[0] if manifests else {}
        deleted = _dedupe_records([item for manifest in manifests for item in manifest.get("删除结果", [])])
        protected = _dedupe_records([item for manifest in manifests for item in manifest.get("保护项", [])])
        failed = _dedupe_records([item for manifest in manifests for item in manifest.get("失败项", [])])
        references = sorted({path for manifest in manifests for path in manifest.get("引用集合", [])})
        checkpoint_ids = sorted({cid for manifest in manifests for cid in manifest.get("引用快照", [])})
        protected_bytes = sum(int(manifest.get("受保护字节", 0)) for manifest in manifests)
        progress = {
            "总数": sum(int((manifest.get("整组进度") or {}).get("总数", 0)) for manifest in manifests),
            "已删除": sum(int((manifest.get("整组进度") or {}).get("已删除", 0)) for manifest in manifests),
            "已保留": sum(int((manifest.get("整组进度") or {}).get("已保留", 0)) for manifest in manifests),
            "失败": sum(int((manifest.get("整组进度") or {}).get("失败", 0)) for manifest in manifests),
        }
        progress["完成"] = bool(manifests) and all(
            bool((manifest.get("整组进度") or {}).get("完成")) for manifest in manifests
        )
        summary = {
            "清单编号": primary.get("清单编号", ""),
            "清单列表": manifests,
            "清单": primary,
            "manifests": manifests,
            "manifest": primary,
            "引用集合": references,
            "引用快照": checkpoint_ids,
            "删除结果": deleted,
            "保护项": protected,
            "失败项": failed,
            "受保护字节": protected_bytes,
            "受保护大小": format_size(protected_bytes),
            "整组进度": progress,
            "进度": progress,
            "批量处理结果": _dedupe_records([item for manifest in manifests for item in manifest.get("批量处理结果", [])]),
            "版本标识": primary.get("版本标识", 0),
            "重复提交": duplicate,
        }
        return summary

    # ------------------------------------------------------------------
    # 清单读取、失败续跑与消费侧版本
    # ------------------------------------------------------------------
    def list_manifests(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [_normalize_manifest(row) for row in store.rows(MANIFEST_MODULE)]
        if keyword:
            rows = [row for row in rows
                    if keyword in str(row.get("清单编号", ""))
                    or keyword in str(row.get("存储路径", ""))
                    or keyword in str(row.get("后端配置编号", ""))]
        if status:
            rows = [row for row in rows if row.get("状态") == status]
        rows.sort(key=_manifest_sort_key, reverse=True)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_manifest(self, manifest_id: int) -> dict[str, Any] | None:
        return _manifest_by_id(manifest_id)

    def retry_manifest(self, manifest_id: int, values: dict[str, Any] | None = None) -> dict[str, Any]:
        values = values or {}
        with store.transaction():
            manifest = _manifest_by_id(manifest_id)
            if manifest is None:
                return {"ok": False, "message": f"状态快照数据清单 {manifest_id} 不存在或已归档"}
            if not manifest.get("失败项"):
                return {"ok": True, "message": "清单没有失败项，无需续跑", "entry": manifest, "manifest": manifest}
            requested = _as_list(_first(values, ["失败路径", "paths", "fail_paths"], []))
            requested_paths = {_normalize_path(item) for item in requested if _normalize_path(item)}
            failure_requests = _failure_requests(values)
            paths = _manifest_paths(manifest)
            references = _collect_references()
            retried: list[dict[str, Any]] = []
            remaining: list[dict[str, Any]] = []
            for item in manifest.get("失败项", []):
                path = _normalize_path(item.get("路径"))
                if requested_paths and path not in requested_paths:
                    remaining.append(item)
                    continue
                if not path:
                    remaining.append(item)
                    continue
                if failure_requests.get(path):
                    # 仍然失败：保留原目录，失败项只更新原因。
                    reason = failure_requests[path]
                    remaining.append({**item, "原因": reason, "结果": FAILED_STATUS, "保留目录": True})
                    _upsert_ledger(path, manifest_id, FAILED_STATUS, reason)
                    retried.append({**item, "结果": reason, "动作": "失败", "保留目录": True})
                    continue
                issue = _consistency_issue(path, references)
                if issue:
                    remaining.append({**item, "原因": issue, "结果": issue, "保留目录": True})
                    _upsert_ledger(path, manifest_id, FAILED_STATUS, issue)
                    retried.append({**item, "结果": issue, "动作": "失败", "保留目录": True})
                    continue
                relevant_refs = _references_for_path(path, references)
                if relevant_refs:
                    # 失败期间新增了恢复引用：续跑只保留目录，不再删除。
                    checkpoint_ids = "、".join(sorted({ref["检查点"] for ref in relevant_refs}))
                    retried.append({
                        **item,
                        "检查点": checkpoint_ids,
                        "结果": "被恢复检查点引用，整组保留",
                        "动作": "保留",
                        "原因": "被恢复检查点引用，整组保留",
                        "保留目录": True,
                    })
                    _upsert_ledger(path, manifest_id, "保留", "被恢复检查点引用")
                    continue
                ledger = _find_ledger(path)
                if ledger is not None and ledger.get("结果") == "已删除":
                    retried.append({**item, "结果": "同一路径已清理", "动作": "跳过", "保留目录": False})
                    continue
                _upsert_ledger(path, manifest_id, "已删除", "")
                retried.append({
                    "路径": path,
                    "结果": "已删除",
                    "动作": "删除",
                    "原因": "",
                    "字节": item.get("字节", 0),
                    "大小": item.get("大小", "0B"),
                    "保留目录": False,
                })
                # 只从失败项里移除真正完成的路径，状态大小同步归零。
                for row in store.rows(MODULE):
                    if _normalize_path(row.get("存储路径")) == path:
                        normalize_state_row(row)
                        row["状态大小"] = "0B"
                        row["状态大小字节"] = 0
                        row["快照状态"] = "已清理"
                        row["清理清单编号"] = manifest.get("清单编号")
                        row["abnormal"] = False
                for row in store.rows(CHECKPOINT_MODULE):
                    normalize_checkpoint_row(row)
                    if _path_overlaps(_normalize_path(row.get("保存路径")), path):
                        row["清理结果"] = "已删除"
                        row["可恢复"] = False
            self._apply_results(manifest, manifest.get("存储路径") or (paths[0] if paths else ""),
                                [], [], references, retried)
            manifest["失败项"] = remaining
            manifest["受保护字节"] = _protected_bytes(manifest.get("保护项", []))
            manifest["受保护大小"] = format_size(manifest["受保护字节"])
            success_paths = {item["路径"] for item in retried if item.get("结果") == "已删除"}
            manifest["完成路径"] = sorted(set(manifest.get("完成路径", [])) | {
                _normalize_path(item.get("路径")) for item in retried
                if item.get("动作") in {"删除", "失败", "保留", "跳过"}
            })
            manifest["整组进度"] = self._group_progress(paths, manifest)
            manifest["状态"] = self._manifest_status(manifest)
            manifest["版本标识"] = int(manifest.get("版本标识", 1)) + 1
            manifest["更新时间"] = _now()
            return {
                "ok": True,
                "message": f"已从未完成路径继续清理：完成 {len(success_paths)} 项，仍失败 {len(remaining)} 项",
                "entry": manifest,
                "manifest": manifest,
                "删除结果": manifest["删除结果"],
                "失败项": manifest["失败项"],
                "受保护大小": manifest["受保护大小"],
                "批量处理结果": retried,
            }

    def list_consumers(self) -> list[dict[str, Any]]:
        current_version = max(
            (int(_normalize_manifest(row).get("版本标识", 0)) for row in store.rows(MANIFEST_MODULE)),
            default=0,
        )
        rows = store.rows(CONSUMER_MODULE)
        if not rows:
            rows.append({"id": 1, "链路": "checkpoint-restore", "已同步版本": 0,
                         "当前清单版本": current_version, "待同步": current_version})
        for row in rows:
            row["当前清单版本"] = current_version
            synced = int(row.get("已同步版本", 0) or 0)
            row["待同步"] = max(current_version - synced, 0)
            row.setdefault("版本标识", synced)
        return rows

    def sync_manifest(self, manifest_id: int, values: dict[str, Any] | None = None) -> dict[str, Any]:
        values = values or {}
        with store.transaction():
            manifest = _manifest_by_id(manifest_id)
            if manifest is None:
                return {"ok": False, "message": f"状态快照数据清单 {manifest_id} 不存在或已归档"}
            consumers = self.list_consumers()
            chain = str(_first(values, ["链路", "link", "consumer"], "") or "").strip()
            version = int(manifest.get("版本标识", 0))
            synced_rows: list[dict[str, Any]] = []
            for row in consumers:
                if chain and str(row.get("链路")) != chain:
                    continue
                row["已同步版本"] = version
                row["当前清单版本"] = version
                row["待同步"] = 0
                row["版本标识"] = version
                row["同步清单编号"] = manifest.get("清单编号")
                row["同步时间"] = _now()
                synced_rows.append(row)
            if not synced_rows:
                row = {
                    "id": max((int(item.get("id", 0)) for item in consumers), default=0) + 1,
                    "链路": chain or "checkpoint-restore",
                    "已同步版本": version,
                    "当前清单版本": version,
                    "待同步": 0,
                    "版本标识": version,
                    "同步清单编号": manifest.get("清单编号"),
                    "同步时间": _now(),
                }
                store.rows(CONSUMER_MODULE).append(row)
                synced_rows.append(row)
            return {
                "ok": True,
                "message": f"状态快照数据清单 {manifest.get('清单编号')} 已同步到消费侧，版本 {version}",
                "entry": manifest,
                "manifest": manifest,
                "消费侧": synced_rows,
                "版本标识": version,
            }
