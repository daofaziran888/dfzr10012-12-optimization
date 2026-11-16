"""状态后端管理接口：维护后端配置，覆盖启用后端、暂停后端、关闭后端等动作。

过期快照批量清理与状态快照数据清单也在这里暴露：批量清理返回清单的删除结果、
受保护大小与失败项；清单读取、失败续跑、消费侧版本同步各自一个入口。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.state import StateService

router = APIRouter(prefix="/api/state", tags=["状态后端管理"])

service = StateService()

LIST_FIELDS = ["配置编号", "后端类型", "存储路径", "状态大小", "过期策略", "清理间隔", "快照频率", "配置状态"]
STATUSES = ["草稿", "已启用", "已暂停", "已关闭"]
MANIFEST_STATUSES = ["已生成", "部分完成", "已完成"]


def _payload_values(payload: EntryPayload | None) -> dict[str, Any]:
    return dict(payload.values) if payload is not None else {}


def _action_result(result: dict[str, Any]) -> ActionResult:
    # 批量清理没有单一清单时，整份批量结果作为 entry 返回，保留整组进度与批量处理结果。
    entry = result.get("entry") or result.get("manifest") or result
    return ActionResult(
        ok=bool(result.get("ok", False)),
        message=str(result.get("message") or ""),
        entry=entry,
    )


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    status: str | None = Query(default=None, description="草稿、已启用、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按配置编号与状态过滤状态后端管理列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出状态后端管理清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "state", "total": total, "items": items}


@router.get("/manifests", response_model=PageResult[dict])
def list_manifests(
    keyword: str | None = Query(default=None, description="按清单编号、存储路径或配置编号检索"),
    status: str | None = Query(default=None, description="已生成、部分完成、已完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """状态快照数据清单列表：删除结果、受保护大小与失败项都随清单返回。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_manifests(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/manifests/consumers", response_model=list[dict])
def list_consumers() -> list[dict]:
    """消费侧维护的版本标识：跨数据链路同步保留同一份状态快照数据清单。"""
    return service.list_consumers()


@router.post("/manifests/cleanup", response_model=ActionResult)
def cleanup_snapshots(payload: EntryPayload | None = None) -> ActionResult:
    """批量清理过期快照：先校验 STAT 快照与 CHEC 引用，再按整组删除或保留。

    提交字段兼容 state_ids / 状态快照 / 状态快照编号 / entries 与
    检查点 / checkpoint_ids；重复提交返回既有清单，不重复删除。
    """
    result = service.cleanup_snapshots(_payload_values(payload))
    if result.get("ok"):
        return _action_result(result)
    return ActionResult(ok=False, message=str(result.get("message") or "快照清理未完成"))


@router.get("/manifests/{manifest_id}", response_model=dict)
def get_manifest(manifest_id: int) -> dict:
    """读取单条状态快照数据清单；不存在时给出可读的错误说明。"""
    manifest = service.get_manifest(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"状态快照数据清单 {manifest_id} 不存在或已归档")
    return manifest


@router.post("/manifests/{manifest_id}/retry", response_model=ActionResult)
def retry_manifest(manifest_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """失败续跑：只从未完成路径继续，失败项继续保留原目录。"""
    result = service.retry_manifest(manifest_id, _payload_values(payload))
    return _action_result(result)


@router.post("/manifests/{manifest_id}/sync", response_model=ActionResult)
def sync_manifest(manifest_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """把同一份状态快照数据清单同步到消费侧，并更新消费侧版本标识。"""
    result = service.sync_manifest(manifest_id, _payload_values(payload))
    return _action_result(result)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条后端配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"后端配置 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条后端配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="后端配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条后端配置执行启用后端、暂停后端、关闭后端或清理快照；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
