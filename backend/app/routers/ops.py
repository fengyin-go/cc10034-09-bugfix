"""CDC 管道运营接口：事件消费、字段增量交付、截断定位、SCD 应用与回放。

这些端点承载跨模块管道链路（binlog_parser → change_event → scd_handler），
与通用资源路由（/api/{module}）并列，注册顺序在其之前。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.pipeline import pipeline_bus
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api/ops", tags=["CDC管道运营"])


def _service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _resolve_config(config_id: int | None, config: dict[str, Any] | None) -> dict[str, Any]:
    if config:
        return config
    if config_id is not None:
        row = store.find("scd_handler", config_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"SCD配置 {config_id} 不存在或已归档")
        return row
    raise HTTPException(status_code=422, detail="需要提供 config_id 或 config")


class ConsumePayload(BaseModel):
    batch_size: int = 50


@router.post("/consume")
def consume_events(payload: ConsumePayload) -> dict[str, Any]:
    """消费管道事件：完整镜像写入变更追踪清单，并交付完整字段增量数据。"""
    entries = _service("change_event").consume_pipeline_events(batch_size=payload.batch_size)
    return {
        "ok": True,
        "consumed": len(entries),
        "event_ids": [e.get("事件ID") for e in entries],
        "queue_depth": len(pipeline_bus.event_queue),
    }


@router.get("/increments")
def list_increments() -> dict[str, Any]:
    """已交付的完整字段增量数据清单。"""
    return {"items": pipeline_bus.increment_log, "total": len(pipeline_bus.increment_log)}


@router.get("/increments/{event_id}")
def get_increment(event_id: str) -> dict[str, Any]:
    increment = pipeline_bus.increments_for(event_id)
    if increment is None:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 的字段增量不存在")
    return increment


@router.get("/truncation/{entry_id}")
def locate_truncation(entry_id: int) -> dict[str, Any]:
    """沿着事件消费链路定位镜像截断点。"""
    report = _service("change_event").locate_truncation(entry_id)
    if not report.get("found"):
        raise HTTPException(status_code=404, detail=f"变更记录 {entry_id} 不存在或已归档")
    return report


class ScdApplyPayload(BaseModel):
    event_id: str
    config_id: int | None = None
    config: dict[str, Any] | None = None


@router.post("/scd/apply")
def scd_apply(payload: ScdApplyPayload) -> dict[str, Any]:
    """让缓慢变化维消费指定事件的完整字段增量数据。"""
    config = _resolve_config(payload.config_id, payload.config)
    increment = pipeline_bus.increments_for(payload.event_id)
    if increment is None:
        entry = _service("change_event").find_by_event_id(payload.event_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"事件 {payload.event_id} 不存在")
        increment = _service("change_event").compute_increment(entry)
    return _service("scd_handler").apply_increment(config, increment)


class ScdReplayPayload(BaseModel):
    coordinates: list[dict[str, Any]] = Field(default_factory=list)
    config_id: int | None = None
    config: dict[str, Any] | None = None


@router.post("/scd/replay")
def scd_replay(payload: ScdReplayPayload) -> dict[str, Any]:
    """按源表、事务号和 binlog_pos 回放历史事件到维度（幂等，可重试）。"""
    config = _resolve_config(payload.config_id, payload.config)
    return _service("scd_handler").replay_by_coordinates(config, payload.coordinates)


@router.get("/scd/type2")
def scd_type2_state(dim_key: str | None = None) -> dict[str, Any]:
    """Type2 状态核对：当前版本、待补字段、实际应用的事件坐标。"""
    return _service("scd_handler").type2_state(dim_key)
