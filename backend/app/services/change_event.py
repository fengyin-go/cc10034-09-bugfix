"""变更事件领域服务：消费管道中的 ChangeEventRecord，执行事件解析、差异计算与字段增量交付。

上游：binlog_parser 产出事件推入管道 → change_event 消费解析
下游：format_adapter 格式适配 → event_filter 过滤 → route_dispatcher 路由分发
     scd_handler 消费本模块交付的完整字段增量数据（pipeline_bus.increment_log）

历史缺陷：消费出口曾把变更前后镜像 JSON 截断到 200 字（LEGACY_IMAGE_CUT），
长字段事件的镜像变成非法 JSON，差异解析报错、字段增量整体丢失。
修复后消费链路不再截断，差异计算恢复，完整字段增量随事件交付；
存量截断/破损/非JSON镜像按状态容错解析，不再整体失败。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus
from app.store import store

# 历史截断点：旧版消费出口把镜像截断到 200 字。新链路不再使用，
# 该常量仅用于在变更追踪清单中定位存量的截断数据。
LEGACY_IMAGE_CUT = 200


class ChangeEventService(BaseModuleService):
    MODULE = "change_event"
    ENTITY = "变更记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["事件ID", "来源表", "操作类型", "变更前镜像", "变更后镜像", "事务ID", "事件时间"]
    REQUIRED_FIELDS = ["事件ID", "来源表", "操作类型"]
    STATUS_ORDER = ["已捕获", "已解析", "已下发", "解析失败"]
    ACTION_RULES = {"解析事件": "已捕获", "下发事件": "已解析", "标记失败": "已解析"}
    NEGATIVE_ACTIONS = []

    def consume_pipeline_events(self, batch_size: int = 50) -> list[dict[str, Any]]:
        """从管道总线消费变更事件，写入变更追踪清单并交付完整字段增量。

        消费 pipeline_bus 中 binlog_parser 产出的缓冲区事件：
        镜像完整写入（不截断），随后恢复字段差异计算，
        把完整字段增量数据交付到 pipeline_bus.increment_log 供 scd_handler 消费。
        """
        raw_events = pipeline_bus.consume_events(batch_size=batch_size)
        if not raw_events:
            return []
        rows = store.rows(self.MODULE)
        entries: list[dict[str, Any]] = []
        for evt in raw_events:
            before_str = (
                json.dumps(evt.before_image, ensure_ascii=False)
                if evt.before_image is not None else ""
            )
            after_str = (
                json.dumps(evt.after_image, ensure_ascii=False)
                if evt.after_image is not None else ""
            )
            entry = self.find_by_event_id(evt.event_id)
            if entry is None:
                entry = {
                    "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                    "status": "已解析",
                    "pending": True,
                    "abnormal": False,
                }
                rows.append(entry)
            entry.update({
                "事件ID": evt.event_id,
                "来源表": f"{evt.source_db}.{evt.source_table}",
                "操作类型": evt.operation,
                "变更前镜像": before_str,  # 完整镜像：修复后不再截断
                "变更后镜像": after_str,
                "事务ID": evt.tx_id,
                "事件时间": evt.event_time.isoformat(),
                "raw_format": evt.raw_format,
                "binlog_file": evt.binlog_file,
                "binlog_pos": evt.binlog_pos,
                "gtid": evt.gtid,
            })
            # 恢复字段差异计算并交付完整字段增量数据
            increment = self.compute_increment(entry)
            pipeline_bus.deliver_increment(increment)
            entry["字段增量"] = increment
            entry["_chain"] = [
                {"stage": "binlog_parser.publish", "status": "ok",
                 "before_len": len(before_str), "after_len": len(after_str)},
                {"stage": "change_event.consume", "status": "ok",
                 "before_len": len(before_str), "after_len": len(after_str)},
                {"stage": "delta_computation",
                 "status": "ok" if increment["complete"] else "degraded",
                 "delta_count": increment["delta_count"]},
            ]
            entries.append(entry)
        pipeline_bus.write_audit(
            "事件消费", f"{len(entries)}条事件",
            f"消费管道事件 {len(entries)} 条，完整镜像与字段增量已交付",
            "change_event", "已解析"
        )
        return entries

    def classify_by_operation(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """对消费后的事件按操作类型分类统计。

        为下游 format_adapter 提供操作类型分布以决定格式适配策略。
        """
        ops: dict[str, int] = {"insert": 0, "update": 0, "delete": 0}
        tables: dict[str, int] = {}
        total_rows_affected = 0
        for evt in events:
            op = evt.get("操作类型", "").upper()
            if op == "INSERT":
                ops["insert"] += 1
            elif op == "UPDATE":
                ops["update"] += 1
            elif op == "DELETE":
                ops["delete"] += 1
            tbl = evt.get("来源表", "unknown")
            tables[tbl] = tables.get(tbl, 0) + 1
            total_rows_affected += 1
        return {
            "total_events": len(events),
            "by_operation": ops,
            "by_table": tables,
            "insert_ratio": round(ops["insert"] / max(len(events), 1) * 100, 1),
            "update_ratio": round(ops["update"] / max(len(events), 1) * 100, 1),
            "delete_ratio": round(ops["delete"] / max(len(events), 1) * 100, 1),
        }

    # ---------- 镜像解析：兼容已有镜像（截断/破损/非JSON 不再整体失败） ----------

    @staticmethod
    def _parse_image(raw: Any) -> tuple[dict[str, Any] | None, str, str | None]:
        """解析镜像，返回 (镜像dict, 状态, 错误说明)。

        状态：ok 完整 / empty 空镜像 / truncated 历史200字截断 /
        corrupt 破损JSON / legacy_text 历史非JSON文本。
        """
        if raw is None:
            return None, "empty", None
        if isinstance(raw, dict):
            return (dict(raw), "ok", None) if raw else (None, "empty", None)
        if not isinstance(raw, str):
            return None, "corrupt", f"镜像类型不支持: {type(raw).__name__}"
        text = raw.strip()
        if not text:
            return None, "empty", None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            if text.startswith(("{", "[")):
                if len(text) == LEGACY_IMAGE_CUT:
                    return None, "truncated", (
                        f"镜像在 {LEGACY_IMAGE_CUT} 字处被历史消费链路截断: {exc}"
                    )
                return None, "corrupt", f"镜像 JSON 破损: {exc}"
            return None, "legacy_text", "历史非 JSON 文本镜像，按原样保留"
        if isinstance(parsed, dict):
            return (parsed, "ok", None) if parsed else (None, "empty", None)
        return None, "corrupt", "镜像 JSON 不是对象"

    def _diff_images(
        self,
        before: dict[str, Any] | None,
        before_status: str,
        after: dict[str, Any] | None,
        after_status: str,
        operation: str,
    ) -> list[dict[str, Any]]:
        """逐字段差异：区分 缺失(missing) 与 显式NULL(null)，并标注可核验性。

        旧值是否可核验（verifiable）决定该字段能否在维度回放中被确认：
        旧前镜像为空/破损/截断/缺字段时，旧值不可核验，按缺失登记并说明原因。
        """
        before = before or {}
        after = after or {}
        fields: list[dict[str, Any]] = []
        op = (operation or "UPDATE").upper()
        for key, new_val in after.items():
            new_state = "null" if new_val is None else "value"
            if key in before:
                old_val = before[key]
                old_state = "null" if old_val is None else "value"
                if old_val == new_val:
                    continue
                verifiable, reason = True, ""
            else:
                old_val = None
                if op == "INSERT":
                    old_state, verifiable, reason = "missing", True, ""
                elif before_status == "ok":
                    old_state = "missing"
                    verifiable = False
                    reason = "旧前镜像缺少该字段旧值，无法核验（按缺失登记，非显式 NULL）"
                elif before_status == "empty":
                    old_state, verifiable = "unknown", False
                    reason = "旧前镜像为空，无法核验旧值（按缺失登记，非显式 NULL）"
                elif before_status == "truncated":
                    old_state, verifiable = "unknown", False
                    reason = f"旧前镜像被截断（{LEGACY_IMAGE_CUT}字），无法核验旧值"
                elif before_status == "corrupt":
                    old_state, verifiable = "unknown", False
                    reason = "旧前镜像已破损，无法核验旧值"
                else:
                    old_state, verifiable = "unknown", False
                    reason = "旧前镜像不可解析，无法核验旧值"
            fields.append({
                "field": key, "old": old_val, "new": new_val,
                "old_state": old_state, "new_state": new_state,
                "verifiable": verifiable, "reason": reason,
            })
        for key, old_val in before.items():
            if key in after:
                continue
            old_state = "null" if old_val is None else "value"
            new_state = "missing" if after_status == "ok" else "unknown"
            fields.append({
                "field": key, "old": old_val, "new": None,
                "old_state": old_state, "new_state": new_state,
                "verifiable": True, "reason": "",
            })
        return fields

    def compute_increment(self, event: dict[str, Any]) -> dict[str, Any]:
        """计算完整字段增量数据：长字段不截断，缺失与显式 NULL 分开记录。

        产出的增量结构随事件交付（pipeline_bus.increment_log），
        是 scd_handler 消费与维度回放的唯一增量来源。
        """
        before, before_status, before_err = self._parse_image(event.get("变更前镜像"))
        after, after_status, after_err = self._parse_image(event.get("变更后镜像"))
        operation = str(event.get("操作类型", "UPDATE")).upper()
        fields = self._diff_images(before, before_status, after, after_status, operation)
        source_table = str(event.get("来源表", ""))
        return {
            "event_id": event.get("事件ID", ""),
            "source_db": source_table.split(".")[0] if "." in source_table else "",
            "source_table": source_table.split(".")[-1] if "." in source_table else source_table,
            "operation": operation,
            "tx_id": event.get("事务ID", ""),
            "binlog_file": event.get("binlog_file", ""),
            "binlog_pos": event.get("binlog_pos", 0),
            "gtid": event.get("gtid", ""),
            "image_status": {"before": before_status, "after": after_status},
            "image_errors": {"before": before_err, "after": after_err},
            "before": before,
            "after": after,
            "fields": fields,
            "delta_count": len(fields),
            "complete": (
                before_status in ("ok", "empty")
                and after_status in ("ok", "empty")
                and all(f["verifiable"] for f in fields)
            ),
            "delivered_at": datetime.now().isoformat(),
        }

    def delta_computation(self, event: dict[str, Any]) -> dict[str, Any]:
        """对变更前后镜像执行差异计算（已恢复）。

        计算 UPDATE 操作中哪些字段实际发生了变化，
        产出变更增量列表供下游 SCD 策略消费。
        兼容已有镜像：截断/破损/非JSON 镜像按状态上报，不再整体报错返空。
        """
        increment = self.compute_increment(event)
        return {
            "delta_fields": [
                {
                    "field": f["field"], "old": f["old"], "new": f["new"],
                    "old_state": f["old_state"], "new_state": f["new_state"],
                }
                for f in increment["fields"]
            ],
            "delta_count": increment["delta_count"],
            "image_status": increment["image_status"],
            "complete": increment["complete"],
            "parse_error": increment["image_errors"]["before"] or increment["image_errors"]["after"],
        }

    # ---------- 链路诊断与坐标定位 ----------

    def locate_truncation(self, entry_id: int) -> dict[str, Any]:
        """沿着事件消费链路定位镜像截断点。

        链路：binlog_parser 产出 → 管道队列 → change_event 消费出口 → 差异解析。
        新链路各阶段镜像长度一致即无截断；存量数据按 200 字截断特征识别，
        截断点指向 change_event.consume_pipeline_events 出口序列化。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"found": False, "entry_id": entry_id, "truncation_point": None}
        stages: list[dict[str, Any]] = []
        for link in entry.get("_chain") or []:
            stages.append({
                "stage": link.get("stage"), "status": link.get("status"),
                "before_len": link.get("before_len"), "after_len": link.get("after_len"),
            })
        truncation_point = None
        for field in ("变更前镜像", "变更后镜像"):
            raw = entry.get(field)
            _, status, err = self._parse_image(raw)
            stages.append({
                "stage": f"consume_store.{field}", "status": status,
                "length": len(raw) if isinstance(raw, str) else 0,
                "detail": err,
            })
            if status == "truncated" and truncation_point is None:
                truncation_point = (
                    "change_event.consume_pipeline_events 出口序列化"
                    f"（镜像被截断到 {LEGACY_IMAGE_CUT} 字）"
                )
        delta = self.delta_computation(entry)
        stages.append({
            "stage": "delta_computation",
            "status": "ok" if delta["complete"] else "degraded",
            "delta_count": delta["delta_count"],
            "parse_error": delta["parse_error"],
        })
        return {
            "found": True,
            "entry_id": entry_id,
            "事件ID": entry.get("事件ID"),
            "stages": stages,
            "truncation_point": truncation_point,
            "chain_healthy": truncation_point is None,
        }

    def find_by_coordinates(
        self,
        source_table: str | None = None,
        tx_id: str | None = None,
        binlog_pos: int | str | None = None,
    ) -> list[dict[str, Any]]:
        """按源表、事务号和 binlog_pos 在变更追踪清单中定位事件。"""
        matches: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if tx_id is not None and str(row.get("事务ID", "")) != str(tx_id):
                continue
            if binlog_pos is not None and str(row.get("binlog_pos", "")) != str(binlog_pos):
                continue
            if source_table:
                table = str(row.get("来源表", ""))
                if table != source_table and not table.endswith(f".{source_table}"):
                    continue
            matches.append(row)
        return matches

    def find_by_event_id(self, event_id: str) -> dict[str, Any] | None:
        """按事件ID在变更追踪清单中定位记录。"""
        for row in store.rows(self.MODULE):
            if row.get("事件ID") == event_id:
                return row
        return None

    def write_back_application(self, event_id: str, result: dict[str, Any]) -> dict[str, Any] | None:
        """把 SCD 应用结果回写到变更追踪清单。"""
        entry = self.find_by_event_id(event_id)
        if entry is None:
            return None
        entry["应用结果"] = result.get("应用结果", "")
        entry["应用维度键"] = result.get("dim_key", "")
        entry["应用时间"] = datetime.now().isoformat()
        entry["已确认字段"] = result.get("confirmed_fields", [])
        entry["待补字段"] = result.get("pending_fields", [])
        entry["应用坐标"] = result.get("coordinate", {})
        return entry

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "下发事件":
            pipeline_bus.write_audit(
                "事件下发", entry.get("来源表", ""),
                f"事件 {entry_id} 下发给下游消费者",
                "change_event", "已下发"
            )
        return entry, msg
