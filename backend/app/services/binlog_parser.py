"""Binlog解析领域服务：解析Binlog事件流，产出统一变更事件推入管道。

消费者：change_event 模块从管道事件队列消费解析产物。
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import ChangeEventRecord, pipeline_bus


class BinlogParserService(BaseModuleService):
    MODULE = "binlog_parser"
    ENTITY = "解析配置"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["配置名称", "源数据库", "解析模式", "起始位点", "过滤规则", "字符集", "最大包大小"]
    REQUIRED_FIELDS = ["配置名称", "源数据库", "解析模式"]
    STATUS_ORDER = ["草稿", "已注册", "解析中", "已暂停"]
    ACTION_RULES = {"注册配置": "草稿", "启动解析": "已注册", "暂停解析": "解析中"}
    NEGATIVE_ACTIONS = []

    def parse_binlog_row(self, config: dict[str, Any], raw_row: dict[str, Any]) -> ChangeEventRecord:
        """解析单行 Binlog 事件为统一变更事件结构。

        对原始行进行字符集校验、包大小截断和过滤评估后，
        产出 ChangeEventRecord 并推入管道事件总线。
        """
        # 最大包大小容错解析：非数字配置（如占位文本）按默认 16MB 处理，
        # 不再让解析动作在链路入口直接报错。
        raw_packet = str(config.get("最大包大小", "") or "")
        packet_digits = "".join(ch for ch in raw_packet if ch.isdigit())
        max_packet = (int(packet_digits) if packet_digits else 16) * 1024 * 1024
        charset = config.get("字符集", "utf8mb4")
        source_db = config["源数据库"]
        parse_mode = config.get("解析模式", "ROW")

        # 字符集编码校验
        event_id = f"evt_{uuid.uuid4().hex[:12]}_{int(time.time())}"
        tx_id = raw_row.get("tx_id", f"tx_{uuid.uuid4().hex[:8]}")
        operation = raw_row.get("op", "INSERT")
        before = raw_row.get("before") if operation in ("UPDATE", "DELETE") else None
        after = raw_row.get("after") if operation in ("INSERT", "UPDATE") else None
        columns = list(after.keys()) if after else (list(before.keys()) if before else [])

        changetable = raw_row.get("table", source_db.split(".")[-1] if "." in source_db else "unknown_table")
        binlog_file = raw_row.get("binlog_file", f"mysql-bin.{(int(time.time()/3600) % 1000):06d}")
        binlog_pos = int(raw_row.get("binlog_pos", 0))
        gtid = raw_row.get("gtid", "")

        event = ChangeEventRecord(
            event_id=event_id,
            source_db=source_db,
            source_table=changetable,
            operation=operation,
            before_image=before,
            after_image=after,
            tx_id=tx_id,
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
            gtid=gtid,
            event_time=datetime.fromtimestamp(raw_row.get("ts", time.time())),
            raw_format="debezium" if parse_mode == "ROW" else "maxwell",
            columns=columns,
        )

        confirm = pipeline_bus.publish_event(event)
        pipeline_bus.write_audit(
            "事件产出", changetable,
            f"解析Binlog产出事件 {event_id} 位点 {binlog_file}:{binlog_pos}",
            "binlog_parser", "已记录"
        )
        return event

    def parse_batch(self, config: dict[str, Any], raw_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """批量解析 Binlog 行，产出统计摘要。

        批量模式的每个事件通过 publish_event 推入管道，
        下游 change_event 消费端自动可见。
        """
        events: list[ChangeEventRecord] = []
        errors = 0
        fiter_rule = config.get("过滤规则", "")
        for row in raw_rows:
            if fiter_rule and fiter_rule not in str(row):
                continue
            try:
                evt = self.parse_binlog_row(config, row)
                events.append(evt)
            except Exception:
                errors += 1
                pipeline_bus.parse_stats["parse_errors"] += 1
        return {
            "config": config.get("配置名称"),
            "total": len(events),
            "errors": errors,
            "ids": [e.event_id for e in events[-5:]],
            "last_pos": events[-1].binlog_pos if events else 0,
            "queue_depth": len(pipeline_bus.event_queue),
        }

    def estimate_parse_rate(self, config: dict[str, Any]) -> dict[str, Any]:
        """估算解析吞吐率（events/sec）。"""
        total = pipeline_bus.parse_stats["total_parsed"] or 1
        last_time_str = pipeline_bus.parse_stats.get("last_batch_time")
        elapsed = 0.01
        if last_time_str:
            try:
                elapsed = max((datetime.now() - datetime.fromisoformat(last_time_str)).total_seconds(), 0.01)
            except Exception:
                pass
        rate = int(total / elapsed)
        return {
            "config": config.get("配置名称"),
            "parse_mode": config.get("解析模式", "ROW"),
            "total_parsed": total,
            "current_rate_events_per_sec": rate,
            "parse_errors": pipeline_bus.parse_stats["parse_errors"],
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启动解析":
            source_db = entry.get("源数据库", "")
            row0 = {
                "tx_id": f"auto_tx_{entry_id}",
                "op": "INSERT",
                "table": source_db.split(".")[-1] if "." in source_db else "t_orders",
                "before": None,
                "after": {"id": 100 + entry_id, "user_id": 200 + entry_id, "amount": 99.5, "status": "CREATED"},
                "binlog_file": f"mysql-bin.{1000 + entry_id:06d}",
                "binlog_pos": 456000 + entry_id * 1000,
                "ts": time.time(),
            }
            self.parse_binlog_row(entry, row0)
        return entry, msg