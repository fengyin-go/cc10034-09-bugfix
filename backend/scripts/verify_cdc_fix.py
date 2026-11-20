"""CDC 镜像截断修复与维度回放核对脚本。

场景（对应缺陷单）：
1. 消费一条带 225 字描述的完整订单 UPDATE（amount 7→9），验证镜像不再从
   第 200 字截断、差异计算恢复、完整字段增量交付；
2. 缓慢变化维消费完整字段增量，应用结果回写变更追踪清单；
3. 回放同一维度键的两笔旧更新（旧前镜像均有缺口、提交位置跨文件无法排序）：
   amount 7→9 可独立核验予以确认，description 缺旧值登记待补（非显式 NULL）；
4. 模拟回放失败后重试、重复提交：只落库已确认差异，不再次封存有效维度版本；
5. 显式 NULL 与缺失分开记录；存量截断/非JSON镜像兼容；
6. 核对 Type2 当前 amount、待补 description、实际应用的事件坐标。

运行：.venv/bin/python scripts/verify_cdc_fix.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.pipeline import pipeline_bus
from app.services import service_for
from app.store import store

LONG_DESC = "订单描述" * 56 + "订"  # 225 字长描述
assert len(LONG_DESC) == 225

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(cond), detail))
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + (f"  | {detail}" if detail else ""))


def reset_state() -> None:
    pipeline_bus.event_queue.clear()
    pipeline_bus.increment_log.clear()
    pipeline_bus.scd_type2_store.clear()
    pipeline_bus.scd_type2_applied.clear()
    pipeline_bus.pending_backfill.clear()
    rows = store.rows("change_event")
    rows[:] = [r for r in rows if not str(r.get("事件ID", "")).startswith("evt_")]


def main() -> int:
    reset_state()
    parser = service_for("binlog_parser")
    ce = service_for("change_event")
    scd = service_for("scd_handler")

    parser_cfg = {"配置名称": "orders_cdc", "源数据库": "shop_db", "解析模式": "ROW"}
    scd_cfg = {
        "配置名称": "dim_orders_type2", "维度表": "dim_orders", "SCD类型": "Type2",
        "自然键": "id", "代理键": "sk_order", "生效时间列": "effective_from",
        "失效时间列": "effective_to",
    }

    print("== 1. 消费带 225 字描述的完整订单 UPDATE ==")
    parser.parse_binlog_row(parser_cfg, {
        "tx_id": "tx_new_1", "op": "UPDATE", "table": "orders",
        "before": {"id": 1001, "amount": 7, "description": "旧备注", "status": "PAID"},
        "after": {"id": 1001, "amount": 9, "description": LONG_DESC, "status": "PAID"},
        "binlog_file": "mysql-bin.000009", "binlog_pos": 8800, "ts": time.time(),
    })
    ce.consume_pipeline_events()
    entry_new = ce.find_by_coordinates("orders", "tx_new_1", 8800)[0]
    inc_new = pipeline_bus.increments_for(entry_new["事件ID"])

    after_raw = entry_new["变更后镜像"]
    check("完整镜像不截断（>200字且可解析）",
          len(after_raw) > 200 and json.loads(after_raw)["description"] == LONG_DESC,
          f"after_image={len(after_raw)}字")
    delta = ce.delta_computation(entry_new)
    amount_delta = next((d for d in delta["delta_fields"] if d["field"] == "amount"), None)
    desc_delta = next((d for d in delta["delta_fields"] if d["field"] == "description"), None)
    check("差异计算恢复：amount 7→9",
          amount_delta is not None and amount_delta["old"] == 7 and amount_delta["new"] == 9)
    check("差异计算恢复：description 完整 225 字",
          desc_delta is not None and desc_delta["new"] == LONG_DESC)
    check("完整字段增量已交付", inc_new is not None and inc_new["complete"] is True
          and inc_new["delta_count"] == 2)

    print("== 2. 缓慢变化维消费完整字段增量 ==")
    r1 = scd.apply_increment(scd_cfg, inc_new)
    cur = pipeline_bus.scd_type2_store["1001"][-1]
    check("Type2 应用长描述更新", r1["action"] == "append" and cur["description"] == LONG_DESC
          and cur["amount"] == 9, f"version={r1['version']}")
    check("应用结果回写变更追踪清单",
          entry_new.get("应用结果") == "已应用" and entry_new.get("应用维度键") == "1001"
          and entry_new.get("已确认字段") == ["amount", "description"])

    print("== 3. 回放两笔旧更新（旧前镜像有缺口、位置不可排序） ==")
    parser.parse_binlog_row(parser_cfg, {
        "tx_id": "tx_old_1", "op": "UPDATE", "table": "orders",
        "before": {"id": 1001, "amount": 7},          # 缺口：缺 description 等列
        "after": {"id": 1001, "amount": 9},
        "binlog_file": "mysql-bin.000007", "binlog_pos": 4520, "ts": time.time(),
    })
    parser.parse_binlog_row(parser_cfg, {
        "tx_id": "tx_old_2", "op": "UPDATE", "table": "orders",
        "before": {"id": 1001},                        # 缺口：缺 description 旧值
        "after": {"id": 1001, "description": "待核验的旧描述"},
        "binlog_file": "mysql-bin.000008", "binlog_pos": 180, "ts": time.time(),
    })
    ce.consume_pipeline_events()
    coord1 = {"source_table": "orders", "tx_id": "tx_old_1", "binlog_pos": 4520}
    coord2 = {"source_table": "orders", "tx_id": "tx_old_2", "binlog_pos": 180}

    # 3a. 首次回放带一个错误坐标 → 部分失败，可重试
    bad = dict(coord1, binlog_pos=9999)
    replay1 = scd.replay_by_coordinates(scd_cfg, [bad, coord2])
    check("回放失败可定位（错误坐标）",
          len(replay1["failures"]) == 1 and replay1["retryable"] is True)
    pend1 = pipeline_bus.pending_backfill.get("1001", {})
    check("description 登记待补（缺失≠显式NULL）",
          pend1.get("description", {}).get("status") == "待补"
          and pend1["description"]["value_state"] == "missing"
          and "旧前镜像" in pend1["description"]["reason"],
          pend1.get("description", {}).get("reason", ""))

    # 3b. 重试：只落库已确认的差异（amount 7→9 可独立核验）
    replay2 = scd.replay_by_coordinates(scd_cfg, [coord1, coord2])
    grp = replay2["groups"][0]
    check("冲突识别：同维度键多笔旧事件且位置不可排序",
          grp["conflict"] is True and grp["positions_comparable"] is False)
    check("重试只落库已确认差异", grp["confirmed_fields"] == ["amount"]
          and grp["pending_fields"] == ["description"])
    check("amount 幂等确认不新增版本",
          len(pipeline_bus.scd_type2_store["1001"]) == 1
          and pipeline_bus.scd_type2_store["1001"][-1]["effective_to"] == "9999-12-31")

    # 3c. 重复提交：不再次封存有效维度版本
    replay3 = scd.replay_by_coordinates(scd_cfg, [coord1, coord2])
    actions = [e["action"] for e in replay3["groups"][0]["events"]]
    check("重复提交不再次封存有效版本",
          "duplicate" in actions and len(pipeline_bus.scd_type2_store["1001"]) == 1
          and pipeline_bus.scd_type2_store["1001"][-1]["effective_to"] == "9999-12-31")
    check("待补记录不重复登记",
          len(pipeline_bus.pending_backfill["1001"]) == 1)

    print("== 4. 显式 NULL 与缺失分开记录 ==")
    parser.parse_binlog_row(parser_cfg, {
        "tx_id": "tx_null_1", "op": "UPDATE", "table": "orders",
        "before": {"id": 2002, "coupon": "C10", "amount": 1},
        "after": {"id": 2002, "coupon": None, "amount": 1},
        "binlog_file": "mysql-bin.000009", "binlog_pos": 9010, "ts": time.time(),
    })
    ce.consume_pipeline_events()
    entry_null = ce.find_by_coordinates("orders", "tx_null_1", 9010)[0]
    inc_null = pipeline_bus.increments_for(entry_null["事件ID"])
    coupon = next(f for f in inc_null["fields"] if f["field"] == "coupon")
    check("显式 NULL 可确认并入增量",
          coupon["new_state"] == "null" and coupon["verifiable"] is True)
    scd.apply_increment(scd_cfg, inc_null)
    check("显式 NULL 落库为 NULL 而非待补",
          pipeline_bus.scd_type2_store["2002"][-1]["coupon"] is None
          and "coupon" not in pipeline_bus.pending_backfill.get("2002", {}))

    print("== 5. 兼容已有镜像 ==")
    seed_delta = ce.delta_computation(store.rows("change_event")[0])
    check("存量非JSON镜像不报错", seed_delta["delta_count"] == 0
          and seed_delta["image_status"]["before"] == "legacy_text")
    legacy_entry = {
        "id": 900001, "事件ID": "evt_legacy_trunc", "来源表": "shop_db.orders",
        "操作类型": "UPDATE", "事务ID": "tx_legacy",
        "变更前镜像": "{}", "变更后镜像": json.dumps(
            {"id": 1, "description": "x" * 300}, ensure_ascii=False)[:200],
        "事件时间": "2026-09-01", "status": "已解析",
    }
    store.rows("change_event").append(legacy_entry)
    legacy_delta = ce.delta_computation(legacy_entry)
    check("存量截断镜像容错解析", legacy_delta["image_status"]["after"] == "truncated"
          and legacy_delta["complete"] is False)
    diag = ce.locate_truncation(900001)
    check("截断点定位到消费出口", diag["truncation_point"] is not None
          and "consume_pipeline_events" in diag["truncation_point"])
    diag_new = ce.locate_truncation(entry_new["id"])
    check("新链路无截断", diag_new["chain_healthy"] is True
          and diag_new["truncation_point"] is None)
    store.rows("change_event").remove(legacy_entry)

    print("== 6. 核对 Type2 当前状态 ==")
    state = scd.type2_state("1001")["dimensions"]["1001"]
    current = state["current"]
    applied_tx = {c.get("tx_id") for c in state["applied_coords"]}
    check("Type2 当前 amount = 9", current.get("amount") == 9)
    check("当前 description 保持完整 225 字", current.get("description") == LONG_DESC)
    check("待补 description 在册", "description" in state["pending_backfill"]
          and state["pending_backfill"]["description"]["candidate_new_value"] == "待核验的旧描述")
    check("实际应用的事件坐标 = tx_new_1 + tx_old_1",
          applied_tx == {"tx_new_1", "tx_old_1"}, f"applied={sorted(applied_tx)}")

    failed = [c for c in CHECKS if not c[1]]
    print(f"\n核对结果：{len(CHECKS) - len(failed)}/{len(CHECKS)} 通过")
    for name, _, detail in failed:
        print(f"  未通过：{name} {detail}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
