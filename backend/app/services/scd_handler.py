"""缓慢变化维领域服务：处理维度数据的SCD Type 1/2/3更新策略。

上游：change_event + format_adapter 产出已适配事件 → scd_handler 应用SCD策略
消费：pipeline_bus 中的 SCD 状态存储为下游分析提供维度快照

本模块直接消费 change_event 交付的完整字段增量数据（pipeline_bus.increment_log）：
- 只落库已确认的差异；旧前镜像缺失/破损的字段登记为待补（与显式 NULL 分开记录）；
- 回放按源表、事务号和 binlog_pos 定位事件与维度键；
- 重复提交幂等：不会再次封存有效维度版本；
- 每项应用结果回写到变更追踪清单（change_event）。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class ScdHandlerService(BaseModuleService):
    MODULE = "scd_handler"
    ENTITY = "SCD配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "维度表", "SCD类型", "自然键", "代理键", "生效时间列", "失效时间列"]
    REQUIRED_FIELDS = ["配置名称", "维度表", "SCD类型"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已启用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ---------- 完整字段增量消费 ----------

    def apply_increment(self, config: dict[str, Any], increment: dict[str, Any]) -> dict[str, Any]:
        """消费完整字段增量数据，应用 SCD 策略，并把应用结果回写变更追踪清单。

        完整事件（新旧镜像齐全）的全部差异均可确认并落库；
        长字段（如 225 字描述）随增量完整进入维度，不再丢失。
        """
        scd_type = config.get("SCD类型", "Type2")
        dim_table = config["维度表"]
        natural_keys = self._natural_keys(config)
        dim_key = self._dim_key_from_increment(increment, natural_keys)
        coord = self._event_coord(increment)

        confirmed = {
            f["field"]: f["new"]
            for f in increment.get("fields", []) if f.get("verifiable")
        }
        pending = [f for f in increment.get("fields", []) if not f.get("verifiable")]

        if scd_type in ("Type2", "ScdType2"):
            apply_out = self._apply_confirmed_type2(
                config, dim_key, confirmed, increment, seed_from_after=True)
        else:
            apply_out = {"action": "skip", "version": 0, "row": {},
                         "reason": "字段增量链路当前仅承载 Type2"}

        pending_fields = self._register_pending(dim_table, dim_key, pending, coord)
        result = {
            "dim_table": dim_table,
            "scd_type": scd_type,
            "dim_key": dim_key,
            "coordinate": coord,
            "confirmed_fields": sorted(confirmed),
            "pending_fields": pending_fields,
            "action": apply_out["action"],
            "version": apply_out.get("version"),
            "row": apply_out.get("row"),
        }
        result["应用结果"] = self._result_label(apply_out["action"], pending_fields)
        self._write_back(increment.get("event_id", ""), result)
        pipeline_bus.pipeline_metrics["scd_processed"] += 1
        pipeline_bus.write_audit(
            f"SCD{scd_type}增量", dim_key,
            f"维度表{dim_table}消费字段增量：确认{len(confirmed)}项 "
            f"待补{len(pending_fields)}项 动作{apply_out['action']}",
            "scd_handler", "已处理"
        )
        return result

    def replay_by_coordinates(self, config: dict[str, Any],
                              coordinates: list[dict[str, Any]]) -> dict[str, Any]:
        """按源表、事务号和 binlog_pos 回放历史事件到维度。

        同一维度键多笔旧事件且提交位置无法比较次序时进入冲突模式：
        逐字段核验，只落库已确认的差异；旧前镜像缺口的字段登记待补
        （缺失与显式 NULL 分开记录并说明原因）。
        回放失败可重试：已应用坐标幂等跳过，重复提交不会再次封存有效维度版本。
        """
        from app.services.change_event import ChangeEventService
        ce_service = ChangeEventService()
        natural_keys = self._natural_keys(config)
        dim_table = config["维度表"]

        resolved: list[tuple[dict[str, Any], dict[str, Any]]] = []
        failures: list[dict[str, Any]] = []
        for coord in coordinates:
            matches = ce_service.find_by_coordinates(
                source_table=coord.get("source_table"),
                tx_id=coord.get("tx_id"),
                binlog_pos=coord.get("binlog_pos"),
            )
            if not matches:
                failures.append({
                    "coordinate": coord,
                    "status": "failed",
                    "error": "变更追踪清单中未找到对应事件，可修正坐标后重试",
                })
                continue
            entry = matches[0]
            increment = (
                pipeline_bus.increments_for(entry.get("事件ID", ""))
                or ce_service.compute_increment(entry)
            )
            resolved.append((entry, increment))

        groups: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
        for entry, inc in resolved:
            dim_key = self._dim_key_from_increment(inc, natural_keys)
            groups.setdefault(dim_key, []).append((entry, inc))

        group_results: list[dict[str, Any]] = []
        for dim_key, members in groups.items():
            conflict = len(members) > 1
            positions_comparable = self._positions_comparable([inc for _, inc in members])

            # 逐字段汇总组内投票：可核验且取值唯一才确认
            field_votes: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
            for _entry, inc in members:
                for f in inc.get("fields", []):
                    field_votes.setdefault(f["field"], []).append((f, inc))
            confirmed: dict[str, Any] = {}
            pending: list[dict[str, Any]] = []
            for field, votes in field_votes.items():
                verifiable = [v for v in votes if v[0]["verifiable"]]
                distinct = {
                    json.dumps(v[0]["new"], ensure_ascii=False, sort_keys=True, default=str)
                    for v in verifiable
                }
                if verifiable and len(distinct) == 1:
                    confirmed[field] = verifiable[0][0]["new"]
                else:
                    if len(distinct) > 1:
                        reason = "多笔旧事件冲突且提交位置无法排序，本轮无法确认该字段"
                    else:
                        reason = votes[0][0].get("reason") or "旧前镜像缺少旧值，无法核验"
                    pending.append({"field": field, "new": votes[0][0].get("new"),
                                    "reason": reason})

            # 只落库已确认的差异：逐事件提交，坐标幂等
            event_results: list[dict[str, Any]] = []
            for entry, inc in members:
                coord = self._event_coord(inc)
                own_verifiable = {
                    f["field"] for f in inc.get("fields", []) if f.get("verifiable")
                }
                event_confirmed = {
                    k: v for k, v in confirmed.items() if k in own_verifiable
                }
                if event_confirmed:
                    apply_out = self._apply_confirmed_type2(
                        config, dim_key, event_confirmed, inc, seed_from_after=False)
                    action = apply_out["action"]
                else:
                    apply_out = {"action": "no_confirmed_fields", "version": None, "row": None}
                    action = "no_confirmed_fields"
                event_pending = [p["field"] for p in pending
                                 if any(v[0]["field"] == p["field"] and v[1] is inc
                                        for v in field_votes[p["field"]])]
                result = {
                    "dim_key": dim_key,
                    "coordinate": coord,
                    "confirmed_fields": sorted(event_confirmed),
                    "pending_fields": event_pending,
                    "action": action,
                }
                result["应用结果"] = self._result_label(action, event_pending)
                self._write_back(inc.get("event_id", ""), result)
                event_results.append({
                    "event_id": inc.get("event_id", ""),
                    "coordinate": coord,
                    "action": action,
                    "confirmed_fields": sorted(event_confirmed),
                    "pending_fields": event_pending,
                })

            pending_names = self._register_pending(
                dim_table, dim_key, pending, self._event_coord(members[0][1]))
            pipeline_bus.pipeline_metrics["scd_processed"] += 1
            pipeline_bus.write_audit(
                "SCD回放", dim_key,
                f"回放{len(members)}笔旧事件：确认{sorted(confirmed)} 待补{pending_names} "
                f"冲突={'是' if conflict else '否'} 位置可排序={'是' if positions_comparable else '否'}",
                "scd_handler", "已处理"
            )
            group_results.append({
                "dim_key": dim_key,
                "event_count": len(members),
                "conflict": conflict,
                "positions_comparable": positions_comparable,
                "confirmed_fields": sorted(confirmed),
                "pending_fields": pending_names,
                "events": event_results,
            })

        return {
            "dim_table": dim_table,
            "groups": group_results,
            "failures": failures,
            "retryable": bool(failures),
        }

    def type2_state(self, dim_key: str | None = None) -> dict[str, Any]:
        """Type2 状态核对：当前版本、待补字段、实际应用的事件坐标。"""
        store2 = pipeline_bus.scd_type2_store
        keys = [dim_key] if dim_key else list(store2)
        state: dict[str, Any] = {}
        for key in keys:
            history = store2.get(key, [])
            state[key] = {
                "current": history[-1] if history else {},
                "version_count": len(history),
                "applied_coords": list(pipeline_bus.scd_type2_applied.get(key, [])),
                "pending_backfill": dict(pipeline_bus.pending_backfill.get(key, {})),
            }
        return {"type": "Type2", "dimensions": state}

    # ---------- 内部：确认、落库、待补登记、回写 ----------

    @staticmethod
    def _natural_keys(config: dict[str, Any]) -> list[str]:
        keys = [k.strip() for k in str(config.get("自然键", "id")).split(",") if k.strip()]
        return keys or ["id"]

    @staticmethod
    def _dim_key_from_increment(increment: dict[str, Any], natural_keys: list[str]) -> str:
        after = increment.get("after") or {}
        before = increment.get("before") or {}
        parts = [str(after.get(nk, before.get(nk, ""))) for nk in natural_keys]
        return "|".join(parts) if parts else str(increment.get("event_id", "unknown"))

    @staticmethod
    def _event_coord(increment: dict[str, Any]) -> dict[str, Any]:
        return {
            k: increment.get(k)
            for k in ("source_table", "tx_id", "binlog_file", "binlog_pos", "event_id")
        }

    @staticmethod
    def _positions_comparable(increments: list[dict[str, Any]]) -> bool:
        """提交位置是否可比较次序：跨 binlog 文件或位置相同均不可排序。"""
        files = {str(i.get("binlog_file", "")) for i in increments}
        if len(files) > 1:
            return False
        positions = [i.get("binlog_pos") for i in increments]
        return len(set(positions)) == len(positions)

    def _apply_confirmed_type2(self, config: dict[str, Any], dim_key: str,
                               confirmed: dict[str, Any], increment: dict[str, Any],
                               seed_from_after: bool) -> dict[str, Any]:
        """以当前有效版本为基线，只叠加已确认差异，幂等提交到 Type2。"""
        history = pipeline_bus.scd_type2_store.get(dim_key, [])
        if not history and not confirmed and not seed_from_after:
            return {"action": "no_confirmed_fields", "version": 0, "row": {}}
        base: dict[str, Any] = {}
        if history:
            base = {
                k: v for k, v in history[-1].items()
                if not str(k).startswith("_")
                and k not in ("effective_from", "effective_to", "version")
            }
        elif seed_from_after:
            base = dict(increment.get("after") or {})
        row = {**base, **confirmed}
        after = increment.get("after") or {}
        for nk in self._natural_keys(config):
            if after.get(nk) is not None:
                row.setdefault(nk, after[nk])
        row["effective_from"] = datetime.now().isoformat()
        row["effective_to"] = "9999-12-31"
        row["_source_table"] = config["维度表"]
        row["_event_id"] = increment.get("event_id", "")
        out = pipeline_bus.apply_scd_type2_tracked(
            dim_key, row, event_coord=self._event_coord(increment))
        return {
            "action": out["action"],
            "version": out["version"],
            "row": out["history"][-1] if out["history"] else {},
        }

    def _register_pending(self, dim_table: str, dim_key: str,
                          pending: list[dict[str, Any]],
                          coord: dict[str, Any]) -> list[str]:
        """把无法确认的字段登记为待补：缺失旧值与显式 NULL 分开记录并说明原因。"""
        names: list[str] = []
        for item in pending:
            record = {
                "dim_table": dim_table,
                "dim_key": dim_key,
                "field": item["field"],
                "status": "待补",
                "value_state": "missing",  # 缺失旧值 ≠ 显式 NULL
                "reason": item.get("reason") or "旧前镜像缺少旧值，无法核验",
                "candidate_new_value": item.get("new"),
                "source_events": [coord],
                "recorded_at": datetime.now().isoformat(),
            }
            pipeline_bus.record_pending_backfill(dim_key, item["field"], record)
            names.append(item["field"])
        return names

    @staticmethod
    def _result_label(action: str, pending_fields: list[str]) -> str:
        if action == "duplicate":
            return "重复跳过（未再次封存有效版本）"
        if pending_fields:
            return "部分确认（存在待补字段）"
        if action == "append":
            return "已应用"
        if action == "noop":
            return "已确认（值幂等，未新增版本）"
        if action == "no_confirmed_fields":
            return "待补（本轮无可确认字段）"
        return "已处理"

    @staticmethod
    def _write_back(event_id: str, result: dict[str, Any]) -> None:
        """把应用结果回写到变更追踪清单。"""
        if not event_id:
            return
        from app.services.change_event import ChangeEventService
        ChangeEventService().write_back_application(event_id, result)

    # ---------- 既有能力（保留兼容） ----------

    def apply_scd(self, config: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        """根据配置的SCD类型处理变更事件。

        读取事件中的变更前后镜像和自然键，
        对维度表执行 Type1(覆盖)、Type2(历史)、Type3(前值列) 策略。
        """
        scd_type = config.get("SCD类型", "Type2")
        dim_table = config["维度表"]
        natural_keys = config.get("自然键", "id").split(",")
        effective_col = config.get("生效时间列", "effective_from")
        surrogate_key = config.get("代理键", f"sk_{dim_table}")

        dim_key = self._build_dim_key(event, natural_keys)
        row = self._extract_dim_row(event, dim_table)
        row["effective_from"] = datetime.now().isoformat()
        row["effective_to"] = "9999-12-31"

        result: dict[str, Any] = {"dim_table": dim_table, "scd_type": scd_type, "natural_keys": dim_key}

        if scd_type in ("Type1", "ScdType1"):
            updated = pipeline_bus.apply_scd_type1(dim_key, row)
            result["action"] = "overwrite"
            result["row"] = updated
        elif scd_type in ("Type2", "ScdType2"):
            # 封存旧版本并追加新版本（幂等：重复坐标/相同值不重复封存）
            history = pipeline_bus.apply_scd_type2(dim_key, row)
            result["action"] = "append_version"
            result["version"] = len(history)
            result["row"] = history[-1]
        elif scd_type in ("Type3", "ScdType3"):
            col_name = effective_col if effective_col else "status"
            updated = pipeline_bus.apply_scd_type3(dim_key, col_name, row)
            result["action"] = "add_previous_column"
            result["row"] = updated
        else:
            result["action"] = "skip"
            result["reason"] = "unknown_scd_type"

        pipeline_bus.pipeline_metrics["scd_processed"] += 1
        pipeline_bus.write_audit(
            f"SCD{scd_type}", dim_key,
            f"维度表{dim_table}应用{scd_type}策略 {result.get('action')}",
            "scd_handler", "已处理"
        )
        return result

    def _build_dim_key(self, event: dict[str, Any], natural_keys: list[str]) -> str:
        parts: list[str] = []
        for nk in natural_keys:
            nk = nk.strip()
            val = event.get(nk, event.get("变更后镜像", {}).get(nk, ""))
            parts.append(str(val))
        return "|".join(parts) if parts else event.get("事件ID", "unknown")

    def _extract_dim_row(self, event: dict[str, Any], dim_table: str) -> dict[str, Any]:
        import json as _json
        after = event.get("变更后镜像", {})
        if isinstance(after, str):
            try:
                after = _json.loads(after)
            except (_json.JSONDecodeError, TypeError):
                after = {}
        row = dict(after) if isinstance(after, dict) else {}
        row["_source_table"] = dim_table
        row["_event_id"] = event.get("事件ID", "")
        row["_operation"] = event.get("操作类型", "")
        return row

    def query_dimension_snapshot(self, dim_table: str, scd_type: str) -> dict[str, Any]:
        """查询维度表的SCD快照。"""
        if scd_type in ("Type1", "ScdType1"):
            store = pipeline_bus.scd_type1_store
        elif scd_type in ("Type2", "ScdType2"):
            current_only = {k: v[-1] if v else {} for k, v in pipeline_bus.scd_type2_store.items()}
            return {"type": "Type2", "dim_table": dim_table, "current_count": len(current_only), "total_history": sum(len(v) for v in pipeline_bus.scd_type2_store.values()), "snapshot": current_only}
        elif scd_type in ("Type3", "ScdType3"):
            store = pipeline_bus.scd_type3_store
        else:
            return {"type": scd_type, "dim_table": dim_table, "count": 0, "snapshot": {}}
        return {"type": scd_type, "dim_table": dim_table, "count": len(store), "snapshot": dict(store)}

    def detect_scd_conflicts(self, natural_keys: list[str], event_batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """检测同一自然键在批次内的冲突更新。"""
        key_counts: dict[str, list[dict[str, Any]]] = {}
        for evt in event_batch:
            key = self._build_dim_key(evt, natural_keys)
            key_counts.setdefault(key, []).append(evt)
        conflicts = []
        for key, evts in key_counts.items():
            if len(evts) > 1:
                conflicts.append({
                    "natural_key": key,
                    "update_count": len(evts),
                    "resolves_to": "last_write_wins",
                    "event_ids": [e.get("事件ID", "") for e in evts],
                })
        return conflicts

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用配置":
            dim_table = entry.get("维度表", "")
            pipeline_bus.write_audit(
                "SCD启用", dim_table,
                f"启用{dim_table}的{entry.get('SCD类型','Type2')}SCD策略",
                "scd_handler", "已启用"
            )
        return entry, msg
