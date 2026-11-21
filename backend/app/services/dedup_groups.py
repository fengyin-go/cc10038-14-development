"""去重组对照视图与回溯重放联动。

一次去重会把同一去重键下的记录固化成「去重组」：一条采纳记录（胜出）和
若干被淘汰记录，每条都保留原始日志来源、字段快照和本次采用的字段版本。

回溯重放从去重组入口取得要复算的原文范围：先按新的字段解析重算组内记录，
再沿着「新键与组外已使用记录重合」的关系把受影响的组一并纳入，直到范围稳定。
重放结果先落在未提交的草稿区，确认前下游（聚合、导出）一律阻断，避免读到
半个去重组；确认时一次性替换旧组并回写修正清单。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.store import store

# 内部表用下划线前缀，store 不会把它们当作业务模块暴露到概览里。
GROUP_TABLE = "_dedup_group"
CORRECTION_TABLE = "_replay_correction"
PENDING_PHASES = {"重放中", "已中断", "待确认"}
DEMO_BATCH_NO = "INGE-DEMO-01"
DEMO_SOURCE_ID = "demo-app-logs"
DEMO_RULE_VERSION = "demo-rule-v1"
DEMO_KEY_FIELDS = ["time", "level"]
DEMO_PARSER_FIELDS = ["time", "level", "message"]
# 演示正文是 `时间|通道|级别|正文`。旧规则把通道当成 level（第 1 段）；
# 把 level 的解析改到第 2 段后，同一旧组里被淘汰的日志会得到不同的新键。
DEMO_POSITIONS = {"time": 0, "level": 1, "message": 3}
DEMO_LOGS: list[str] = [
    # 组 A：两条同键日志（旧键 08:00|app），一条胜出、一条淘汰。
    "2026-06-01T08:00:00|app|INFO|svc-a heartbeat",
    "2026-06-01T08:00:00|app|WARN|svc-a retry",
    # 组 B：三条同键日志（旧键 08:00|worker），两条被淘汰，
    # 规则改成取第 2 段后分别得到 WARN / ERROR / DEBUG 三个不同新键。
    "2026-06-01T08:00:00|worker|WARN|svc-b degraded",
    "2026-06-01T08:00:00|worker|ERROR|svc-b overflow",
    "2026-06-01T08:00:00|worker|DEBUG|svc-b detail",
    # 组 C：组外已使用日志，新键 08:00|ERROR，与组 B 淘汰日志的新键重合。
    "2026-06-01T08:00:00|proxy|ERROR|svc-c timeout",
    # 组 D：时间不同，不进入重放范围。
    "2026-06-01T08:10:00|worker|INFO|svc-d idle",
]


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def _json_fields(entity: dict[str, Any]) -> dict[str, Any]:
    try:
        data = json.loads(entity.get("标准化字段集", "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _positions(member: dict[str, Any]) -> dict[str, int]:
    raw = member.get("field_positions")
    if isinstance(raw, dict):
        return {str(k): int(v) for k, v in raw.items() if str(v).lstrip("-").isdigit()}
    return dict(DEMO_POSITIONS)


def _normalized_versions(
    base: dict[str, dict[str, Any]], entity: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """没有真实解析规则时，用明细上记下的段位补齐字段版本。"""
    if base and all(v.get("parser_rule") for v in base.values()):
        return {}
    positions = _positions(entity)
    return {
        field: {
            "field": field,
            "key_field": True,
            "source_position": positions.get(field, ""),
            "parser_rule": entity.get("parser_rule") or entity.get("解析规则版本", ""),
            "parsed_at": entity.get("parsed_at", ""),
        }
        for field in base
    }


def _make_key(fields: dict[str, Any], key_fields: list[str]) -> str:
    return "||".join(str(fields.get(field, "")) for field in key_fields)


def _parse_body(body: str, positions: dict[str, int]) -> dict[str, str]:
    parts = body.split("|")
    fields: dict[str, str] = {}
    for field, pos in positions.items():
        fields[field] = parts[pos].strip() if 0 <= pos < len(parts) else ""
    return fields


def _member(entity: dict[str, Any], winner_id: int, eliminated: bool) -> dict[str, Any]:
    """把标准化明细展开成对照视图里的一行（含原文来源）。"""
    raw_id = str(entity.get("原始记录ID", ""))
    raw = None
    for row in store.rows("raw_records"):
        if str(row.get("id")) == raw_id:
            raw = row
            break
    return {
        "entity_id": entity.get("id"),
        "raw_record_id": raw_id,
        "batch_no": entity.get("来源批次", ""),
        "line_no": raw.get("原始行号", "") if raw else "",
        "raw_body": raw.get("原始正文", "") if raw else "",
        "ingested_at": raw.get("采集时间戳", "") if raw else "",
        "fields": _json_fields(entity),
        "field_versions": entity.get("field_versions") or {},
        "parser_rule": entity.get("parser_rule") or entity.get("解析规则版本", ""),
        "entity_status": entity.get("status", ""),
        "dedup_id": entity.get("dedup_id", ""),
        "eliminated": eliminated,
        "winner_entity_id": winner_id,
        "eliminated_reason": (
            f"与原始记录#{winner_id} 去重键相同，保留策略未采纳" if eliminated else ""
        ),
    }


def _public_group(group: dict[str, Any]) -> dict[str, Any]:
    winner_id = int(group.get("winning_entity_id", 0))
    members = []
    for entity_id in group.get("member_entity_ids", []):
        entity = store.find("parsed_entity", int(entity_id))
        if entity is not None:
            members.append(_member(entity, winner_id, int(entity_id) != winner_id))
    eliminated = [m for m in members if m["eliminated"]]
    return {
        "id": group["id"],
        "group_no": group.get("group_no", ""),
        "batch_no": group.get("batch_no", ""),
        "key_fields": group.get("key_fields", ""),
        "key_value": group.get("key_value", ""),
        "retain_strategy": group.get("retain_strategy", ""),
        "state": group.get("state", ""),
        "active": bool(group.get("active", True)),
        "replay_id": group.get("replay_id"),
        "created_at": group.get("created_at", ""),
        "winner_entity_id": winner_id,
        "member_count": len(members),
        "eliminated_count": len(eliminated),
        "field_versions": group.get("field_versions") or {},
        "winner": next((m for m in members if not m["eliminated"]), None),
        "members": members,
        "eliminated": eliminated,
    }
class DedupGroupService:
    # ---------- 建组 ----------
    def build_groups(
        self,
        key_fields: list[str],
        retain_strategy: str,
        parser_rule: str,
        field_versions: dict[str, dict[str, Any]],
        batch_no: str | None = None,
    ) -> list[dict[str, Any]]:
        """按当前解析结果把已完成校验的明细固化成去重组并落库。"""
        buckets: dict[str, list[dict[str, Any]]] = {}
        for entity in store.rows("parsed_entity"):
            if entity.get("来源批次") == "":
                continue
            if batch_no and entity.get("来源批次") != batch_no:
                continue
            if entity.get("status") not in ("校验通过", "校验失败"):
                continue
            if entity.get("dedup_group_id"):
                continue
            key = _make_key(_json_fields(entity), key_fields)
            buckets.setdefault(key, []).append(entity)

        created: list[dict[str, Any]] = []
        for key, members in buckets.items():
            winner = members[0] if retain_strategy == "keep_first" else members[-1]
            for entity in members:
                entity["field_versions"] = {
                    **_normalized_versions(field_versions, entity),
                    **field_versions,
                    **(entity.get("field_versions") or {}),
                }
                entity["dedup_id"] = str(winner["id"]) if entity is not winner else ""
            group = self._persist_group(
                key_fields=key_fields,
                key_value=key,
                retain_strategy=retain_strategy,
                batch_no=members[0].get("来源批次", ""),
                members=members,
                winner_id=int(winner["id"]),
                replay_id=None,
                active=True,
                state="已确认",
                field_versions=entity["field_versions"],
                parser_rule=parser_rule,
            )
            for entity in members:
                entity["dedup_group_id"] = group["id"]
            created.append(group)
        return created

    def _persist_group(
        self,
        *,
        key_fields: list[str],
        key_value: str,
        retain_strategy: str,
        batch_no: str,
        members: list[dict[str, Any]],
        winner_id: int,
        replay_id: int | None,
        active: bool,
        state: str,
        field_versions: dict[str, Any],
        parser_rule: str,
    ) -> dict[str, Any]:
        group = {
            "id": _next_id(GROUP_TABLE),
            "group_no": "",
            "batch_no": batch_no,
            "key_fields": ",".join(key_fields),
            "key_value": key_value,
            "retain_strategy": retain_strategy,
            "state": state,
            "active": active,
            "status": "已确认" if state == "已确认" else state,
            "pending": state != "已确认",
            "abnormal": False,
            "replay_id": replay_id,
            "winning_entity_id": winner_id,
            "member_entity_ids": [int(m["id"]) for m in members],
            "field_versions": field_versions,
            "parser_rule": parser_rule,
            "batch_no_list": sorted({str(m.get("来源批次", "")) for m in members}),
            "raw_record_ids": [str(m.get("原始记录ID", "")) for m in members],
            "created_at": _now_iso(),
        }
        group["group_no"] = f"DG-{group['id']:04d}"
        store.rows(GROUP_TABLE).append(group)
        return group

    # ---------- 查询 ----------
    def list_groups(
        self,
        *,
        keyword: str = "",
        state: str = "",
        batch_no: str = "",
        include_pending: bool = True,
    ) -> list[dict[str, Any]]:
        groups = []
        for group in store.rows(GROUP_TABLE):
            if not include_pending and not group.get("active", True):
                continue
            if state and group.get("state") != state:
                continue
            if batch_no and group.get("batch_no") != batch_no:
                continue
            if keyword:
                haystack = " ".join([
                    str(group.get("group_no", "")),
                    str(group.get("key_value", "")),
                    str(group.get("batch_no", "")),
                ])
                if keyword.casefold() not in haystack.casefold():
                    continue
            groups.append(_public_group(group))
        return groups

    def get_group(self, group_id: int) -> dict[str, Any] | None:
        group = store.find(GROUP_TABLE, group_id)
        return _public_group(group) if group else None

    def group_for_entity(self, entity_id: int) -> dict[str, Any] | None:
        for group in store.rows(GROUP_TABLE):
            if entity_id in [int(i) for i in group.get("member_entity_ids", [])]:
                return _public_group(group)
        return None

    # ---------- 未确认重放的门禁 ----------
    def pending_replays(self) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(CORRECTION_TABLE)
            if row.get("phase") in PENDING_PHASES
        ]

    def downstream_blocker(self) -> str | None:
        """下游读取前的门禁：存在未确认重放时给出阻断原因。"""
        pending = self.pending_replays()
        if not pending:
            return None
        numbers = "、".join(row.get("task_no", "") for row in pending)
        return f"重放任务 {numbers} 尚未确认，去重组结果暂不可被下游读取"

    # ---------- 演示数据 ----------
    def ensure_demo_group(self) -> dict[str, Any]:
        """准备一组「两条同键原始日志」的去重结果，供对照视图直接打开。"""
        existing = self.find_demo_group()
        if existing is not None:
            return {"ok": True, "message": "演示去重组已存在", "group": _public_group(existing)}

        if not any(r.get("批次编号") == DEMO_BATCH_NO for r in store.rows("raw_records")):
            source = next(
                (s for s in store.rows("source_registry") if s.get("来源标识") == DEMO_SOURCE_ID),
                None,
            )
            if source is None:
                source = {
                    "id": _next_id("source_registry"),
                    "来源标识": DEMO_SOURCE_ID,
                    "接入协议": "http",
                    "连接凭证": f"token-demo",
                    "采集区域": "default",
                    "状态": "已验证",
                    "status": "已验证",
                    "pending": False,
                    "abnormal": False,
                    "source_version": 2,
                }
                store.rows("source_registry").append(source)
            batch = {
                "id": _next_id("ingest_batch"),
                "批次编号": DEMO_BATCH_NO,
                "所属数据源": DEMO_SOURCE_ID,
                "采集时间窗口": "2026-06-01",
                "记录总数": str(len(DEMO_LOGS)),
                "批次状态": "已完成",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "batch_version": 1,
            }
            store.rows("ingest_batch").append(batch)

            rule = next(
                (p for p in store.rows("field_parser") if p.get("规则名称") == DEMO_RULE_VERSION),
                None,
            )
            if rule is None:
                rule = {
                    "id": _next_id("field_parser"),
                    "规则名称": DEMO_RULE_VERSION,
                    "适用数据源": DEMO_SOURCE_ID,
                    "输入格式": "pipe-delimited",
                    "输出字段列表": ",".join(DEMO_PARSER_FIELDS),
                    "解析引擎": "内置分隔符解析",
                    "status": "已发布",
                    "pending": False,
                    "abnormal": False,
                }
                store.rows("field_parser").append(rule)

            for idx, body in enumerate(DEMO_LOGS):
                raw = {
                    "id": _next_id("raw_records"),
                    "批次编号": DEMO_BATCH_NO,
                    "原始行号": str(idx + 1),
                    "采集时间戳": "2026-06-01T08:00:00",
                    "原始正文": body,
                    "解析状态": "已解析",
                    "status": "已解析",
                    "pending": False,
                    "abnormal": False,
                    "source_id": DEMO_SOURCE_ID,
                }
                store.rows("raw_records").append(raw)
                fields = _parse_body(body, DEMO_POSITIONS)
                entity = {
                    "id": _next_id("parsed_entity"),
                    "来源批次": DEMO_BATCH_NO,
                    "原始记录ID": str(raw["id"]),
                    "解析规则版本": DEMO_RULE_VERSION,
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "校验通过",
                    "status": "校验通过",
                    "pending": False,
                    "abnormal": False,
                    "parser_rule": DEMO_RULE_VERSION,
                    "schema_version": "v1.0",
                    "parsed_at": "2026-06-01T08:00:00",
                    "content_digest": "",
                    "field_positions": dict(DEMO_POSITIONS),
                }
                store.rows("parsed_entity").append(entity)

        field_versions = {
            name: {
                "field": name,
                "source_position": pos,
                "key_field": name in DEMO_KEY_FIELDS,
                "parser_rule": DEMO_RULE_VERSION,
                "parsed_at": "2026-06-01T08:00:00",
            }
            for name, pos in DEMO_POSITIONS.items()
        }
        created = self.build_groups(
            key_fields=DEMO_KEY_FIELDS,
            retain_strategy="keep_first",
            parser_rule=DEMO_RULE_VERSION,
            field_versions=field_versions,
            batch_no=DEMO_BATCH_NO,
        )
        group = self.find_demo_group()
        if group is not None:
            return {"ok": True, "message": "演示去重组已生成", "group": _public_group(group)}
        return {"ok": False, "message": "演示数据未形成两条同键日志的去重组"}

    def find_demo_group(self) -> dict[str, Any] | None:
        """入口组：两条同键日志的去重结果（一胜出、一淘汰）。"""
        candidates = [
            group for group in store.rows(GROUP_TABLE)
            if group.get("batch_no") == DEMO_BATCH_NO and len(group.get("member_entity_ids", [])) == 2
        ]
        if candidates:
            return min(candidates, key=lambda g: int(g["id"]))
        return None

    # ---------- 重放：影响范围 ----------
    def preview_replay(
        self,
        group_id: int,
        field: str,
        position: int,
        new_rule_version: str | None = None,
    ) -> dict[str, Any]:
        """值班员在组视图点「发起重放」时看到的受影响范围与待复算清单。"""
        seed = store.find(GROUP_TABLE, group_id)
        if seed is None:
            return {"ok": False, "message": f"去重组 {group_id} 不存在"}
        if not field.strip() or position < 0:
            return {"ok": False, "message": "需要指定要重算的字段和它在原文中的段位"}

        scope = self._collapse_scope(self._affect_scope(seed, field, position))
        records = self._replay_records(scope, field, position)
        source_groups = [g["group_no"] for g in scope]
        return {
            "ok": True,
            "seed_group_id": group_id,
            "field": field,
            "position": position,
            "new_rule_version": new_rule_version or self._next_rule_version(field, position),
            "source_groups": source_groups,
            "affected_group_count": len(scope),
            "affected_groups": [_public_group(g) for g in scope],
            "records": records,
            "record_count": len(records),
            "raw_record_count": len({r["raw_record_id"] for r in records}),
            "cross_group_records": [r for r in records if r.get("cross_group")],
        }

    def _affect_scope(
        self, seed: dict[str, Any], field: str, position: int
    ) -> list[dict[str, Any]]:
        """从入口组出发，把「新键与组外已用记录重合」的组一路纳入。"""
        scope: dict[int, dict[str, Any]] = {int(seed["id"]): seed}
        queue: list[dict[str, Any]] = [seed]
        key_fields = [f for f in str(seed.get("key_fields", "")).split(",") if f]
        while queue:
            group = queue.pop(0)
            for target in self._overlapping_groups(group, field, position, key_fields):
                tid = int(target["id"])
                if tid not in scope:
                    scope[tid] = target
                    queue.append(target)
        return list(scope.values())

    def _rekeyed(self, group: dict[str, Any], field: str, position: int) -> list[tuple[dict[str, Any], str]]:
        """按新的字段解析重算组内成员的去重键。"""
        key_fields = [f for f in str(group.get("key_fields", "")).split(",") if f]
        result = []
        for entity_id in group.get("member_entity_ids", []):
            entity = store.find("parsed_entity", int(entity_id))
            if entity is None:
                continue
            body = self._raw_body(entity)
            fields = _json_fields(entity)
            if body:
                parts = body.split("|")
                if position < len(parts):
                    fields[field] = parts[position].strip()
            result.append(({"entity": entity, "fields": fields}, _make_key(fields, key_fields)))
        return result

    def _overlapping_groups(
        self, group: dict[str, Any], field: str, position: int, key_fields: list[str]
    ) -> list[dict[str, Any]]:
        """新键与组外组的新键重合即纳入；组边界本来按旧键划分，重合必是新产生的。"""
        own_keys = {key for _, key in self._rekeyed(group, field, position)}
        found = []
        for other in store.rows(GROUP_TABLE):
            if int(other["id"]) == int(group["id"]) or not other.get("active", True):
                continue
            other_keys = {key for _, key in self._rekeyed(other, field, position)}
            if own_keys & other_keys:
                found.append(other)
        return found

    def _collapse_scope(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按组号去重并稳定排序，确保预览和重放拿到同一份范围。"""
        selected: dict[int, dict[str, Any]] = {}
        for group in groups:
            selected[int(group["id"])] = group
        return sorted(selected.values(), key=lambda g: int(g["id"]))

    def _raw_body(self, entity: dict[str, Any]) -> str:
        raw_id = str(entity.get("原始记录ID", ""))
        for raw in store.rows("raw_records"):
            if str(raw.get("id")) == raw_id:
                return str(raw.get("原始正文", ""))
        return ""

    def _replay_records(
        self, scope: list[dict[str, Any]], field: str, position: int
    ) -> list[dict[str, Any]]:
        """把范围展平成待复算的原始日志清单，并标出跨组重合的条目。"""
        keys_by_group = {
            group["group_no"]: {key for _, key in self._rekeyed(group, field, position)}
            for group in scope
        }
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        for group in scope:
            for item, key in self._rekeyed(group, field, position):
                entity = item["entity"]
                raw_id = str(entity.get("原始记录ID", ""))
                if raw_id in seen:
                    continue
                seen.add(raw_id)
                # 只有新键落在「组外」已使用的键上才算跨组重合，需要在清单里点明。
                cross_nos = [
                    other_no for other_no, other_keys in keys_by_group.items()
                    if other_no != group["group_no"] and key in other_keys
                ]
                records.append({
                    "entity_id": int(entity["id"]),
                    "raw_record_id": raw_id,
                    "item_no": len(records) + 1,
                    "source_group_id": int(group["id"]),
                    "source_group_no": group["group_no"],
                    "batch_no": entity.get("来源批次", ""),
                    "line_no": self._raw_meta(raw_id).get("原始行号", ""),
                    "raw_body": self._raw_body(entity),
                    "current_fields": _json_fields(entity),
                    "current_key": group.get("key_value", ""),
                    "recomputed_fields": item["fields"],
                    "recomputed_key": key,
                    "key_changed": key != group.get("key_value", ""),
                    "was_winner": int(entity["id"]) == int(group.get("winning_entity_id", 0)),
                    "cross_group": bool(cross_nos),
                    "cross_group_nos": cross_nos,
                    "recomputed_rule_version": "",
                    "note": (
                        f"新键与组 {'、'.join(cross_nos)} 的已使用记录重合，需一并复算"
                        if cross_nos else ""
                    ),
                    "added_by_group": group["group_no"],
                })
        return records

    def _raw_meta(self, raw_id: str) -> dict[str, Any]:
        for raw in store.rows("raw_records"):
            if str(raw.get("id")) == raw_id:
                return raw
        return {}

    def _next_rule_version(self, field: str, position: int) -> str:
        return f"{DEMO_RULE_VERSION}-{field}@{position}"

    # ---------- 重放：草稿、确认、恢复 ----------
    def start_replay(
        self,
        group_id: int,
        field: str,
        position: int,
        new_rule_version: str | None = None,
        interrupt_after: int | None = None,
    ) -> dict[str, Any]:
        """从去重组发起重放。结果只写入草稿区，确认前下游被阻断。"""
        preview = self.preview_replay(group_id, field, position, new_rule_version)
        if not preview["ok"]:
            return preview
        blocking = self.downstream_blocker()
        if blocking:
            return {"ok": False, "message": f"已有重放未确认：{blocking}"}

        task = self._new_task(preview, group_id, field, position)
        task["phase"] = "重放中"
        task["status"] = "重放中"
        task["draft"] = []
        self._mirror_task(task)
        store.rows(CORRECTION_TABLE).append(task)

        done = self._write_draft(task, preview["records"], field, position, interrupt_after)
        if interrupt_after is not None and done < len(preview["records"]):
            task["phase"] = "已中断"
            task["status"] = "部分失败"
            task["pending"] = True
            task["abnormal"] = True
            task["interrupted_at"] = done
            self._mirror_task(task)
            return {
                "ok": False,
                "interrupted": True,
                "message": f"重放在第 {done}/{len(preview['records'])} 条后中断，草稿未提交",
                "task": self._public_task(task),
            }

        task["phase"] = "待确认"
        task["status"] = "待确认"
        self._mirror_task(task)
        return {
            "ok": True,
            "message": f"重放完成，{len(preview['records'])} 条待复算原文已进入草稿，确认后才可被下游读取",
            "task": self._public_task(task),
            "preview": preview,
        }

    def _new_task(
        self, preview: dict[str, Any], group_id: int, field: str, position: int
    ) -> dict[str, Any]:
        task_id = _next_id(CORRECTION_TABLE)
        scope = preview["affected_groups"]
        return {
            "id": task_id,
            "task_no": f"RPL-{task_id:04d}",
            "phase": "待确认",
            "status": "重放中",
            "pending": True,
            "abnormal": False,
            "seed_group_id": group_id,
            "seed_group_no": next(
                (g["group_no"] for g in scope if g["id"] == group_id), ""
            ),
            "field": field,
            "position": position,
            "new_rule_version": preview["new_rule_version"],
            "source_groups": preview["source_groups"],
            "affected_group_count": preview["affected_group_count"],
            "record_count": preview["record_count"],
            "raw_record_count": preview["raw_record_count"],
            "records": preview["records"],
            "draft": [],
            "corrections": [],
            "created_at": _now_iso(),
            "confirmed_at": "",
            "历史批次范围": "、".join(sorted({g["batch_no"] for g in scope})),
            "新解析规则版本": preview["new_rule_version"],
            "重放状态": "待确认",
            "产出记录数": "0",
            "执行时间": _now_iso(),
        }

    def _write_draft(
        self,
        task: dict[str, Any],
        records: list[dict[str, Any]],
        field: str,
        position: int,
        interrupt_after: int | None,
    ) -> int:
        """把重算结果写进草稿；中断时保留已写的部分，但不触碰线上组。"""
        draft: list[dict[str, Any]] = task["draft"]
        start = len(draft)
        for index, record in enumerate(records[start:], start=start):
            if interrupt_after is not None and index >= interrupt_after:
                return index
            fields = dict(record["recomputed_fields"])
            draft.append({
                "entity_id": record["entity_id"],
                "raw_record_id": record["raw_record_id"],
                "source_group_id": record["source_group_id"],
                "source_group_no": record["source_group_no"],
                "batch_no": record["batch_no"],
                "line_no": record["line_no"],
                "raw_body": record["raw_body"],
                "old_key": record["current_key"],
                "new_key": record["recomputed_key"],
                "key_changed": record["key_changed"],
                "was_winner": record["was_winner"],
                "cross_group": record["cross_group"],
                "fields": fields,
                "field_version": {
                    "field": field,
                    "source_position": position,
                    "parser_rule": task["new_rule_version"],
                },
            })
        task["产出记录数"] = str(len(draft))
        return len(records)

    def resume_replay(self, task_id: int) -> dict[str, Any]:
        """重放中断后恢复：只把草稿补完，仍然不能提前让下游读到结果。"""
        task = self._find_task(task_id)
        if task is None:
            return {"ok": False, "message": f"重放任务 {task_id} 不存在"}
        if task.get("phase") not in ("已中断", "重放中"):
            return {"ok": False, "message": f"任务 {task['task_no']} 当前状态为 {task.get('phase')}，无需恢复"}
        records = task.get("records", [])
        start = len(task.get("draft", []))
        task["phase"] = "重放中"
        task["status"] = "重放中"
        task["pending"] = True
        self._mirror_task(task)
        self._write_draft(task, records, task["field"], int(task["position"]), None)
        task["phase"] = "待确认"
        task["status"] = "待确认"
        task["abnormal"] = False
        task.pop("interrupted_at", None)
        task["resumed_at"] = _now_iso()
        self._mirror_task(task)
        return {
            "ok": True,
            "message": f"已补齐 {len(task['draft']) - start} 条复算，仍需确认后下游才可读取",
            "task": self._public_task(task),
        }

    def confirm_replay(self, task_id: int) -> dict[str, Any]:
        """确认重放：原子替换旧组、写入修正清单，解除下游阻断。"""
        task = self._find_task(task_id)
        if task is None:
            return {"ok": False, "message": f"重放任务 {task_id} 不存在"}
        if task.get("phase") != "待确认":
            if task.get("phase") == "已中断" and len(task.get("draft", [])) < len(task.get("records", [])):
                return {"ok": False, "message": "草稿未补齐，不能确认"}
            return {"ok": False, "message": f"任务 {task['task_no']} 当前为 {task.get('phase')}，不能确认"}
        if len(task.get("draft", [])) < len(task.get("records", [])):
            return {"ok": False, "message": "草稿未补齐，不能确认"}

        draft = task["draft"]
        field = task["field"]
        # 1. 冻结旧组，原始日志与旧组判定都保留在对照视图里可回溯。
        old_groups = []
        for group in store.rows(GROUP_TABLE):
            if group.get("group_no") in task.get("source_groups", []):
                old_groups.append(group)
                group["state"] = "已被替代"
                group["active"] = False
                group["superseded_by"] = task["task_no"]
                group["superseded_at"] = _now_iso()
                group["pending"] = False

        # 2. 把草稿里的重算字段写回明细，并记下本次采用的字段版本。
        touched: dict[str, dict[str, Any]] = {r["raw_record_id"]: r for r in draft}
        entity_by_raw: dict[str, dict[str, Any]] = {}
        for raw_id, record in touched.items():
            entity = store.find("parsed_entity", int(record["entity_id"]))
            if entity is None:
                continue
            entity["标准化字段集"] = json.dumps(record["fields"], ensure_ascii=False)
            entity["field_versions"] = {
                **(entity.get("field_versions") or {}),
                field: record["field_version"],
            }
            entity["解析规则版本"] = task["new_rule_version"]
            entity["parser_rule"] = task["new_rule_version"]
            entity_by_raw[raw_id] = entity

        # 3. 按新键重新成组（被替代组中的旧实体退出）。
        new_groups = self._regroup(
            old_groups, entity_by_raw, field, task
        )
        # 4. 修正清单：每条原始日志 + 受影响的判定。
        group_by_entity = {
            int(entity_id): group
            for group in new_groups
            for entity_id in group["member_entity_ids"]
        }
        task["corrections"] = []
        for item in draft:
            entity = entity_by_raw.get(item["raw_record_id"])
            new_group = group_by_entity.get(int(entity["id"])) if entity is not None else None
            winner = new_group["winning_entity_id"] if new_group else None
            task["corrections"].append({
                "raw_record_id": item["raw_record_id"],
                "source_group_no": item["source_group_no"],
                "batch_no": item["batch_no"],
                "line_no": item["line_no"],
                "raw_body": item["raw_body"],
                "old_key": item["old_key"],
                "new_key": item["new_key"],
                "old_verdict": "胜出" if item["was_winner"] else "淘汰",
                "new_verdict": (
                    "胜出" if entity is not None and winner == int(entity["id"])
                    else ("淘汰" if new_group else "撤组")
                ),
                "new_group_no": new_group["group_no"] if new_group else "",
                "cross_group": item["cross_group"],
                "affected_verdict": (
                    "胜出→淘汰" if item["was_winner"] and entity is not None and winner != int(entity["id"])
                    else "淘汰→胜出" if (not item["was_winner"] and entity is not None and winner == int(entity["id"]))
                    else "保持"
                ),
            })

        # 5. 一次性提交：任务转为已确认，组状态同步。
        task["phase"] = "已确认"
        task["status"] = "已完成"
        task["pending"] = False
        task["abnormal"] = False
        task["产出记录数"] = str(len(draft))
        task["重放状态"] = "已完成"
        task["confirmed_at"] = _now_iso()
        task["new_groups"] = [g["group_no"] for g in new_groups]
        self._mirror_task(task)
        return {
            "ok": True,
            "message": f"重放 {task['task_no']} 已确认，修正清单 {len(task['corrections'])} 条",
            "task": self._public_task(task),
            "corrections": task["corrections"],
            "winner_groups": [_public_group(g) for g in new_groups],
        }

    def _regroup(
        self,
        old_groups: list[dict[str, Any]],
        entity_by_raw: dict[str, dict[str, Any]],
        field: str,
        task: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """用重算后的字段为受影响的实体重新成组，跨组合并时保留原保留策略。"""
        affected_ids = {int(e["id"]) for e in entity_by_raw.values()}
        key_fields = [f for f in str(old_groups[0].get("key_fields", "")).split(",") if f] if old_groups else DEMO_KEY_FIELDS
        retain = old_groups[0].get("retain_strategy", "keep_first") if old_groups else "keep_first"
        old_ids = {int(g["id"]) for g in old_groups}
        buckets: dict[str, list[dict[str, Any]]] = {}
        for entity in store.rows("parsed_entity"):
            eid = int(entity.get("id", 0))
            if eid in affected_ids or int(entity.get("dedup_group_id") or 0) in old_ids:
                key = _make_key(_json_fields(entity), key_fields)
                buckets.setdefault(key, []).append(entity)
        # 把没被本次影响、但新键与草稿重合的组外实体一并拉进来（跨组重合成组）。
        new_keys = set(buckets)
        for entity in store.rows("parsed_entity"):
            if int(entity.get("id", 0)) in affected_ids:
                continue
            gid = int(entity.get("dedup_group_id") or 0)
            if not gid or gid in old_ids:
                continue
            group = store.find(GROUP_TABLE, gid)
            if group is None or not group.get("active", True):
                continue
            key = _make_key(_json_fields(entity), key_fields)
            if key in new_keys:
                buckets.setdefault(key, []).append(entity)

        field_versions = {}
        for group in old_groups:
            for name, version in (group.get("field_versions") or {}).items():
                field_versions[name] = dict(version)
        field_versions[field] = {
            "field": field,
            "source_position": int(task["position"]),
            "key_field": field in key_fields,
            "parser_rule": task["new_rule_version"],
            "parsed_at": _now_iso(),
        }

        created: list[dict[str, Any]] = []
        for key, members in buckets.items():
            members.sort(key=lambda e: int(e["id"]))
            winner = members[0] if retain == "keep_first" else members[-1]
            for entity in members:
                entity["dedup_id"] = str(winner["id"]) if entity is not winner else ""
                entity["field_versions"] = field_versions
            group = self._persist_group(
                key_fields=key_fields,
                key_value=key,
                retain_strategy=retain,
                batch_no=members[0].get("来源批次", ""),
                members=members,
                winner_id=int(winner["id"]),
                replay_id=int(task["id"]),
                active=True,
                state="已确认",
                field_versions=field_versions,
                parser_rule=task["new_rule_version"],
            )
            for entity in members:
                entity["dedup_group_id"] = group["id"]
            created.append(group)
        return created

    # ---------- 重放任务查询 ----------
    def list_replays(self, phase: str = "") -> list[dict[str, Any]]:
        items = [
            self._public_task(task)
            for task in store.rows(CORRECTION_TABLE)
            if not phase or task.get("phase") == phase
        ]
        return sorted(items, key=lambda t: int(t["id"]), reverse=True)

    def get_replay(self, task_id: int) -> dict[str, Any] | None:
        task = self._find_task(task_id)
        return self._public_task(task) if task else None

    def _find_task(self, task_id: int) -> dict[str, Any] | None:
        for row in store.rows(CORRECTION_TABLE):
            if int(row.get("id", 0)) == task_id:
                return row
        return None

    def _public_task(self, task: dict[str, Any]) -> dict[str, Any]:
        data = {k: v for k, v in task.items() if not k.startswith("_")}
        data["downstream_blocked"] = task.get("phase") in PENDING_PHASES
        return data

    def _mirror_task(self, task: dict[str, Any]) -> None:
        """把重放任务映射到回溯重放引擎列表，便于在既有模块看到进度。"""
        target = None
        for row in store.rows("replay_engine"):
            if row.get("correction_task_no") == task["task_no"]:
                target = row
                break
        if target is None:
            target = {
                "id": _next_id("replay_engine"),
                "correction_task_no": task["task_no"],
                "历史批次范围": task.get("历史批次范围", ""),
                "新解析规则版本": task.get("新解析规则版本", ""),
                "重放状态": task.get("phase", "重放中"),
                "产出记录数": str(task.get("产出记录数", "0")),
                "执行时间": task.get("执行时间", _now_iso()),
                "status": task.get("status", "重放中"),
                "pending": True,
                "abnormal": False,
            }
            store.rows("replay_engine").append(target)
        target["重放状态"] = task.get("phase", target.get("重放状态", ""))
        target["产出记录数"] = str(task.get("产出记录数", target.get("产出记录数", "0")))
        target["status"] = task.get("status", target.get("status", "重放中"))
        target["pending"] = task.get("pending", True)
        target["abnormal"] = task.get("abnormal", False)


dedup_groups = DedupGroupService()



