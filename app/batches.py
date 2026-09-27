"""现场设备观测批次链：规范化载荷与 SHA-256 摘要计算。

批次摘要只标识本批内容：规范化载荷包含设备编号与全部业务字段，
排除摘要字段（``previous_digest`` 与 ``digest``）；链式完整性由服务端在
追加事务内校验 ``previous_digest`` 是否等于当前链头摘要来保证。
客户端无需自行计算摘要：上一批响应游标中的 ``expected_previous_digest``
即下一批应填写的 ``previous_digest``。
"""
from __future__ import annotations

import hashlib
import json

#: 计算批次摘要时排除的字段（链式引用与摘要自身不属于本批内容）
DIGEST_EXCLUDED_FIELDS = ("previous_digest", "digest")


def canonical_payload(device_id: str, req) -> dict:
    """构造批次的规范化载荷：设备编号 + 全部业务字段，排除摘要字段。"""
    return {
        "device_id": device_id,
        "batch_id": req.batch_id,
        "sequence": req.sequence,
        "observations": [
            {"captured_at": o.captured_at, "local_id": o.local_id}
            for o in req.observations
        ],
        "tolerance": req.tolerance,
        "max_drift": req.max_drift,
        "max_missed": req.max_missed,
        "max_false": req.max_false,
    }


def canonical_json(payload: dict) -> str:
    """规范化 JSON：对象键按字典序、紧凑分隔符（无空白）、非 ASCII 字符不转义。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_digest(payload: dict) -> str:
    """对规范化载荷的 UTF-8 编码计算 SHA-256，返回小写十六进制字符串。"""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
