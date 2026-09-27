"""生成示例设备批次请求（某设备的首批观测，previous_digest 为空）。

用法：python3 samples/generate_batch_request.py > /tmp/batch1.json
后续批次的 previous_digest 取上一批响应游标中的 cursor.expected_previous_digest
（摘要由服务端计算，客户端无需自行求 SHA-256）。
"""
import json

batch = {
    "batch_id": "dev01-20260927-0001",
    "sequence": 1,
    "previous_digest": "",
    "observations": [
        {"captured_at": 1_700_000_000.25 + 10.0 * i, "local_id": f"obs-{i:04d}"}
        for i in range(8)
    ],
    # 与 /judge 相同的四项判定阈值：仅随批次保存，上传时不判定
    "tolerance": 0.5,
    "max_drift": 5e-4,
    "max_missed": 3,
    "max_false": 3,
}
print(json.dumps(batch, ensure_ascii=False, indent=2))
