"""请求/响应数据模型（Pydantic v2）。"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class AidIn(BaseModel):
    """航标灯质特征：编号、周期（秒）、周期内闪光时刻（秒，[0, period)）。"""

    aid_id: str = Field(min_length=1, max_length=64, description="航标编号")
    period: float = Field(gt=0, description="灯质周期，秒")
    flashes: list[float] = Field(min_length=1, description="周期内闪光时刻，秒")

    @field_validator("flashes")
    @classmethod
    def _flashes_finite(cls, v: list[float]) -> list[float]:
        for f in v:
            if f != f or f in (float("inf"), float("-inf")):
                raise ValueError("flash offset must be a finite number")
        return v

    @model_validator(mode="after")
    def _offsets_in_range(self) -> "AidIn":
        for f in self.flashes:
            if not 0.0 <= f < self.period:
                raise ValueError(
                    f"flash offset {f} out of range [0, period={self.period})"
                )
        self.flashes = sorted(set(self.flashes))
        return self


class LibraryImportRequest(BaseModel):
    """导入 JSON 特征库。mode=merge 按编号 upsert；mode=replace 清空后导入。

    兼容旧接口：服务端基于当前激活版本原子创建并直接激活新版本。
    """

    aids: list[AidIn] = Field(min_length=1)
    mode: Literal["merge", "replace"] = "merge"


class VersionCreateRequest(BaseModel):
    """创建不可变版本草稿：提交完整特征库与基线版本号。

    草稿保存时即按 aid_id 计算相对 base_version 的增/删/改；激活时还会
    在单个事务内再次确认 base_version 仍是当前激活版本，否则返回 409。
    """

    aids: list[AidIn] = Field(min_length=1)
    base_version: int = Field(
        ge=1, description="基线版本号（当前激活版本或历史版本，不能是草稿）"
    )

    @field_validator("aids")
    @classmethod
    def _unique_aid_ids(cls, v: list[AidIn]) -> list[AidIn]:
        ids = [a.aid_id for a in v]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate aid_id in version aids")
        return v


class JudgeRequest(BaseModel):
    """判定请求：观测时间戳 + 判定参数，可内联特征库覆盖已存库。"""

    observations: list[float] = Field(default_factory=list, description="闪光观测时间戳（Unix 秒）")
    tolerance: float = Field(gt=0, description="时间容差，秒")
    max_drift: float = Field(ge=0, lt=1, description="最大线性钟漂（无量纲，s/s）")
    max_missed: int = Field(ge=0, description="漏记上限")
    max_false: int = Field(ge=0, description="误记上限")
    library: Optional[list[AidIn]] = Field(
        default=None,
        description="内联特征库；缺省时在请求开始固定当前激活版本，整个判定只读该快照",
    )

    @field_validator("observations")
    @classmethod
    def _obs_finite(cls, v: list[float]) -> list[float]:
        for t in v:
            if t != t or t in (float("inf"), float("-inf")):
                raise ValueError("observation timestamps must be finite numbers")
        return v


class PatrolWaypointIn(BaseModel):
    """计划航线航点：航点编号 + 该处航标编号（须存在于激活特征库版本）。"""

    waypoint_id: str = Field(min_length=1, max_length=64, description="航点编号（请求内唯一）")
    aid_id: str = Field(min_length=1, max_length=64, description="航标编号")


class PatrolWindowIn(BaseModel):
    """一个观测窗口：按采集顺序排列的闪光观测时间戳。"""

    observations: list[float] = Field(default_factory=list, description="闪光观测时间戳（Unix 秒）")

    @field_validator("observations")
    @classmethod
    def _obs_finite(cls, v: list[float]) -> list[float]:
        for t in v:
            if t != t or t in (float("inf"), float("-inf")):
                raise ValueError("observation timestamps must be finite numbers")
        return v


class PatrolAnalyzeRequest(BaseModel):
    """巡检整体核对请求：计划航线 + 观测窗口序列 + 与 /judge 相同的判定参数。"""

    waypoints: list[PatrolWaypointIn] = Field(
        max_length=50, description="计划航线航点（按航线顺序，waypoint_id 唯一）"
    )
    windows: list[PatrolWindowIn] = Field(
        max_length=20, description="观测窗口（按采集顺序）"
    )
    tolerance: float = Field(gt=0, description="时间容差，秒")
    max_drift: float = Field(ge=0, lt=1, description="最大线性钟漂（无量纲，s/s）")
    max_missed: int = Field(ge=0, description="漏记上限")
    max_false: int = Field(ge=0, description="误记上限")

    @field_validator("waypoints")
    @classmethod
    def _unique_waypoint_ids(cls, v: list[PatrolWaypointIn]) -> list[PatrolWaypointIn]:
        ids = [w.waypoint_id for w in v]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate waypoint_id in waypoints")
        return v
