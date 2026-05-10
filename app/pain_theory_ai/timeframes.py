from __future__ import annotations

from bisect import bisect_right
from typing import Sequence


EXECUTION_1M_SEC = 60
ANALYSIS_5M_SEC = 300


def closed_htf_cutoff_start_ts(
    execution_start_ts: int,
    *,
    execution_tf_sec: int = EXECUTION_1M_SEC,
    higher_tf_sec: int = ANALYSIS_5M_SEC,
) -> int:
    """Latest higher-timeframe candle start that is known after execution close."""
    return int(execution_start_ts) + int(execution_tf_sec) - int(higher_tf_sec)


def higher_timeframe_is_available(
    higher_start_ts: int,
    execution_start_ts: int,
    *,
    execution_tf_sec: int = EXECUTION_1M_SEC,
    higher_tf_sec: int = ANALYSIS_5M_SEC,
) -> bool:
    return int(higher_start_ts) <= closed_htf_cutoff_start_ts(
        int(execution_start_ts),
        execution_tf_sec=execution_tf_sec,
        higher_tf_sec=higher_tf_sec,
    )


def available_higher_timeframe_end_index(
    higher_start_timestamps: Sequence[int],
    execution_start_ts: int,
    *,
    execution_tf_sec: int = EXECUTION_1M_SEC,
    higher_tf_sec: int = ANALYSIS_5M_SEC,
) -> int:
    cutoff = closed_htf_cutoff_start_ts(
        int(execution_start_ts),
        execution_tf_sec=execution_tf_sec,
        higher_tf_sec=higher_tf_sec,
    )
    return bisect_right(higher_start_timestamps, cutoff)
