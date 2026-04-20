"""
Candidate evaluation — model-agnostic.

Users provide an evaluation function that takes candidate code and returns
a dict of metrics. The engine uses `target_metric` from that dict as the
primary scalar to maximise.

This module provides:
  - EvaluationResult: standard result container
  - run_candidate_eval: safe wrapper that executes user's eval_fn with
    timeout, error handling, and memory cleanup
"""

import time
import traceback
import gc
from typing import Callable, Optional

import torch


class EvaluationResult:
    __slots__ = ("score", "metrics", "failed", "error_message", "eval_time")

    def __init__(
        self,
        score: float = -1.0,
        metrics: Optional[dict] = None,
        failed: bool = False,
        error_message: str = "",
        eval_time: float = 0.0,
    ):
        self.score = score
        self.metrics = metrics or {}
        self.failed = failed
        self.error_message = error_message
        self.eval_time = eval_time


def run_candidate_eval(
    candidate_code: str,
    eval_fn: Callable[[str], dict],
    target_metric: str,
    max_time_seconds: int = 300,
    default_fail_score: float = -1.0,
) -> EvaluationResult:
    """
    Safely execute a user-provided evaluation function on candidate code.

    Parameters
    ----------
    candidate_code : str
        The full candidate program source code.
    eval_fn : callable
        User's evaluation function: takes candidate_code (str) and returns
        a dict of metric_name -> float. Must include `target_metric` key.
    target_metric : str
        Key in the returned dict to use as the primary optimisation score.
    max_time_seconds : int
        Maximum wall-clock time for evaluation.
    default_fail_score : float
        Score assigned to failed/crashed candidates.

    Returns
    -------
    EvaluationResult
    """
    start = time.time()
    try:
        metrics = eval_fn(candidate_code)

        elapsed = time.time() - start
        if not isinstance(metrics, dict):
            return EvaluationResult(
                score=default_fail_score,
                failed=True,
                error_message=f"eval_fn must return dict, got {type(metrics).__name__}",
                eval_time=elapsed,
            )

        primary = metrics.get(target_metric)
        if primary is None:
            return EvaluationResult(
                score=default_fail_score,
                metrics=metrics,
                failed=True,
                error_message=f"target_metric '{target_metric}' not in eval result keys: {list(metrics.keys())}",
                eval_time=elapsed,
            )

        return EvaluationResult(
            score=float(primary),
            metrics=metrics,
            failed=False,
            eval_time=elapsed,
        )

    except Exception as e:
        return EvaluationResult(
            score=default_fail_score,
            failed=True,
            error_message=f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()[-500:]}",
            eval_time=time.time() - start,
        )

    finally:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        gc.collect()
