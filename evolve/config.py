"""Configuration for EvolveEngine experiments."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EvolveConfig:
    # --- Experiment identity ---
    experiment_id: str = "exp_001"
    session_id: str = "session_001"

    # --- Evolution parameters ---
    max_iterations: int = 200
    population_size: int = 50
    num_islands: int = 5
    concurrent_evaluations: int = 4
    migration_interval: int = 20
    migration_rate: float = 0.1

    # --- Evaluation ---
    target_metric: str = "score"  # Primary scalar to maximise (key in eval result dict)
    default_fail_score: float = -1.0
    max_eval_time_seconds: int = 300

    # --- LLM settings (Gemini only) ---
    llm_model_primary: str = "gemini-2.5-pro"      # High quality, used ~30%
    llm_model_secondary: str = "gemini-2.5-flash"   # Fast throughput, used ~70%
    primary_ratio: float = 0.3
    temperature: float = 0.8
    max_tokens: int = 4096

    # --- Meta-prompt evolution ---
    evolve_system_prompt: bool = True
    meta_prompt_interval: int = 25

    # --- Stopping criteria ---
    target_score_threshold: Optional[float] = None  # Stop if achieved
    stagnation_limit: int = 50  # Stop if no improvement for N iterations

    # --- Infrastructure ---
    device: str = "auto"  # "auto", "cuda", "cpu"
    output_dir: str = "evolve_results"
    save_all_candidates: bool = False
    save_top_k: int = 10
    log_interval: int = 5
