"""
EvolveEngine — AlphaEvolve-inspired evolutionary model improvement.

Model-agnostic. Gemini-powered. Works on any model/framework.

Quick start:
    from evolve import EvolveEngine, EvolveConfig

    engine = EvolveEngine(
        config=EvolveConfig(target_metric="accuracy"),
        seed_code=my_code_with_evolve_blocks,
        eval_fn=my_evaluation_function,  # str -> dict
        system_prompt="Improve this classifier...",
    )
    summary = asyncio.run(engine.run())

Built-in example:
    python -m evolve.run --example stock --iterations 50
"""

from .config import EvolveConfig
from .engine import EvolveEngine
from .evaluator import EvaluationResult, run_candidate_eval
from .population import PopulationDatabase, Candidate
from .seed_program import (
    extract_evolution_blocks,
    replace_evolution_block,
    list_evolution_blocks,
    detect_code_features,
)
