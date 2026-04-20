#!/usr/bin/env python3
"""
Entry point for EvolveEngine experiments.

Usage:
    # Run stock predictor example with mock LLM (no API key needed)
    python -m evolve.run --example stock --iterations 20

    # Run with Gemini (production)
    GOOGLE_API_KEY=... python -m evolve.run --example stock --iterations 200

    # Run with custom seed program + eval function
    python -m evolve.run --seed my_model.py --eval my_eval.py --prompt my_prompt.txt

    # Resume from previous experiment
    python -m evolve.run --example stock --seed-from evolve_results/final_population.json
"""

import argparse
import asyncio
import json
import logging
import os
import sys

from .config import EvolveConfig
from .engine import EvolveEngine


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("evolve")


def create_gemini_client():
    """Create Gemini client from GOOGLE_API_KEY env var."""
    try:
        import google.generativeai as genai
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            logger.warning("GOOGLE_API_KEY not set. Using mock LLM.")
            return None
        genai.configure(api_key=api_key)
        return genai
    except ImportError:
        logger.warning("google-generativeai not installed. Using mock LLM. "
                        "Install with: pip install google-generativeai")
        return None


def load_custom_eval(eval_path: str):
    """Load a user-provided eval function from a Python file.

    The file must define an `evaluate(candidate_code: str) -> dict` function.
    """
    import importlib.util
    spec = importlib.util.spec_from_file_location("user_eval", eval_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "evaluate"):
        raise ValueError(f"{eval_path} must define an `evaluate(candidate_code: str) -> dict` function")
    return mod.evaluate


def parse_args():
    parser = argparse.ArgumentParser(description="EvolveEngine — Evolutionary Model Improvement (Gemini)")

    # Mode: built-in example or custom
    parser.add_argument("--example", choices=["stock", "xgb_wpp"], default=None,
                        help="Run a built-in example (stock=PyTorch, xgb_wpp=XGBoost WPP)")
    parser.add_argument("--seed", type=str, default=None,
                        help="Path to seed program file (.py)")
    parser.add_argument("--eval", type=str, default=None,
                        help="Path to eval function file (.py with evaluate() function)")
    parser.add_argument("--prompt", type=str, default=None,
                        help="Path to system prompt file (.txt)")

    # Evolution parameters
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--concurrent", type=int, default=4)
    parser.add_argument("--population", type=int, default=50)
    parser.add_argument("--islands", type=int, default=5)
    parser.add_argument("--target-metric", default=None,
                        help="Primary metric key to maximize (default depends on example)")
    parser.add_argument("--output-dir", default="evolve_results")
    parser.add_argument("--experiment-id", default="exp_001")
    parser.add_argument("--seed-from", type=str, default=None,
                        help="Previous population JSON to seed from")
    parser.add_argument("--device", default="auto", choices=["auto", "cuda", "cpu"])
    parser.add_argument("--no-meta-prompt", action="store_true")

    # Example-specific
    parser.add_argument("--difficulty", choices=["easy", "medium", "hard"], default="medium",
                        help="Synthetic data difficulty (stock example only)")
    return parser.parse_args()


async def main():
    args = parse_args()

    # Determine mode
    if args.example == "stock":
        from .examples.stock_predictor import SEED_CODE, SYSTEM_PROMPT, make_eval_fn
        seed_code = SEED_CODE
        system_prompt = SYSTEM_PROMPT
        eval_fn = make_eval_fn(difficulty=args.difficulty)
        target_metric = args.target_metric or "macro_f1"

    elif args.example == "xgb_wpp":
        from .examples.xgb_wpp_daily import SEED_CODE, SYSTEM_PROMPT, make_eval_fn
        seed_code = SEED_CODE
        system_prompt = SYSTEM_PROMPT
        eval_fn = make_eval_fn(difficulty=args.difficulty)
        target_metric = args.target_metric or "composite"

    elif args.seed and args.eval:
        with open(args.seed) as f:
            seed_code = f.read()
        eval_fn = load_custom_eval(args.eval)
        system_prompt = ""
        if args.prompt:
            with open(args.prompt) as f:
                system_prompt = f.read()
        target_metric = args.target_metric or "score"

    else:
        print("Specify --example stock  OR  --seed <file> --eval <file>")
        print("Run with --help for full options.")
        sys.exit(1)

    # Config
    config = EvolveConfig(
        experiment_id=args.experiment_id,
        max_iterations=args.iterations,
        population_size=args.population,
        num_islands=args.islands,
        concurrent_evaluations=args.concurrent,
        target_metric=target_metric,
        output_dir=args.output_dir,
        device=args.device,
        evolve_system_prompt=not args.no_meta_prompt,
    )

    # Gemini client
    gemini = create_gemini_client()

    # Engine
    engine = EvolveEngine(
        config=config,
        seed_code=seed_code,
        eval_fn=eval_fn,
        system_prompt=system_prompt,
        gemini_client=gemini,
    )

    # Seed from previous experiment
    if args.seed_from:
        prev = engine.population.load_top_candidates(args.seed_from)
        for c in prev[:5]:
            engine.population.add_candidate(c)
        logger.info(f"Seeded from {args.seed_from}: {len(prev)} candidates")

    # Run
    summary = await engine.run()

    # Print summary
    print("\n" + "=" * 70)
    print("EVOLUTION COMPLETE")
    print("=" * 70)
    print(f"Experiment:      {summary['experiment_id']}")
    print(f"Iterations:      {summary['iterations_completed']}")
    print(f"Total time:      {summary['total_time_seconds']:.0f}s")
    print(f"Best score:      {summary['best_score']:.4f}")
    print(f"Best program:    {summary['best_program_id']}")
    print(f"Meta-prompt:     {'evolved' if summary['system_prompt_evolved'] else 'unchanged'}")
    print(f"\nBest metrics:")
    for k, v in summary.get("best_metrics", {}).items():
        if isinstance(v, float):
            print(f"  {k:20s}: {v:.4f}")
        else:
            print(f"  {k:20s}: {v}")
    print(f"\nResults saved to: {args.output_dir}/")
    print("=" * 70)

    return summary


if __name__ == "__main__":
    asyncio.run(main())
