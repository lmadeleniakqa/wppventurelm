"""
EvolveEngine — model-agnostic evolutionary loop powered by Gemini.

Orchestrates: prompt sampling → Gemini generation → evaluation → selection.
Works with any model/framework — user provides seed code and eval function.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from .config import EvolveConfig
from .population import PopulationDatabase, Candidate
from .seed_program import detect_code_features
from .prompt_sampler import (
    build_evolution_prompt,
    build_meta_prompt_evolution_prompt,
    parse_candidate_response,
    parse_meta_prompt_response,
)
from .evaluator import run_candidate_eval

logger = logging.getLogger("evolve_engine")


class EvolveEngine:
    """
    Model-agnostic evolutionary engine using Gemini (Pro + Flash).

    Parameters
    ----------
    config : EvolveConfig
        Experiment configuration.
    seed_code : str
        Initial program with EVOLVE-BLOCK markers.
    eval_fn : callable
        Function that takes candidate_code (str) and returns dict of metrics.
        Must include config.target_metric as a key.
    system_prompt : str
        Domain-specific guidance for the evolutionary search.
    gemini_client : optional
        Pre-configured google.generativeai module. If None, uses mock LLM.
    """

    def __init__(
        self,
        config: EvolveConfig,
        seed_code: str,
        eval_fn: Callable[[str], dict],
        system_prompt: str,
        gemini_client=None,
    ):
        self.config = config
        self.seed_code = seed_code
        self.eval_fn = eval_fn
        self.system_prompt = system_prompt
        self.original_system_prompt = system_prompt
        self.gemini = gemini_client
        self.population = PopulationDatabase(
            num_islands=config.num_islands,
            max_per_island=config.population_size // config.num_islands,
        )
        self.iteration = 0
        self.stagnation_counter = 0
        self.start_time = None

    # ----- LLM calls (Gemini only) -----

    async def _call_llm(self, prompt: str, use_primary: bool = False) -> str:
        if self.gemini:
            return await self._call_gemini(prompt, use_primary)
        return self._mock_mutate()

    async def _call_gemini(self, prompt: str, use_primary: bool) -> str:
        model_name = (
            self.config.llm_model_primary if use_primary
            else self.config.llm_model_secondary
        )
        gen_model = self.gemini.GenerativeModel(model_name)

        # Relax safety settings for code generation
        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ]

        response = await asyncio.to_thread(
            gen_model.generate_content,
            prompt,
            generation_config=self.gemini.types.GenerationConfig(
                temperature=self.config.temperature,
                max_output_tokens=self.config.max_tokens,
            ),
            safety_settings=safety_settings,
        )
        # Handle safety-filtered or empty responses
        try:
            return response.text
        except (ValueError, AttributeError):
            if response.candidates:
                for candidate in response.candidates:
                    if candidate.content and candidate.content.parts:
                        return candidate.content.parts[0].text
            logger.debug("Gemini response empty, using mock mutation")
            return self._mock_mutate()

    def _mock_mutate(self) -> str:
        """Random structural mutations for testing without API access."""
        from .seed_program import extract_evolution_blocks, replace_evolution_block
        blocks = extract_evolution_blocks(self.seed_code)
        if not blocks:
            return self.seed_code

        # Pick a random block and make small textual mutations
        block_name = random.choice(list(blocks.keys()))
        code = blocks[block_name]
        lines = code.split("\n")

        mutations = []
        for line in lines:
            # Randomly tweak numeric literals
            tokens = line.split()
            new_tokens = []
            for tok in tokens:
                try:
                    val = float(tok)
                    if random.random() < 0.3:
                        factor = random.uniform(0.5, 2.0)
                        val = round(val * factor, 6)
                    new_tokens.append(str(val) if "." in tok else str(int(val)))
                except ValueError:
                    # Randomly swap activations
                    if tok in ("ReLU()", "GELU()", "SiLU()", "Mish()") and random.random() < 0.2:
                        tok = random.choice(["ReLU()", "GELU()", "SiLU()", "Mish()"])
                    new_tokens.append(tok)
            mutations.append(" ".join(new_tokens))

        mutated = "\n".join(mutations)
        return f"# EVOLVE-BLOCK-START: {block_name}\n{mutated}\n# EVOLVE-BLOCK-END: {block_name}"

    # ----- Candidate lifecycle -----

    async def _generate_candidate(self, island_id: int) -> Candidate:
        use_primary = random.random() < self.config.primary_ratio
        prompt = build_evolution_prompt(
            self.population, island_id, self.system_prompt,
            self.seed_code, self.iteration,
        )

        try:
            response = await self._call_llm(prompt, use_primary)
            code = parse_candidate_response(response, self.seed_code)
        except Exception as e:
            logger.warning(f"LLM call failed: {e}")
            code = self.seed_code

        program_id = hashlib.md5(
            f"{self.iteration}_{island_id}_{time.time()}".encode()
        ).hexdigest()[:12]

        return Candidate(
            program_id=f"prog_{program_id}",
            iteration=self.iteration,
            code=code,
            island_id=island_id,
            generation=self.iteration,
            architectural_features=detect_code_features(code),
        )

    def _evaluate_candidate_sync(self, candidate: Candidate) -> Candidate:
        result = run_candidate_eval(
            candidate.code,
            self.eval_fn,
            target_metric=self.config.target_metric,
            max_time_seconds=self.config.max_eval_time_seconds,
            default_fail_score=self.config.default_fail_score,
        )
        candidate.score = result.score
        candidate.metrics = result.metrics
        candidate.failed = result.failed
        candidate.error_message = result.error_message
        candidate.eval_time_seconds = result.eval_time
        return candidate

    async def _evolve_meta_prompt(self):
        if not self.config.evolve_system_prompt:
            return
        prompt = build_meta_prompt_evolution_prompt(
            self.system_prompt, self.population, self.iteration
        )
        try:
            response = await self._call_llm(prompt, use_primary=True)
            new_prompt = parse_meta_prompt_response(response)
            if new_prompt and len(new_prompt) > 100:
                self.system_prompt = new_prompt
                logger.info("Meta-prompt evolved successfully")
        except Exception as e:
            logger.warning(f"Meta-prompt evolution failed: {e}")

    # ----- Main loop -----

    async def run(self) -> dict:
        """
        Run the full evolutionary loop.
        Returns summary dict with best candidate, scores, and trajectory.
        """
        self.start_time = time.time()
        output_dir = self.config.output_dir
        os.makedirs(output_dir, exist_ok=True)

        logger.info(
            f"Starting EvolveEngine: {self.config.max_iterations} iters, "
            f"{self.config.concurrent_evaluations} concurrent evals, "
            f"Gemini {'connected' if self.gemini else 'mock'}"
        )

        # Evaluate seed first
        seed_candidate = Candidate(
            program_id="seed_000", iteration=0, code=self.seed_code,
            island_id=0, generation=0,
            architectural_features=detect_code_features(self.seed_code),
        )
        seed_candidate = self._evaluate_candidate_sync(seed_candidate)
        self.population.add_candidate(seed_candidate)
        logger.info(f"Seed score: {seed_candidate.score:.4f}")
        logger.info(f"Seed metrics: {json.dumps(seed_candidate.metrics, indent=2, default=str)}")

        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=self.config.concurrent_evaluations)

        for iteration in range(1, self.config.max_iterations + 1):
            self.iteration = iteration

            # Generate candidates
            gen_tasks = [
                self._generate_candidate(
                    (iteration * self.config.concurrent_evaluations + s) % self.config.num_islands
                )
                for s in range(self.config.concurrent_evaluations)
            ]
            candidates = await asyncio.gather(*gen_tasks)

            # Evaluate concurrently
            eval_futures = [
                loop.run_in_executor(executor, self._evaluate_candidate_sync, c)
                for c in candidates
            ]
            evaluated = await asyncio.gather(*eval_futures, return_exceptions=True)

            # Register results
            new_best = False
            for result in evaluated:
                if isinstance(result, Exception):
                    logger.warning(f"Eval exception: {result}")
                    continue
                if self.population.add_candidate(result):
                    new_best = True
                    self.stagnation_counter = 0

            if not new_best:
                self.stagnation_counter += 1

            # Migration
            if iteration % self.config.migration_interval == 0:
                self.population.migrate(self.config.migration_rate)

            # Meta-prompt
            if (self.config.evolve_system_prompt
                    and iteration % self.config.meta_prompt_interval == 0):
                await self._evolve_meta_prompt()

            # Logging
            if iteration % self.config.log_interval == 0:
                stats = self.population.get_stats()
                elapsed = time.time() - self.start_time
                logger.info(
                    f"[Iter {iteration}/{self.config.max_iterations}] "
                    f"Best: {stats['best_score']:.4f} | "
                    f"Top 5: {stats['top_5_scores']} | "
                    f"Evaluated: {stats['total_candidates']} | "
                    f"Failed: {stats['failed_candidates']} | "
                    f"Stagnation: {self.stagnation_counter} | "
                    f"Time: {elapsed:.0f}s"
                )

            # Checkpoint
            if iteration % 25 == 0:
                self.population.save(f"{output_dir}/checkpoint_iter_{iteration}.json")

            # Stopping criteria
            if (self.config.target_score_threshold is not None
                    and self.population.best_score >= self.config.target_score_threshold):
                logger.info(f"Target score {self.config.target_score_threshold} reached!")
                break

            if self.stagnation_counter >= self.config.stagnation_limit:
                logger.info(f"Stagnation limit ({self.config.stagnation_limit}) reached.")
                break

        executor.shutdown(wait=False)

        # Save results
        self.population.save(f"{output_dir}/final_population.json")
        self._save_best_program(output_dir)
        summary = self._build_summary()
        with open(f"{output_dir}/summary.json", "w") as f:
            json.dump(summary, f, indent=2, default=str)

        logger.info(f"\nEvolution complete! Best: {self.population.best_score:.4f}")
        return summary

    def _save_best_program(self, output_dir: str):
        if self.population.best_candidate:
            with open(f"{output_dir}/best_program.py", "w") as f:
                f.write(self.population.best_candidate.code)
            for i, c in enumerate(self.population.get_top_k(self.config.save_top_k)):
                with open(f"{output_dir}/top_{i+1}_program.py", "w") as f:
                    f.write(c.code)

    def _build_summary(self) -> dict:
        stats = self.population.get_stats()
        best = self.population.best_candidate
        return {
            "experiment_id": self.config.experiment_id,
            "session_id": self.config.session_id,
            "iterations_completed": self.iteration,
            "total_time_seconds": time.time() - self.start_time if self.start_time else 0,
            "best_score": self.population.best_score,
            "best_program_id": best.program_id if best else None,
            "best_metrics": best.metrics if best else {},
            "best_features": best.architectural_features if best else {},
            "score_trajectory": self.population.score_history,
            "population_stats": stats,
            "config": {
                "max_iterations": self.config.max_iterations,
                "population_size": self.config.population_size,
                "target_metric": self.config.target_metric,
                "gemini_primary": self.config.llm_model_primary,
                "gemini_secondary": self.config.llm_model_secondary,
            },
            "system_prompt_evolved": self.system_prompt != self.original_system_prompt,
        }
