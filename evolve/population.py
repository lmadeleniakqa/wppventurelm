"""
MAP-elites inspired population database with island-based diversity.

Maintains a structured archive of candidate programs, balancing exploration
(diversity across solution space) with exploitation (refining top performers).
"""

import json
import hashlib
import time
import os
from dataclasses import dataclass, field, asdict
from typing import Optional
from collections import defaultdict
import random
import math


@dataclass
class Candidate:
    program_id: str
    iteration: int
    code: str
    score: float = -1.0
    metrics: dict = field(default_factory=dict)
    parent_id: Optional[str] = None
    island_id: int = 0
    generation: int = 0
    created_at: float = field(default_factory=time.time)
    eval_time_seconds: float = 0.0
    failed: bool = False
    error_message: str = ""
    diff_summary: str = ""
    architectural_features: dict = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class PopulationDatabase:
    """
    Island-based MAP-elites population for evolutionary code search.

    Candidates are distributed across islands. Each island maintains its own
    elite archive. Periodic migration shares top performers between islands.
    """

    def __init__(self, num_islands: int = 5, max_per_island: int = 20):
        self.num_islands = num_islands
        self.max_per_island = max_per_island
        self.islands: list[list[Candidate]] = [[] for _ in range(num_islands)]
        self.all_candidates: dict[str, Candidate] = {}
        self.best_candidate: Optional[Candidate] = None
        self.best_score: float = -float("inf")
        self.generation_counter = 0
        self.score_history: list[tuple[int, float]] = []

        # Behavioral diversity bins for MAP-elites
        self.feature_bins: dict[str, list[Candidate]] = defaultdict(list)

    def add_candidate(self, candidate: Candidate) -> bool:
        self.all_candidates[candidate.program_id] = candidate

        if candidate.failed or candidate.score <= -1.0:
            return False

        island = self.islands[candidate.island_id % self.num_islands]
        island.append(candidate)

        # Maintain island size limit — evict weakest
        if len(island) > self.max_per_island:
            island.sort(key=lambda c: c.score, reverse=True)
            island.pop()

        # Update global best
        if candidate.score > self.best_score:
            self.best_score = candidate.score
            self.best_candidate = candidate
            self.score_history.append((candidate.iteration, candidate.score))
            return True

        # Update behavioral archive
        feature_key = self._compute_feature_key(candidate)
        bin_list = self.feature_bins[feature_key]
        if not bin_list or candidate.score > bin_list[0].score:
            self.feature_bins[feature_key] = [candidate]

        return False

    def _compute_feature_key(self, candidate: Candidate) -> str:
        """Discretize architectural features into a MAP-elites bin key."""
        feats = candidate.architectural_features
        parts = []
        for key in sorted(feats.keys()):
            val = feats[key]
            if isinstance(val, (int, float)):
                # Bin numeric values into 5 buckets
                bucket = min(4, int(val * 5) if 0 <= val <= 1 else hash(str(val)) % 5)
                parts.append(f"{key}:{bucket}")
            else:
                parts.append(f"{key}:{val}")
        return "|".join(parts) if parts else "default"

    def sample_parents(self, island_id: int, n: int = 3) -> list[Candidate]:
        """
        Sample parents for the next generation from a given island.
        Uses tournament selection biased toward higher scores but maintaining diversity.
        """
        island = self.islands[island_id % self.num_islands]
        if not island:
            # Fall back to any island with candidates
            for isl in self.islands:
                if isl:
                    island = isl
                    break
        if not island:
            return []

        selected = []
        for _ in range(n):
            # Tournament selection: pick 3, take best
            tournament_size = min(3, len(island))
            tournament = random.sample(island, tournament_size)
            winner = max(tournament, key=lambda c: c.score)
            selected.append(winner)

        return selected

    def sample_diverse(self, n: int = 5) -> list[Candidate]:
        """Sample candidates from different behavioral bins for diversity."""
        bins = list(self.feature_bins.values())
        if not bins:
            return self.get_top_k(n)

        random.shuffle(bins)
        result = []
        for bin_list in bins[:n]:
            if bin_list:
                result.append(bin_list[0])
        return result

    def migrate(self, rate: float = 0.1):
        """Share top performers between islands (island model migration)."""
        for i in range(self.num_islands):
            island = self.islands[i]
            if not island:
                continue
            n_migrants = max(1, int(len(island) * rate))
            migrants = sorted(island, key=lambda c: c.score, reverse=True)[:n_migrants]

            # Send to random neighboring island
            target = (i + random.randint(1, self.num_islands - 1)) % self.num_islands
            for m in migrants:
                clone = Candidate(
                    program_id=f"{m.program_id}_mig_{target}",
                    iteration=m.iteration,
                    code=m.code,
                    score=m.score,
                    metrics=m.metrics.copy(),
                    parent_id=m.program_id,
                    island_id=target,
                    generation=m.generation,
                    architectural_features=m.architectural_features.copy(),
                )
                self.islands[target].append(clone)

            # Trim target island
            target_island = self.islands[target]
            if len(target_island) > self.max_per_island:
                target_island.sort(key=lambda c: c.score, reverse=True)
                self.islands[target] = target_island[: self.max_per_island]

    def get_top_k(self, k: int = 10) -> list[Candidate]:
        """Get the k highest-scoring candidates across all islands."""
        all_scored = [c for c in self.all_candidates.values() if not c.failed and c.score > -1]
        all_scored.sort(key=lambda c: c.score, reverse=True)
        return all_scored[:k]

    def get_stats(self) -> dict:
        total = len(self.all_candidates)
        failed = sum(1 for c in self.all_candidates.values() if c.failed)
        island_sizes = [len(isl) for isl in self.islands]
        top = self.get_top_k(5)

        return {
            "total_candidates": total,
            "failed_candidates": failed,
            "success_rate": f"{(total - failed) / total * 100:.1f}%" if total > 0 else "N/A",
            "best_score": round(self.best_score, 6) if self.best_candidate else None,
            "best_program_id": self.best_candidate.program_id if self.best_candidate else None,
            "island_sizes": island_sizes,
            "unique_bins": len(self.feature_bins),
            "top_5_scores": [round(c.score, 4) for c in top],
        }

    def save(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        data = {
            "best_score": self.best_score,
            "best_program_id": self.best_candidate.program_id if self.best_candidate else None,
            "score_history": self.score_history,
            "stats": self.get_stats(),
            "top_candidates": [c.to_dict() for c in self.get_top_k(20)],
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def load_top_candidates(self, path: str) -> list[Candidate]:
        """Load candidates from a previous experiment to seed a new one."""
        with open(path) as f:
            data = json.load(f)
        return [Candidate.from_dict(d) for d in data.get("top_candidates", [])]
