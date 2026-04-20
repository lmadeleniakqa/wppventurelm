"""
Prompt sampler for the evolutionary loop — model-agnostic.

Constructs rich prompts from the population database, seed program,
and system instructions to guide Gemini-based code generation.
"""

import random
from typing import Optional

from .population import Candidate, PopulationDatabase
from .seed_program import extract_evolution_blocks, list_evolution_blocks


def build_evolution_prompt(
    population: PopulationDatabase,
    island_id: int,
    system_prompt: str,
    seed_code: str,
    iteration: int,
    target_blocks: Optional[list[str]] = None,
) -> str:
    """
    Build a prompt for Gemini to generate a new candidate program.

    Uses programs from the database alongside the seed code and system
    instructions to produce targeted modifications.
    """
    parents = population.sample_parents(island_id, n=3)
    diverse_samples = population.sample_diverse(n=2)
    best = population.best_candidate

    # Discover available blocks from the seed code
    all_blocks = list_evolution_blocks(seed_code)
    if not all_blocks:
        all_blocks = ["FULL_PROGRAM"]

    if target_blocks:
        blocks_to_evolve = [b for b in target_blocks if b in all_blocks] or all_blocks[:1]
    else:
        n_blocks = random.choices(
            list(range(1, len(all_blocks) + 1)),
            weights=[1.0 / i for i in range(1, len(all_blocks) + 1)],
        )[0]
        blocks_to_evolve = random.sample(all_blocks, min(n_blocks, len(all_blocks)))

    parts = []

    # Part 1: Task
    parts.append(f"""## Task
You are evolving a program through an evolutionary search.
Generate an improved version by modifying ONLY the evolution blocks listed below.
Return the COMPLETE code for each modified block.

Iteration: {iteration}
Blocks to evolve: {', '.join(blocks_to_evolve)}
Available blocks: {', '.join(all_blocks)}
""")

    # Part 2: User's domain guidance
    parts.append(f"## Guidance\n{system_prompt}\n")

    # Part 3: Current best or seed
    if best and best.score > 0:
        parts.append(f"""## Current Best Program (score: {best.score:.4f})
```python
{best.code}
```
""")
    else:
        parts.append(f"""## Seed Program
```python
{seed_code}
```
""")

    # Part 4: Parent candidates
    if parents:
        parts.append("## Parent Candidates (for reference)")
        for i, parent in enumerate(parents):
            scores_str = ", ".join(
                f"{k}: {v:.4f}" if isinstance(v, (int, float)) else f"{k}: {v}"
                for k, v in parent.metrics.items()
            )
            parts.append(f"\n### Parent {i+1} (score: {parent.score:.4f}, {scores_str})")
            parts.append(f"Diff summary: {parent.diff_summary or 'N/A'}")

    # Part 5: Diverse samples for exploration
    if diverse_samples and iteration > 20:
        parts.append("\n## Diverse Approaches (from other islands)")
        for sample in diverse_samples[:2]:
            feats = sample.architectural_features
            feat_str = ", ".join(f"{k}={v}" for k, v in feats.items() if v)
            parts.append(f"- Score {sample.score:.4f}: {feat_str}")

    # Part 6: Score trajectory
    if population.score_history:
        recent = population.score_history[-5:]
        trajectory = " -> ".join(f"{s:.4f}" for _, s in recent)
        parts.append(f"\n## Score Trajectory\n{trajectory}")

    # Part 7: Output format
    parts.append(f"""
## Output Format
Return the COMPLETE modified code for each evolution block you change.
Use this exact format:

```
# EVOLVE-BLOCK-START: BLOCK_NAME
<your modified code here>
# EVOLVE-BLOCK-END: BLOCK_NAME
```

You MUST modify at least one of: {', '.join(blocks_to_evolve)}

Constraints:
- Code must be syntactically valid Python
- Do not modify code outside evolution blocks
- Maintain the same function/class interfaces as the seed program
""")

    return "\n".join(parts)


def build_meta_prompt_evolution_prompt(
    current_system_prompt: str,
    population: PopulationDatabase,
    iteration: int,
) -> str:
    """Build a prompt for meta-prompt evolution."""
    stats = population.get_stats()
    top_5 = population.get_top_k(5)

    feature_summary = {}
    for c in top_5:
        for k, v in c.architectural_features.items():
            if v and v is not True:
                feature_summary.setdefault(k, []).append(str(v))
            elif v is True:
                feature_summary.setdefault(k, []).append("present")

    prompt = f"""## Meta-Prompt Evolution

Improve the system prompt that guides an evolutionary program search.
It has been used for {iteration} iterations.

### Current System Prompt
```
{current_system_prompt}
```

### Population Statistics
- Total evaluated: {stats['total_candidates']}
- Success rate: {stats['success_rate']}
- Best score: {stats['best_score']}
- Top 5 scores: {stats['top_5_scores']}
- Unique bins: {stats['unique_bins']}

### Winning Features
"""
    for feat, vals in feature_summary.items():
        prompt += f"- {feat}: {', '.join(set(vals))}\n"

    prompt += """
### Task
Rewrite the system prompt to:
1. Emphasize directions that have been working well
2. Suggest new unexplored directions to increase diversity
3. Remove suggestions that haven't yielded improvements

Return the improved system prompt between ```prompt and ``` markers.
"""
    return prompt


def parse_candidate_response(response: str, seed_code: str) -> str:
    """
    Parse Gemini response and apply modifications to the seed code.
    Handles evolution block format and markdown code blocks.
    """
    blocks = extract_evolution_blocks(response)

    if blocks:
        from .seed_program import replace_evolution_block
        result = seed_code
        for block_name, block_content in blocks.items():
            result = replace_evolution_block(result, block_name, block_content)
        return result

    # Fallback: extract code from markdown code blocks
    code_blocks = []
    in_code = False
    current_block = []
    for line in response.split("\n"):
        if line.strip().startswith("```python") or line.strip().startswith("```"):
            if in_code:
                code_blocks.append("\n".join(current_block))
                current_block = []
                in_code = False
            else:
                in_code = True
        elif in_code:
            current_block.append(line)

    if current_block:
        code_blocks.append("\n".join(current_block))

    if code_blocks:
        largest = max(code_blocks, key=len)
        # Check it looks like a complete program
        if "def " in largest or "class " in largest:
            return largest

    return seed_code


def parse_meta_prompt_response(response: str) -> Optional[str]:
    """Extract the evolved system prompt from a meta-prompt response."""
    in_prompt = False
    lines = []
    for line in response.split("\n"):
        if line.strip().startswith("```prompt"):
            in_prompt = True
        elif line.strip() == "```" and in_prompt:
            break
        elif in_prompt:
            lines.append(line)
    return "\n".join(lines) if lines else None
