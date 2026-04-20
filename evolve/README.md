# EvolveEngine — Gemini-Powered Evolutionary Model Improvement

An AlphaEvolve-inspired system that uses Google Gemini to autonomously evolve model architectures, hyperparameters, loss functions, and feature engineering through an iterative generate-evaluate-refine loop.

**Model-agnostic.** Works on XGBoost, PyTorch, sklearn, or any Python model.

---

## Quick Start: Evolve the XGBoost WPP Model

### Prerequisites

```bash
pip install xgboost numpy scikit-learn scipy google-generativeai
gcloud auth application-default login   # for BigQuery access
```

### Run on Real Data (recommended)

```bash
export GOOGLE_API_KEY=<your-gemini-api-key>

python -m evolve.run \
  --example xgb_wpp \
  --bigquery \
  --iterations 100 \
  --concurrent 4 \
  --output-dir evolve_results/xgb_wpp_real
```

### Run Smoke Test (synthetic data, no API keys needed)

```bash
python -m evolve.run \
  --example xgb_wpp \
  --difficulty medium \
  --iterations 20 \
  --output-dir evolve_results/smoke_test
```

> **Warning:** Synthetic results do NOT transfer to real data. Always validate on BigQuery.

---

## How It Works

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Seed Model  │────>│    Gemini    │────>│   Candidate   │
│  (your code  │     │  (Pro 30%    │     │   Program     │
│  with EVOLVE │     │   Flash 70%) │     │   (mutated)   │
│  blocks)     │     └──────────────┘     └──────┬───────┘
└──────────────┘                                  │
       ▲                                          ▼
       │                                 ┌──────────────┐
       │                                 │   Evaluate    │
       │                                 │   (train +    │
       │                                 │    score vs   │
       │                                 │    baseline)  │
       │                                 └──────┬───────┘
       │                                          │
       │         ┌──────────────┐                │
       └─────────│  Population  │<───────────────┘
                 │  Database    │
                 │  (MAP-elites │
                 │   + islands) │
                 └──────────────┘
```

Each iteration:
1. **Gemini** proposes code changes to marked evolution blocks
2. **Evaluator** trains the candidate and scores it against the baseline
3. **Population** keeps the best, maintains diversity across islands
4. **Repeat** — progressively better models emerge

---

## Current Baseline (the bar to beat)

| Metric | Value |
|--------|-------|
| Accuracy | 58.0% |
| ROC AUC | 0.587 |
| Trades | 274 |
| Win Rate | 59.5% |
| P&L | +82.5% |
| Sharpe | 0.37 |

**Config:** `max_depth=6, lr=0.03, 300 rounds, gbtree, moderate regularization`
**Model location:** `gs://na-analytics-media-stocks/models/xgb_wpp_evolved/model.bst`

---

## Scoring

Candidates are scored **relative to baseline** (not absolute):

- **Score > 0** = better than baseline
- **Score = 0** = matches baseline
- **Score < 0** = worse than baseline
- **Score = -1** = crash, or fewer than 50 trades (useless model)

The score is a weighted combination of improvement deltas:

```
score = 0.30 * Δaccuracy + 0.25 * Δauc + 0.20 * Δwin_rate + 0.15 * Δsharpe + 0.10 * trade_penalty
```

---

## Lessons Learned (read before running)

### What FAILED on real data
- Aggressive regularization (gamma>2, reg_alpha>5) → model too conservative, zero trades
- DART booster with high dropout → predicts "up" for everything
- max_depth < 4 → too shallow for feature interactions
- Very low learning_rate with 800+ rounds → overfits to noise

### What WORKED
- **Monotonic constraint on `net_wins`** — domain knowledge that transfers (baked into seed)
- **Feature interactions** — cross-feature signals that XGBoost can't discover alone
- **Moderate hyperparameter adjustments** — small moves from baseline, not radical changes

### The synthetic data trap
Models evolved on synthetic data scored +9% improvement but **performed worse on real data**. The synthetic distribution doesn't match real market dynamics. Always use `--bigquery`.

---

## Evolve Your Own Model

### 1. Prepare Your Seed Program

Add `EVOLVE-BLOCK` markers around mutable regions:

```python
# EVOLVE-BLOCK-START: HYPERPARAMETERS
LEARNING_RATE = 0.001
HIDDEN_DIM = 128
# EVOLVE-BLOCK-END: HYPERPARAMETERS

# EVOLVE-BLOCK-START: MODEL_ARCHITECTURE
class MyModel(nn.Module):
    ...
# EVOLVE-BLOCK-END: MODEL_ARCHITECTURE
```

Save as `my_seed.py`.

### 2. Write an Evaluation Function

Create `my_eval.py` with a single function:

```python
def evaluate(candidate_code: str) -> dict:
    """
    Execute the candidate, train it, return metrics.
    Must include your target metric key.
    """
    exec(candidate_code, namespace)
    model = namespace["MyModel"](...)
    # ... train and evaluate ...
    return {
        "accuracy": 0.85,
        "f1": 0.82,
        "my_target_metric": 0.83,
    }
```

### 3. Write a System Prompt

Create `my_prompt.txt` with domain knowledge and directions to explore:

```
You are evolving a classifier for [domain].
Current accuracy: X%. Try: [specific ideas].
Avoid: [things that failed].
```

### 4. Run

```bash
export GOOGLE_API_KEY=<key>

python -m evolve.run \
  --seed my_seed.py \
  --eval my_eval.py \
  --prompt my_prompt.txt \
  --target-metric my_target_metric \
  --iterations 100 \
  --output-dir evolve_results/my_experiment
```

### 5. Resume from Previous Run

```bash
python -m evolve.run \
  --seed my_seed.py \
  --eval my_eval.py \
  --prompt my_prompt.txt \
  --target-metric my_target_metric \
  --seed-from evolve_results/my_experiment/final_population.json \
  --iterations 200
```

---

## CLI Reference

```
python -m evolve.run [OPTIONS]

Mode (pick one):
  --example {stock,xgb_wpp}    Built-in example
  --seed FILE --eval FILE       Custom model

Options:
  --bigquery                    Use real BigQuery data (required for honest results)
  --prompt FILE                 System prompt file (custom mode)
  --target-metric KEY           Metric key to maximize (default: score)
  --iterations N                Max evolutionary iterations (default: 50)
  --concurrent N                Parallel candidate evaluations (default: 4)
  --population N                Population size (default: 50)
  --islands N                   Number of islands (default: 5)
  --output-dir DIR              Results directory (default: evolve_results)
  --experiment-id ID            Experiment identifier
  --seed-from FILE              Resume from previous population JSON
  --device {auto,cuda,cpu}      Compute device
  --difficulty {easy,medium,hard} Synthetic data difficulty (smoke test only)
  --no-meta-prompt              Disable system prompt self-evolution
```

---

## Output Files

After a run, `output-dir/` contains:

| File | Description |
|------|-------------|
| `best_program.py` | Winning candidate — drop-in replacement |
| `top_1_program.py` ... `top_10_program.py` | Top 10 candidates for ensembling |
| `summary.json` | Full metrics, score trajectory, config |
| `final_population.json` | Population state (for resuming) |
| `checkpoint_iter_*.json` | Periodic snapshots |

---

## Architecture

```
evolve/
├── __init__.py              # Public API
├── config.py                # EvolveConfig dataclass
├── engine.py                # Core evolutionary loop (async)
├── evaluator.py             # Safe candidate evaluation wrapper
├── population.py            # MAP-elites + island population database
├── prompt_sampler.py        # Gemini prompt construction
├── seed_program.py          # EVOLVE-BLOCK utilities
├── run.py                   # CLI entry point
├── requirements.txt
└── examples/
    ├── stock_predictor.py   # PyTorch multi-task example
    └── xgb_wpp_daily.py     # XGBoost WPP example (recommended)
```

---

## Programmatic Usage

```python
import asyncio
from evolve import EvolveEngine, EvolveConfig

config = EvolveConfig(
    target_metric="accuracy",
    max_iterations=100,
    concurrent_evaluations=4,
)

engine = EvolveEngine(
    config=config,
    seed_code=open("my_seed.py").read(),
    eval_fn=my_evaluate_function,
    system_prompt="Improve this classifier...",
    gemini_client=genai,  # from google.generativeai
)

summary = asyncio.run(engine.run())
print(f"Best score: {summary['best_score']}")
```

---

## FAQ

**Q: Can I use this on non-XGBoost models?**
Yes. It's model-agnostic. See `examples/stock_predictor.py` for a PyTorch example.

**Q: How many Gemini API calls does it make?**
~1 call per candidate + 1 meta-prompt call every 25 iterations. At 4 concurrent evals and 100 iterations, expect ~400 Gemini calls.

**Q: How long does a run take?**
Depends on eval time. XGBoost on 2,481 rows: ~1 second per candidate. 100 iterations × 4 concurrent = ~10-15 minutes.

**Q: What if Gemini responses get filtered?**
The engine has safety settings relaxed for code generation and falls back to random mutations when responses are blocked. This is expected for ~5-10% of calls.

**Q: Should I use synthetic or real data?**
**Real data. Always.** Synthetic is only for testing the pipeline itself. Results on synthetic data do not transfer.
