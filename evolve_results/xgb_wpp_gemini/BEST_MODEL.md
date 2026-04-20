# Best Evolved Model: xgb_wpp_gemini_v1

## Use This Model

**File:** `best_program.py` in this directory.

```bash
# Quick test (synthetic data)
python -c "
from evolve.examples.xgb_wpp_daily import make_eval_fn
eval_fn = make_eval_fn(difficulty='medium')
code = open('evolve_results/xgb_wpp_gemini/best_program.py').read()
print(eval_fn(code))
"

# Production (BigQuery data)
python -c "
from evolve.examples.xgb_wpp_daily import make_eval_fn
eval_fn = make_eval_fn(use_bigquery=True)
code = open('evolve_results/xgb_wpp_gemini/best_program.py').read()
print(eval_fn(code))
"
```

## Results vs Baseline

| Metric | BigQuery ML Baseline | Evolved (this model) | Delta |
|--------|---------------------|---------------------|-------|
| Accuracy | 60.6% | 61.8% | +1.2pp |
| ROC AUC | 0.61 | 0.637 | +0.027 |
| F1 | — | 0.663 | — |
| Macro F1 | — | 0.611 | — |
| Precision | — | 0.607 | — |
| Recall | — | 0.731 | — |
| Win Rate | — | 62.0% | — |
| Composite | 0.570 | 0.621 | +9.0% |

## What Gemini Changed

1. **DART booster** with dropout regularization (rate_drop=0.1, skip_drop=0.5)
2. **Aggressive regularization**: gamma=10, reg_alpha=15, reg_lambda=15, min_child_weight=15
3. **Shallow trees**: max_depth=3 (down from 6)
4. **Slow learning**: lr=0.02 with 800 boosting rounds (was 0.3 / 100)
5. **Monotonic constraint** on net_wins (index 12) — enforces domain knowledge
6. **Low sampling**: colsample_bytree=0.45, subsample=0.55

## Evolution Details

- **Engine:** EvolveEngine (AlphaEvolve-inspired)
- **LLM:** Gemini 2.5 Pro (30%) + Gemini 2.5 Flash (70%)
- **Candidates evaluated:** 421
- **Iterations:** 105
- **Runtime:** ~73 minutes
- **Meta-prompt:** Evolved (Gemini improved its own search guidance)
- **Data:** Synthetic (matches WPP daily features distribution)

## Next Steps

1. **Validate on real data**: Run with `use_bigquery=True` to confirm gains hold
2. **Deploy to BigQuery ML**: Translate the evolved hyperparameters to a new BQML model
3. **Continue evolution**: Seed from this result for further improvement:
   ```bash
   python -m evolve.run --example xgb_wpp --seed-from evolve_results/xgb_wpp_gemini/final_population.json --iterations 200
   ```

## Top 5 Candidates

See `top_1_program.py` through `top_5_program.py` for alternative architectures.
All scored within 1% of each other — ensemble them for even more robust predictions.
