"""
Stock Predictor — built-in example for EvolveEngine.

Demonstrates how to use EvolveEngine on WPP's multi-task stock prediction model.
Provides: seed code, system prompt, and evaluation function.

To use your own model, follow this pattern:
  1. Define SEED_CODE with EVOLVE-BLOCK markers
  2. Write a SYSTEM_PROMPT with domain-specific guidance
  3. Implement make_eval_fn() returning a callable: code_str -> dict of metrics
"""

import gc
import time
import traceback

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.metrics import f1_score, accuracy_score


# ---------------------------------------------------------------------------
# 1. SEED CODE — the starting model with evolution blocks
# ---------------------------------------------------------------------------

SEED_CODE = '''
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np


# EVOLVE-BLOCK-START: HYPERPARAMETERS
LEARNING_RATE = 0.001
WEIGHT_DECAY = 0.01
BATCH_SIZE = 256
MAX_EPOCHS = 100
DROPOUT_RATE_1 = 0.3
DROPOUT_RATE_2 = 0.2
HIDDEN_DIM_1 = 128
HIDDEN_DIM_2 = 64
HIDDEN_DIM_3 = 32
HEAD_DIM = 16
DIRECTION_LOSS_WEIGHT = 0.6
RETURN_LOSS_WEIGHT = 0.4
SCHEDULER_T_MAX = 100
GRAD_CLIP_NORM = 1.0
# EVOLVE-BLOCK-END: HYPERPARAMETERS


# EVOLVE-BLOCK-START: CUSTOM_LOSSES
class DirectionLoss(nn.Module):
    """Loss for direction prediction. Default: BCELoss."""
    def __init__(self):
        super().__init__()
        self.bce = nn.BCELoss()
    def forward(self, pred, target):
        return self.bce(pred, target)

class ReturnLoss(nn.Module):
    """Loss for return prediction. Default: MSELoss."""
    def __init__(self):
        super().__init__()
        self.mse = nn.MSELoss()
    def forward(self, pred, target):
        return self.mse(pred, target)

class AuxiliaryLoss(nn.Module):
    """Optional auxiliary loss on embeddings."""
    def __init__(self):
        super().__init__()
    def forward(self, embeddings, labels=None):
        return torch.tensor(0.0, device=embeddings.device)
# EVOLVE-BLOCK-END: CUSTOM_LOSSES


# EVOLVE-BLOCK-START: MODEL_ARCHITECTURE
class StockPredictor(nn.Module):
    """
    Multi-task stock predictor.
    forward(x) -> (direction_prob, predicted_return, embedding)
    """
    def __init__(self, n_features):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(n_features, HIDDEN_DIM_1),
            nn.BatchNorm1d(HIDDEN_DIM_1),
            nn.ReLU(),
            nn.Dropout(DROPOUT_RATE_1),
            nn.Linear(HIDDEN_DIM_1, HIDDEN_DIM_2),
            nn.BatchNorm1d(HIDDEN_DIM_2),
            nn.ReLU(),
            nn.Dropout(DROPOUT_RATE_2),
            nn.Linear(HIDDEN_DIM_2, HIDDEN_DIM_3),
            nn.ReLU(),
        )
        self.dir_head = nn.Sequential(
            nn.Linear(HIDDEN_DIM_3, HEAD_DIM), nn.ReLU(),
            nn.Linear(HEAD_DIM, 1), nn.Sigmoid(),
        )
        self.ret_head = nn.Sequential(
            nn.Linear(HIDDEN_DIM_3, HEAD_DIM), nn.ReLU(),
            nn.Linear(HEAD_DIM, 1),
        )

    def forward(self, x):
        embedding = self.encoder(x)
        direction = self.dir_head(embedding).squeeze(-1)
        ret = self.ret_head(embedding).squeeze(-1)
        return direction, ret, embedding
# EVOLVE-BLOCK-END: MODEL_ARCHITECTURE


# EVOLVE-BLOCK-START: TRAINING_PROCEDURE
def create_optimizer(model):
    return torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)

def create_scheduler(optimizer):
    return torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=SCHEDULER_T_MAX)

def train_step(model, batch_X, batch_dir, batch_ret, optimizer, dir_loss_fn, ret_loss_fn, aux_loss_fn, device):
    batch_X = batch_X.to(device)
    batch_dir = batch_dir.to(device)
    batch_ret = batch_ret.to(device)
    pred_dir, pred_ret, emb = model(batch_X)
    loss = (DIRECTION_LOSS_WEIGHT * dir_loss_fn(pred_dir, batch_dir)
            + RETURN_LOSS_WEIGHT * ret_loss_fn(pred_ret, batch_ret)
            + aux_loss_fn(emb, batch_dir))
    optimizer.zero_grad()
    loss.backward()
    torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP_NORM)
    optimizer.step()
    return loss.item()
# EVOLVE-BLOCK-END: TRAINING_PROCEDURE
'''


# ---------------------------------------------------------------------------
# 2. SYSTEM PROMPT — domain guidance for Gemini
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert neural network architect evolving a multi-task stock prediction model.
The model predicts: (1) direction (up/down) and (2) 5-day return for media holding company stocks.

## Current Architecture
- Shared encoder: 3-layer MLP (128->64->32) with BatchNorm, ReLU, Dropout
- Direction head: Linear->ReLU->Linear->Sigmoid
- Return head: Linear->ReLU->Linear
- Loss: 0.6 * BCELoss + 0.4 * MSELoss
- Optimizer: AdamW (lr=0.001, weight_decay=0.01)

## Directions to Explore

### Architecture
- Separate encoders per feature group (market vs competitive vs technical)
- Gating mechanisms (squeeze-and-excitation, feature-wise)
- Cross-modal attention between feature groups
- Residual connections, skip connections
- LayerNorm instead of BatchNorm
- Different activations (GELU, SiLU/Swish, Mish, PReLU)

### Loss Functions
- Focal Loss for class imbalance
- Centroid Loss / Cosine Centroid Attraction
- Label smoothing
- Contrastive loss between positive/negative samples
- Learnable loss weights

### Hyperparameters
- Learning rate: 1e-4 to 5e-3
- Weight decay: 1e-4 to 0.1
- Dropout: 0.1 to 0.5
- Hidden dims: 32 to 256
- Batch size: 64 to 512

### Regularization
- Mixup on tabular features
- Spectral normalization
- Feature noise injection

## Constraints
- forward(x) must return (direction, return, embedding)
- direction must be probability in [0, 1]
- Use only torch, numpy, sklearn, scipy
"""


# ---------------------------------------------------------------------------
# 3. EVALUATION FUNCTION FACTORY
# ---------------------------------------------------------------------------

def make_eval_fn(
    difficulty: str = "medium",
    device: str = "auto",
    max_epochs: int = 100,
    early_stopping_patience: int = 15,
):
    """
    Returns an evaluation function: candidate_code (str) -> dict of metrics.

    Uses synthetic data so the example works without BigQuery access.
    For production, replace the data loading with BigQuery.
    """
    # Generate synthetic data once
    np.random.seed(42)
    n_features = 19

    if difficulty == "easy":
        n_samples, noise = 2000, 0.1
    elif difficulty == "medium":
        n_samples, noise = 1500, 0.3
    else:
        n_samples, noise = 1000, 0.6

    X = np.random.randn(n_samples, n_features).astype(np.float32)
    signal = (0.3 * X[:, 0] - 0.2 * X[:, 1] + 0.15 * X[:, 2] * X[:, 3]
              + 0.1 * np.sin(X[:, 4]) - 0.25 * X[:, 5])
    y_dir = (signal + noise * np.random.randn(n_samples) > 0).astype(np.float32)
    y_ret = (signal * 0.02 + noise * 0.01 * np.random.randn(n_samples)).astype(np.float32)

    if difficulty in ("medium", "hard"):
        flip = (y_dir == 1) & (np.random.rand(n_samples) > 0.6)
        y_dir[flip] = 0

    split = int(n_samples * 0.85)
    X_train, X_val = X[:split], X[split:]
    y_dir_train, y_dir_val = y_dir[:split], y_dir[split:]
    y_ret_train, y_ret_val = y_ret[:split], y_ret[split:]

    dev = torch.device(
        "cuda" if device == "auto" and torch.cuda.is_available()
        else device if device != "auto" else "cpu"
    )

    def evaluate(candidate_code: str) -> dict:
        """Train candidate model and return metrics dict."""
        try:
            ns = {"torch": torch, "nn": nn, "F": torch.nn.functional, "np": np}
            exec(candidate_code, ns)

            ModelClass = ns.get("StockPredictor")
            if ModelClass is None:
                return {"macro_f1": -1.0, "error": "StockPredictor not found"}

            batch_size = max(32, min(512, int(ns.get("BATCH_SIZE", 256))))
            epochs = max(20, min(max_epochs, int(ns.get("MAX_EPOCHS", 100))))

            model = ModelClass(n_features).to(dev)

            DirLoss = ns.get("DirectionLoss")
            RetLoss = ns.get("ReturnLoss")
            AuxLoss = ns.get("AuxiliaryLoss")
            dir_loss = DirLoss().to(dev) if DirLoss else nn.BCELoss().to(dev)
            ret_loss = RetLoss().to(dev) if RetLoss else nn.MSELoss().to(dev)
            aux_loss = AuxLoss().to(dev) if AuxLoss else None

            create_opt = ns.get("create_optimizer")
            create_sched = ns.get("create_scheduler")
            train_step_fn = ns.get("train_step")

            optimizer = create_opt(model) if create_opt else torch.optim.AdamW(
                model.parameters(),
                lr=float(ns.get("LEARNING_RATE", 0.001)),
                weight_decay=float(ns.get("WEIGHT_DECAY", 0.01)),
            )
            scheduler = create_sched(optimizer) if create_sched else None

            X_tr_t = torch.FloatTensor(X_train).to(dev)
            y_dir_t = torch.FloatTensor(y_dir_train).to(dev)
            y_ret_t = torch.FloatTensor(y_ret_train).to(dev)
            X_val_t = torch.FloatTensor(X_val).to(dev)
            dataset = TensorDataset(X_tr_t, y_dir_t, y_ret_t)
            loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

            dir_w = float(ns.get("DIRECTION_LOSS_WEIGHT", 0.6))
            ret_w = float(ns.get("RETURN_LOSS_WEIGHT", 0.4))
            clip = float(ns.get("GRAD_CLIP_NORM", 1.0))

            null_aux = lambda emb, lab=None: torch.tensor(0.0, device=emb.device)
            best_val, patience, best_state = -1.0, 0, None

            for epoch in range(epochs):
                model.train()
                for bX, bD, bR in loader:
                    if train_step_fn:
                        train_step_fn(model, bX, bD, bR, optimizer, dir_loss, ret_loss,
                                      aux_loss if aux_loss else null_aux, dev)
                    else:
                        pD, pR, emb = model(bX)
                        loss = dir_w * dir_loss(pD, bD) + ret_w * ret_loss(pR, bR)
                        if aux_loss:
                            loss = loss + aux_loss(emb, bD)
                        optimizer.zero_grad()
                        loss.backward()
                        torch.nn.utils.clip_grad_norm_(model.parameters(), clip)
                        optimizer.step()
                if scheduler:
                    scheduler.step()

                if (epoch + 1) % 5 == 0:
                    model.eval()
                    with torch.no_grad():
                        vD, _, _ = model(X_val_t)
                    f1 = f1_score(y_dir_val, (vD.cpu().numpy() > 0.5).astype(int),
                                  average="macro", zero_division=0)
                    if f1 > best_val:
                        best_val = f1
                        patience = 0
                        best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
                    else:
                        patience += 1
                        if patience >= early_stopping_patience // 5:
                            break

            if best_state:
                model.load_state_dict({k: v.to(dev) for k, v in best_state.items()})

            # Full eval
            model.eval()
            with torch.no_grad():
                pD, pR, _ = model(X_val_t)
                pD_np = pD.cpu().numpy()
                pR_np = pR.cpu().numpy()

            preds = (pD_np > 0.5).astype(int)
            macro_f1 = f1_score(y_dir_val, preds, average="macro", zero_division=0)
            accuracy = accuracy_score(y_dir_val, preds)
            rmse = float(np.sqrt(np.mean((pR_np - y_ret_val) ** 2)))

            trades = []
            for i in range(len(pD_np)):
                if pD_np[i] > 0.55:
                    trades.append(float(y_ret_val[i]))
                elif pD_np[i] < 0.45:
                    trades.append(-float(y_ret_val[i]))
            sharpe = (float(np.mean(trades) / np.std(trades) * np.sqrt(52))
                      if trades and np.std(trades) > 0 else 0.0)

            return {
                "macro_f1": float(macro_f1),
                "accuracy": float(accuracy),
                "rmse": rmse,
                "sharpe": sharpe,
                "n_trades": len(trades),
            }

        except Exception as e:
            return {"macro_f1": -1.0, "error": f"{type(e).__name__}: {e}"}

        finally:
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            gc.collect()

    return evaluate
