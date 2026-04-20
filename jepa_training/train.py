"""
Financial JEPA Training Script — Vertex AI (T4 GPU)

Phase 1: Pre-train on all 3 stocks (learn latent financial representations)
Phase 2: Fine-tune on WPP (predict 5-day direction)
Phase 3: Extract embeddings → feed to XGBoost (hybrid approach)
"""

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, TensorDataset
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score
from google.cloud import bigquery, storage
import json, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from model import FinancialJEPA, vicreg_loss

# Config
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
PATCH_SIZE = 5       # 1 trading week
SEQ_LEN = 60         # 60 days lookback (~3 months)
N_FEATURES = 16
D_MODEL = 128
N_HEADS = 4
N_ENC_LAYERS = 4
D_FF = 512
D_PRED = 64
N_PRED_LAYERS = 2
MAX_PATCHES = SEQ_LEN // PATCH_SIZE  # 12 patches
PRETRAIN_EPOCHS = 100
FINETUNE_EPOCHS = 50
BATCH_SIZE = 128
LR_PRETRAIN = 3e-4
LR_FINETUNE = 1e-4
EMA_DECAY = 0.996

FEATURE_COLS = ['ret_1d', 'ret_5d', 'ret_20d', 'ret_60d',
                'sp_ret_1d', 'sp_ret_5d', 'sp_ret_20d',
                'rel_5d', 'rel_20d', 'vol_20d', 'beta_60d', 'rsi_14',
                'net_wins', 'net_spend_bn', 'gdelt_tone', 'gdelt_volume']


def load_data():
    """Load from BigQuery and build windowed sequences"""
    client = bigquery.Client(project='na-analytics')

    # Load all stocks for pre-training
    df = client.query("""
        SELECT * FROM `na-analytics.media_stocks.daily_features`
        ORDER BY ticker, date
    """).to_dataframe()
    print(f"Loaded {len(df)} rows from BigQuery")

    # Build windowed sequences: (seq_len, n_features) with target
    sequences = []
    targets = []
    tickers = []

    for ticker in ['WPP', 'Publicis', 'Omnicom']:
        tdf = df[df['ticker'] == ticker].sort_values('date').reset_index(drop=True)
        X = tdf[FEATURE_COLS].values.astype(np.float32)
        y = tdf['target_direction'].values.astype(np.float32)

        # Normalize features per-stock (z-score)
        mean = X.mean(axis=0)
        std = X.std(axis=0) + 1e-8
        X = (X - mean) / std

        # Sliding windows
        for i in range(SEQ_LEN, len(X) - 1):
            seq = X[i - SEQ_LEN:i]
            sequences.append(seq)
            targets.append(y[i])
            tickers.append(ticker)

    X_all = np.array(sequences)
    y_all = np.array(targets)
    tickers_all = np.array(tickers)
    print(f"Built {len(X_all)} sequences ({SEQ_LEN}-day windows)")
    print(f"  WPP: {(tickers_all == 'WPP').sum()}, Publicis: {(tickers_all == 'Publicis').sum()}, Omnicom: {(tickers_all == 'Omnicom').sum()}")

    return X_all, y_all, tickers_all


def pretrain(model, X_all, epochs=PRETRAIN_EPOCHS):
    """Phase 1: Self-supervised pre-training on all stocks"""
    print(f"\n{'='*60}")
    print(f"  PHASE 1: PRE-TRAINING (all stocks, {len(X_all)} sequences)")
    print(f"{'='*60}")

    dataset = TensorDataset(torch.FloatTensor(X_all))
    loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True, drop_last=True)

    optimizer = torch.optim.AdamW(
        list(model.context_encoder.parameters()) +
        list(model.predictor.parameters()) +
        list(model.patch_embed.parameters()) +
        list(model.pos_enc.parameters()),
        lr=LR_PRETRAIN, weight_decay=0.05
    )
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs)

    best_loss = float('inf')
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        n_batches = 0

        for (batch_x,) in loader:
            batch_x = batch_x.to(DEVICE)

            predicted, target, context = model.pretrain_forward(batch_x)

            # JEPA loss: L2 in latent space
            jepa_loss = F.mse_loss(predicted, target)

            # VICReg anti-collapse on target representations
            target_flat = target.reshape(-1, target.size(-1))
            predicted_flat = predicted.reshape(-1, predicted.size(-1))
            vic_loss = vicreg_loss(predicted_flat, target_flat,
                                   sim_weight=1.0, var_weight=25.0, cov_weight=1.0)

            loss = jepa_loss + 0.1 * vic_loss

            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

            # EMA update target encoder
            model._update_target_encoder()

            total_loss += loss.item()
            n_batches += 1

        scheduler.step()
        avg_loss = total_loss / n_batches

        if avg_loss < best_loss:
            best_loss = avg_loss
            torch.save(model.state_dict(), '/tmp/jepa_pretrained.pt')

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{epochs}, Loss: {avg_loss:.4f} (best: {best_loss:.4f})")

    model.load_state_dict(torch.load('/tmp/jepa_pretrained.pt'))
    print(f"  Pre-training done. Best loss: {best_loss:.4f}")
    return model


def finetune(model, X_wpp, y_wpp, epochs=FINETUNE_EPOCHS):
    """Phase 2: Fine-tune on WPP for direction prediction"""
    print(f"\n{'='*60}")
    print(f"  PHASE 2: FINE-TUNING (WPP, {len(X_wpp)} sequences)")
    print(f"{'='*60}")

    # Sequential split
    split = int(len(X_wpp) * 0.8)
    X_train, X_test = X_wpp[:split], X_wpp[split:]
    y_train, y_test = y_wpp[:split], y_wpp[split:]

    train_ds = TensorDataset(torch.FloatTensor(X_train), torch.FloatTensor(y_train))
    train_loader = DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True)

    # Freeze target encoder, fine-tune context encoder + head
    optimizer = torch.optim.AdamW([
        {'params': model.context_encoder.parameters(), 'lr': LR_FINETUNE * 0.1},  # slow LR for encoder
        {'params': model.fine_tune_head.parameters(), 'lr': LR_FINETUNE},
        {'params': model.cls_token, 'lr': LR_FINETUNE},
    ], weight_decay=0.01)

    criterion = nn.BCEWithLogitsLoss()
    best_acc = 0

    for epoch in range(epochs):
        model.train()
        total_loss = 0
        for batch_x, batch_y in train_loader:
            batch_x, batch_y = batch_x.to(DEVICE), batch_y.to(DEVICE)
            logits, _ = model(batch_x)
            loss = criterion(logits, batch_y)
            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += loss.item()

        # Evaluate
        model.eval()
        with torch.no_grad():
            X_te = torch.FloatTensor(X_test).to(DEVICE)
            logits, _ = model(X_te)
            probs = torch.sigmoid(logits).cpu().numpy()
            preds = (probs >= 0.5).astype(int)
            acc = accuracy_score(y_test, preds)

            if acc > best_acc:
                best_acc = acc
                torch.save(model.state_dict(), '/tmp/jepa_finetuned.pt')

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{epochs}, Loss: {total_loss/len(train_loader):.4f}, Acc: {acc*100:.1f}% (best: {best_acc*100:.1f}%)")

    model.load_state_dict(torch.load('/tmp/jepa_finetuned.pt'))
    return model, X_test, y_test


def extract_embeddings_for_xgboost(model, X_wpp, y_wpp):
    """Phase 3: Extract JEPA embeddings and train XGBoost on top"""
    print(f"\n{'='*60}")
    print(f"  PHASE 3: JEPA → XGBoost HYBRID ({len(X_wpp)} sequences)")
    print(f"{'='*60}")

    model.eval()
    split = int(len(X_wpp) * 0.8)

    with torch.no_grad():
        # Extract embeddings for all data
        embeds_list = []
        for i in range(0, len(X_wpp), 256):
            batch = torch.FloatTensor(X_wpp[i:i+256]).to(DEVICE)
            _, emb = model(batch)
            embeds_list.append(emb.cpu().numpy())
        embeddings = np.concatenate(embeds_list, axis=0)

    print(f"  Embeddings shape: {embeddings.shape}")

    # Combine JEPA embeddings with original features (last timestep)
    original_features = X_wpp[:, -1, :]  # last day's features
    X_combined = np.concatenate([original_features, embeddings], axis=1)
    print(f"  Combined features: {X_combined.shape[1]} ({original_features.shape[1]} original + {embeddings.shape[1]} JEPA)")

    X_train, X_test = X_combined[:split], X_combined[split:]
    y_train, y_test = y_wpp[:split], y_wpp[split:]

    # XGBoost with JEPA embeddings
    try:
        import xgboost as xgb
        dtrain = xgb.DMatrix(X_train, label=y_train)
        dtest = xgb.DMatrix(X_test, label=y_test)

        params = {
            'objective': 'binary:logistic', 'eval_metric': 'logloss',
            'max_depth': 6, 'learning_rate': 0.03,
            'subsample': 0.7, 'colsample_bytree': 0.7,
            'min_child_weight': 10, 'random_state': 42,
        }

        xgb_model = xgb.train(params, dtrain, num_boost_round=300,
                               evals=[(dtest, 'val')], early_stopping_rounds=30, verbose_eval=False)

        probs_xgb = xgb_model.predict(dtest)
        preds_xgb = (probs_xgb >= 0.5).astype(int)
        acc_xgb = accuracy_score(y_test, preds_xgb)
        auc_xgb = roc_auc_score(y_test, probs_xgb)
        f1_xgb = f1_score(y_test, preds_xgb)

        print(f"\n  XGBoost + JEPA embeddings:")
        print(f"    Accuracy: {acc_xgb*100:.1f}%")
        print(f"    ROC AUC:  {auc_xgb:.3f}")
        print(f"    F1:       {f1_xgb:.3f}")

        return acc_xgb, auc_xgb, f1_xgb
    except ImportError:
        print("  XGBoost not available, skipping hybrid evaluation")
        return 0, 0, 0


def main():
    print(f"Device: {DEVICE}")
    print(f"Architecture: d_model={D_MODEL}, heads={N_HEADS}, layers={N_ENC_LAYERS}, patches={MAX_PATCHES}")

    # Load data
    X_all, y_all, tickers = load_data()

    # Build model
    model = FinancialJEPA(
        n_features=N_FEATURES, patch_size=PATCH_SIZE, d_model=D_MODEL,
        n_heads=N_HEADS, n_encoder_layers=N_ENC_LAYERS, d_ff=D_FF,
        d_predictor=D_PRED, n_predictor_layers=N_PRED_LAYERS,
        n_predictor_heads=N_HEADS, max_patches=MAX_PATCHES,
        dropout=0.1, ema_decay=EMA_DECAY
    ).to(DEVICE)

    n_params = sum(p.numel() for p in model.parameters())
    print(f"Model parameters: {n_params:,}")

    # Phase 1: Pre-train on all stocks
    model = pretrain(model, X_all)

    # Phase 2: Fine-tune on WPP
    wpp_mask = tickers == 'WPP'
    X_wpp, y_wpp = X_all[wpp_mask], y_all[wpp_mask]
    model, X_test, y_test = finetune(model, X_wpp, y_wpp)

    # Evaluate fine-tuned JEPA alone
    model.eval()
    with torch.no_grad():
        logits, _ = model(torch.FloatTensor(X_test).to(DEVICE))
        probs = torch.sigmoid(logits).cpu().numpy()
        preds = (probs >= 0.5).astype(int)
        acc = accuracy_score(y_test, preds)
        auc = roc_auc_score(y_test, probs)
        f1 = f1_score(y_test, preds)

    print(f"\n  JEPA standalone results:")
    print(f"    Accuracy: {acc*100:.1f}%")
    print(f"    ROC AUC:  {auc:.3f}")
    print(f"    F1:       {f1:.3f}")

    # Phase 3: Hybrid JEPA + XGBoost
    acc_hybrid, auc_hybrid, f1_hybrid = extract_embeddings_for_xgboost(model, X_wpp, y_wpp)

    # XGBoost baseline (no JEPA)
    print(f"\n{'='*60}")
    print(f"  BASELINE: XGBoost without JEPA")
    print(f"{'='*60}")
    split = int(len(X_wpp) * 0.8)
    original = X_wpp[:, -1, :]  # last day features only
    try:
        import xgboost as xgb
        dtrain = xgb.DMatrix(original[:split], label=y_wpp[:split])
        dtest = xgb.DMatrix(original[split:], label=y_wpp[split:])
        params = {'objective':'binary:logistic','eval_metric':'logloss','max_depth':6,'learning_rate':0.03,'subsample':0.7,'colsample_bytree':0.7,'min_child_weight':10,'random_state':42}
        xgb_base = xgb.train(params, dtrain, num_boost_round=300, evals=[(dtest,'val')], early_stopping_rounds=30, verbose_eval=False)
        probs_base = xgb_base.predict(dtest)
        preds_base = (probs_base >= 0.5).astype(int)
        acc_base = accuracy_score(y_wpp[split:], preds_base)
        auc_base = roc_auc_score(y_wpp[split:], probs_base)
        f1_base = f1_score(y_wpp[split:], preds_base)
        print(f"    Accuracy: {acc_base*100:.1f}%")
        print(f"    ROC AUC:  {auc_base:.3f}")
        print(f"    F1:       {f1_base:.3f}")
    except:
        acc_base, auc_base, f1_base = 0, 0, 0

    # Summary
    results = {
        'jepa_standalone': {'accuracy': round(acc*100, 1), 'roc_auc': round(float(auc), 3), 'f1': round(float(f1), 3)},
        'jepa_xgboost_hybrid': {'accuracy': round(acc_hybrid*100, 1), 'roc_auc': round(float(auc_hybrid), 3), 'f1': round(float(f1_hybrid), 3)},
        'xgboost_baseline': {'accuracy': round(acc_base*100, 1), 'roc_auc': round(float(auc_base), 3), 'f1': round(float(f1_base), 3)},
        'architecture': {
            'd_model': D_MODEL, 'n_heads': N_HEADS, 'n_layers': N_ENC_LAYERS,
            'patch_size': PATCH_SIZE, 'seq_len': SEQ_LEN, 'n_params': n_params,
            'pretrain_epochs': PRETRAIN_EPOCHS, 'finetune_epochs': FINETUNE_EPOCHS,
        }
    }

    print(f"\n{'='*60}")
    print(f"  FINAL COMPARISON")
    print(f"{'='*60}")
    print(f"  {'Model':<25} {'Acc':>8} {'AUC':>8} {'F1':>8}")
    print(f"  {'-'*49}")
    print(f"  {'XGBoost baseline':<25} {acc_base*100:>7.1f}% {auc_base:>8.3f} {f1_base:>8.3f}")
    print(f"  {'JEPA standalone':<25} {acc*100:>7.1f}% {auc:>8.3f} {f1:>8.3f}")
    print(f"  {'JEPA + XGBoost hybrid':<25} {acc_hybrid*100:>7.1f}% {auc_hybrid:>8.3f} {f1_hybrid:>8.3f}")

    # Save results to GCS
    try:
        sc = storage.Client(project='na-analytics')
        bucket = sc.get_bucket('na-analytics-media-stocks')
        bucket.blob('models/jepa/results.json').upload_from_string(json.dumps(results, default=float))
        torch.save(model.state_dict(), '/tmp/jepa_final.pt')
        bucket.blob('models/jepa/model.pt').upload_from_filename('/tmp/jepa_final.pt')
        print(f"\n  Saved to gs://na-analytics-media-stocks/models/jepa/")
    except Exception as e:
        print(f"\n  GCS save failed: {e}")

    print("\nDone!")


if __name__ == '__main__':
    main()
