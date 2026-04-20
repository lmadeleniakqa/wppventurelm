import json, numpy as np, os, sys
from google.cloud import bigquery, storage

# Fetch data from BigQuery
client = bigquery.Client(project='na-analytics')
query = "SELECT * FROM `na-analytics.media_stocks.daily_features` ORDER BY date"
df = client.query(query).to_dataframe()
print(f"Loaded {len(df)} rows from BigQuery")

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

# Prepare features
feature_cols = ['ret_1d', 'ret_5d', 'ret_20d', 'ret_60d',
                'sp_ret_1d', 'sp_ret_5d', 'sp_ret_20d',
                'rel_5d', 'rel_20d', 'vol_20d', 'beta_60d', 'rsi_14',
                'net_wins', 'net_spend_bn', 'gdelt_tone', 'gdelt_volume']

# One-hot encode ticker
for t in ['WPP', 'Publicis', 'Omnicom']:
    df[f'is_{t}'] = (df['ticker'] == t).astype(float)
    feature_cols.append(f'is_{t}')

X = df[feature_cols].values.astype(np.float32)
y_dir = df['target_direction'].values.astype(np.float32)
y_ret = df['target_5d_return'].values.astype(np.float32)

# Normalize
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()

# Sequential split: 80% train, 20% test
split = int(len(X) * 0.8)
X_train, X_test = X[:split], X[split:]
y_dir_train, y_dir_test = y_dir[:split], y_dir[split:]
y_ret_train, y_ret_test = y_ret[:split], y_ret[split:]
dates_test = df['date'].values[split:]
tickers_test = df['ticker'].values[split:]
prices_test = df['price'].values[split:]

X_train_s = scaler.fit_transform(X_train)
X_test_s = scaler.transform(X_test)

# Convert to tensors
X_tr = torch.FloatTensor(X_train_s)
y_dir_tr = torch.FloatTensor(y_dir_train)
y_ret_tr = torch.FloatTensor(y_ret_train)
X_te = torch.FloatTensor(X_test_s)

# Model: Multi-task with shared embedding
class StockPredictor(nn.Module):
    def __init__(self, n_features):
        super().__init__()
        # Shared encoder (embedding layer)
        self.encoder = nn.Sequential(
            nn.Linear(n_features, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
        )
        # Direction head
        self.dir_head = nn.Sequential(
            nn.Linear(32, 16), nn.ReLU(), nn.Linear(16, 1), nn.Sigmoid()
        )
        # Return head
        self.ret_head = nn.Sequential(
            nn.Linear(32, 16), nn.ReLU(), nn.Linear(16, 1)
        )
    
    def forward(self, x):
        embedding = self.encoder(x)
        direction = self.dir_head(embedding).squeeze()
        ret = self.ret_head(embedding).squeeze()
        return direction, ret, embedding

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Training on: {device}")

model = StockPredictor(len(feature_cols)).to(device)
optimizer = torch.optim.AdamW(model.parameters(), lr=0.001, weight_decay=0.01)
scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=100)

dir_loss_fn = nn.BCELoss()
ret_loss_fn = nn.MSELoss()

# Training
dataset = TensorDataset(X_tr, y_dir_tr, y_ret_tr)
loader = DataLoader(dataset, batch_size=256, shuffle=True)

best_loss = float('inf')
for epoch in range(100):
    model.train()
    total_loss = 0
    for batch_X, batch_dir, batch_ret in loader:
        batch_X = batch_X.to(device)
        batch_dir = batch_dir.to(device)
        batch_ret = batch_ret.to(device)
        
        pred_dir, pred_ret, _ = model(batch_X)
        loss = 0.6 * dir_loss_fn(pred_dir, batch_dir) + 0.4 * ret_loss_fn(pred_ret, batch_ret)
        
        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        total_loss += loss.item()
    
    scheduler.step()
    avg_loss = total_loss / len(loader)
    if avg_loss < best_loss:
        best_loss = avg_loss
        torch.save(model.state_dict(), '/tmp/best_model.pt')
    
    if (epoch + 1) % 20 == 0:
        print(f"  Epoch {epoch+1}/100, Loss: {avg_loss:.4f}")

# Evaluate
model.load_state_dict(torch.load('/tmp/best_model.pt'))
model.eval()
with torch.no_grad():
    pred_dir, pred_ret, embeddings = model(X_te.to(device))
    pred_dir = pred_dir.cpu().numpy()
    pred_ret = pred_ret.cpu().numpy()
    embeddings = embeddings.cpu().numpy()

# Metrics
dir_preds = (pred_dir > 0.5).astype(int)
accuracy = np.mean(dir_preds == y_dir_test) * 100
ret_rmse = np.sqrt(np.mean((pred_ret - y_ret_test) ** 2))
ret_corr = np.corrcoef(pred_ret, y_ret_test)[0, 1]

# Per-stock metrics
results = {'overall': {'accuracy': round(accuracy, 1), 'rmse': round(ret_rmse, 4), 
                        'return_correlation': round(ret_corr, 3), 'n_test': len(y_dir_test)}}

for ticker in ['WPP', 'Publicis', 'Omnicom']:
    mask = tickers_test == ticker
    if mask.sum() == 0: continue
    t_acc = np.mean(dir_preds[mask] == y_dir_test[mask]) * 100
    t_corr = np.corrcoef(pred_ret[mask], y_ret_test[mask])[0, 1] if mask.sum() > 10 else 0
    
    # Trading sim
    trades = []
    for j in np.where(mask)[0]:
        if pred_dir[j] > 0.55:
            trades.append(float(y_ret_test[j]))
        elif pred_dir[j] < 0.45:
            trades.append(-float(y_ret_test[j]))
    
    total_pnl = sum(trades)
    win_rate = sum(1 for t in trades if t > 0) / len(trades) * 100 if trades else 0
    sharpe = np.mean(trades) / np.std(trades) * np.sqrt(52) if trades and np.std(trades) > 0 else 0
    
    results[ticker] = {
        'accuracy': round(t_acc, 1), 'return_correlation': round(float(t_corr), 3),
        'n_test': int(mask.sum()), 'n_trades': len(trades),
        'total_pnl': round(total_pnl, 1), 'win_rate': round(win_rate, 1),
        'sharpe': round(float(sharpe), 2),
    }
    print(f"\n{ticker}: Accuracy={t_acc:.1f}%, Corr={t_corr:.3f}, Trades={len(trades)}, P&L={total_pnl:+.1f}%, WinRate={win_rate:.1f}%, Sharpe={sharpe:.2f}")

# Save results
output_path = os.environ.get('AIP_MODEL_DIR', '/tmp')
with open(f'{output_path}/results.json', 'w') as f:
    json.dump(results, f, default=lambda x: float(x) if hasattr(x, 'item') else str(x))

# Also save to GCS
bucket_name = 'na-analytics-media-stocks'
try:
    storage_client = storage.Client(project='na-analytics')
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except:
        bucket = storage_client.create_bucket(bucket_name, location='us-central1')
    blob = bucket.blob('vertex_results/results.json')
    blob.upload_from_string(json.dumps(results))
    print(f"\nResults saved to gs://{bucket_name}/vertex_results/results.json")
except Exception as e:
    print(f"\nGCS save failed: {e}")
    # Save locally
    with open('/tmp/vertex_results.json', 'w') as f:
        json.dump(results, f, default=lambda x: float(x) if hasattr(x, 'item') else str(x))

print(f"\nOverall: Accuracy={accuracy:.1f}%, RMSE={ret_rmse:.4f}, Correlation={ret_corr:.3f}")
print(f"\nDone!")
