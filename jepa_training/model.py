"""
Financial JEPA — Joint Embedding Predictive Architecture for Stock Prediction

Architecture adapted from I-JEPA (Assran et al., CVPR 2023) for 1D financial time series.
Key adaptations:
  - Patch-based: 5-day windows (1 trading week) of multi-feature vectors
  - Causal masking: always mask future patches (predict what comes next in latent space)
  - VICReg anti-collapse regularization (critical for low-signal financial data)
  - EMA target encoder (no gradients, slow-moving representation target)
  - Hybrid: pre-trained embeddings fed to XGBoost for final prediction

References:
  - LeCun (2022): "A Path Towards Autonomous Machine Intelligence"
  - Assran et al. (2023): I-JEPA (CVPR)
  - Bardes et al. (2022): VICReg
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import math
import copy


class PatchEmbedding(nn.Module):
    """Convert (batch, seq_len, n_features) → (batch, n_patches, d_model)"""
    def __init__(self, patch_size, n_features, d_model):
        super().__init__()
        self.patch_size = patch_size
        self.proj = nn.Linear(patch_size * n_features, d_model)
        self.norm = nn.LayerNorm(d_model)

    def forward(self, x):
        B, L, F = x.shape
        n_patches = L // self.patch_size
        x = x[:, :n_patches * self.patch_size, :]
        x = x.reshape(B, n_patches, self.patch_size * F)
        return self.norm(self.proj(x))


class PositionalEncoding(nn.Module):
    """Learned positional + day-of-week encoding"""
    def __init__(self, max_patches, d_model):
        super().__init__()
        self.pos_embed = nn.Parameter(torch.randn(1, max_patches, d_model) * 0.02)

    def forward(self, x):
        return x + self.pos_embed[:, :x.size(1), :]


class TransformerEncoder(nn.Module):
    """Standard transformer encoder for JEPA context/target"""
    def __init__(self, d_model, n_heads, n_layers, d_ff, dropout=0.1):
        super().__init__()
        layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=d_ff,
            dropout=dropout, batch_first=True, norm_first=True
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=n_layers)
        self.norm = nn.LayerNorm(d_model)

    def forward(self, x, mask=None):
        return self.norm(self.encoder(x, src_key_padding_mask=mask))


class Predictor(nn.Module):
    """Narrow transformer that predicts target representations from context + mask tokens"""
    def __init__(self, d_model, d_pred, n_heads, n_layers, max_patches):
        super().__init__()
        self.mask_token = nn.Parameter(torch.randn(1, 1, d_pred) * 0.02)
        self.input_proj = nn.Linear(d_model, d_pred)
        self.pos_embed = nn.Parameter(torch.randn(1, max_patches, d_pred) * 0.02)
        layer = nn.TransformerEncoderLayer(
            d_model=d_pred, nhead=n_heads, dim_feedforward=d_pred * 4,
            dropout=0.1, batch_first=True, norm_first=True
        )
        self.transformer = nn.TransformerEncoder(layer, num_layers=n_layers)
        self.output_proj = nn.Linear(d_pred, d_model)
        self.norm = nn.LayerNorm(d_model)

    def forward(self, context_embeds, mask_indices, n_patches):
        B = context_embeds.size(0)
        # Project context to predictor dimension
        x = self.input_proj(context_embeds)
        # Build full sequence with mask tokens for masked positions
        full_seq = self.mask_token.expand(B, n_patches, -1).clone()
        # Place context embeddings at visible positions
        visible_indices = [i for i in range(n_patches) if i not in mask_indices]
        for idx, vis_idx in enumerate(visible_indices):
            if idx < x.size(1):
                full_seq[:, vis_idx, :] = x[:, idx, :]
        # Add positional encoding
        full_seq = full_seq + self.pos_embed[:, :n_patches, :]
        # Transform
        out = self.transformer(full_seq)
        # Extract predictions at masked positions only
        masked_preds = out[:, mask_indices, :]
        return self.norm(self.output_proj(masked_preds))


class FinancialJEPA(nn.Module):
    """
    Full JEPA model for financial time series.

    Pre-training: predict masked patch representations in latent space
    Fine-tuning: use context encoder embeddings as features for downstream prediction
    """
    def __init__(self, n_features=16, patch_size=5, d_model=128, n_heads=4,
                 n_encoder_layers=4, d_ff=512, d_predictor=64, n_predictor_layers=2,
                 n_predictor_heads=4, max_patches=60, dropout=0.1, ema_decay=0.996):
        super().__init__()
        self.patch_size = patch_size
        self.max_patches = max_patches
        self.ema_decay = ema_decay
        self.d_model = d_model

        # Patch embedding (shared architecture, separate weights for context/target)
        self.patch_embed = PatchEmbedding(patch_size, n_features, d_model)
        self.pos_enc = PositionalEncoding(max_patches, d_model)

        # Context encoder — receives masked input
        self.context_encoder = TransformerEncoder(d_model, n_heads, n_encoder_layers, d_ff, dropout)

        # Target encoder — receives full input, EMA-updated (no gradients)
        self.target_encoder = TransformerEncoder(d_model, n_heads, n_encoder_layers, d_ff, dropout)
        # Initialize target = context
        self._init_target_encoder()

        # Predictor — predicts target representations from context
        self.predictor = Predictor(d_model, d_predictor, n_predictor_heads, n_predictor_layers, max_patches)

        # Fine-tuning head (for downstream prediction)
        self.cls_token = nn.Parameter(torch.randn(1, 1, d_model) * 0.02)
        self.fine_tune_head = nn.Sequential(
            nn.Linear(d_model, d_model // 2),
            nn.GELU(),
            nn.Dropout(0.2),
            nn.Linear(d_model // 2, 1),
        )

    def _init_target_encoder(self):
        for p_tgt, p_ctx in zip(self.target_encoder.parameters(), self.context_encoder.parameters()):
            p_tgt.data.copy_(p_ctx.data)
            p_tgt.requires_grad = False

    @torch.no_grad()
    def _update_target_encoder(self):
        for p_tgt, p_ctx in zip(self.target_encoder.parameters(), self.context_encoder.parameters()):
            p_tgt.data.mul_(self.ema_decay).add_(p_ctx.data, alpha=1 - self.ema_decay)

    def _generate_mask(self, n_patches, mask_ratio=0.6):
        """Generate causal-aware mask: always mask last 30% + random 30% of rest"""
        n_future = max(1, int(n_patches * 0.3))
        n_random = max(1, int((n_patches - n_future) * 0.3))
        # Always mask future
        future_mask = list(range(n_patches - n_future, n_patches))
        # Random mask from past/present
        past_indices = list(range(n_patches - n_future))
        random_mask = sorted(torch.randperm(len(past_indices))[:n_random].tolist())
        random_mask = [past_indices[i] for i in random_mask]
        return sorted(set(future_mask + random_mask))

    def pretrain_forward(self, x):
        """Pre-training forward pass: predict masked representations"""
        # Embed patches
        patches = self.patch_embed(x)
        n_patches = patches.size(1)
        patches = self.pos_enc(patches)

        # Generate mask
        mask_indices = self._generate_mask(n_patches)
        visible_indices = [i for i in range(n_patches) if i not in mask_indices]

        # Context encoder: only visible patches
        context_input = patches[:, visible_indices, :]
        context_embeds = self.context_encoder(context_input)

        # Target encoder: full sequence (no gradients)
        with torch.no_grad():
            target_embeds = self.target_encoder(patches)
            target_masked = target_embeds[:, mask_indices, :].detach()

        # Predictor: predict masked representations
        predicted = self.predictor(context_embeds, mask_indices, n_patches)

        return predicted, target_masked, context_embeds

    def get_embeddings(self, x):
        """Extract embeddings for downstream use (fine-tuning or XGBoost)"""
        patches = self.patch_embed(x)
        patches = self.pos_enc(patches)
        # Add CLS token
        cls = self.cls_token.expand(x.size(0), -1, -1)
        tokens = torch.cat([cls, patches], dim=1)
        encoded = self.context_encoder(tokens)
        return encoded[:, 0, :]  # CLS token representation

    def forward(self, x):
        """Fine-tuning forward: embeddings → prediction"""
        embeds = self.get_embeddings(x)
        return self.fine_tune_head(embeds).squeeze(-1), embeds


def vicreg_loss(z1, z2, sim_weight=25.0, var_weight=25.0, cov_weight=1.0):
    """VICReg regularization to prevent representation collapse"""
    # Invariance (MSE between representations)
    sim_loss = F.mse_loss(z1, z2)

    # Variance: keep std of each dimension above threshold
    std_z1 = torch.sqrt(z1.var(dim=0) + 1e-4)
    std_z2 = torch.sqrt(z2.var(dim=0) + 1e-4)
    var_loss = torch.mean(F.relu(1.0 - std_z1)) + torch.mean(F.relu(1.0 - std_z2))

    # Covariance: decorrelate dimensions
    z1_centered = z1 - z1.mean(dim=0)
    z2_centered = z2 - z2.mean(dim=0)
    N = z1.size(0)
    cov_z1 = (z1_centered.T @ z1_centered) / (N - 1)
    cov_z2 = (z2_centered.T @ z2_centered) / (N - 1)
    d = z1.size(1)
    # Off-diagonal elements
    off_diag_z1 = cov_z1.flatten()[:-1].view(d - 1, d + 1)[:, 1:].flatten()
    off_diag_z2 = cov_z2.flatten()[:-1].view(d - 1, d + 1)[:, 1:].flatten()
    cov_loss = (off_diag_z1.pow(2).sum() + off_diag_z2.pow(2).sum()) / d

    return sim_weight * sim_loss + var_weight * var_loss + cov_weight * cov_loss
