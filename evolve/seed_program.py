"""
Utilities for working with seed programs and evolution blocks.

Evolution blocks are marked with:
    # EVOLVE-BLOCK-START: BLOCK_NAME
    ... mutable code ...
    # EVOLVE-BLOCK-END: BLOCK_NAME

Code outside these markers is the fixed skeleton.
These utilities are model-agnostic — they work on any code with the markers.
"""


def extract_evolution_blocks(code: str) -> dict[str, str]:
    """Extract named evolution blocks from code."""
    blocks = {}
    lines = code.split("\n")
    current_block = None
    current_lines = []

    for line in lines:
        if "# EVOLVE-BLOCK-START:" in line:
            current_block = line.split("# EVOLVE-BLOCK-START:")[1].strip()
            current_lines = []
        elif "# EVOLVE-BLOCK-END:" in line:
            if current_block:
                blocks[current_block] = "\n".join(current_lines)
                current_block = None
                current_lines = []
        elif current_block is not None:
            current_lines.append(line)

    return blocks


def replace_evolution_block(code: str, block_name: str, new_content: str) -> str:
    """Replace a named evolution block with new content."""
    lines = code.split("\n")
    result = []
    skip = False

    for line in lines:
        if f"# EVOLVE-BLOCK-START: {block_name}" in line:
            result.append(line)
            result.append(new_content.rstrip())
            skip = True
        elif f"# EVOLVE-BLOCK-END: {block_name}" in line:
            result.append(line)
            skip = False
        elif not skip:
            result.append(line)

    return "\n".join(result)


def list_evolution_blocks(code: str) -> list[str]:
    """Return the names of all evolution blocks in the code."""
    return list(extract_evolution_blocks(code).keys())


def detect_code_features(code: str) -> dict:
    """
    Detect high-level code features for MAP-elites behavioural binning.
    Works on any Python ML code — not tied to a specific model.
    """
    features = {}
    cl = code.lower()

    # Architecture patterns
    features["has_attention"] = "attention" in cl and "self" in cl
    features["has_gating"] = "gate" in cl or "gating" in cl or "squeeze" in cl
    features["has_residual"] = "+=" in code or "residual" in cl or "skip" in cl
    features["has_dropout"] = "dropout" in cl
    features["has_batchnorm"] = "batchnorm" in cl or "batch_norm" in cl
    features["has_layernorm"] = "layernorm" in cl or "layer_norm" in cl

    # Loss patterns
    features["has_focal_loss"] = "focal" in cl
    features["has_contrastive"] = "contrastive" in cl or "centroid" in cl or "triplet" in cl
    features["has_label_smoothing"] = "label_smooth" in cl or "smoothing" in cl

    # Activation
    for act in ["gelu", "silu", "swish", "mish", "prelu", "leakyrelu", "elu", "relu"]:
        if act in cl:
            features["activation"] = act
            break
    else:
        features["activation"] = "unknown"

    # Framework
    if "import torch" in code or "nn.Module" in code:
        features["framework"] = "pytorch"
    elif "import tensorflow" in code or "keras" in code:
        features["framework"] = "tensorflow"
    elif "import sklearn" in code or "from sklearn" in code:
        features["framework"] = "sklearn"
    else:
        features["framework"] = "other"

    # Complexity proxy: count of nn.Linear / Dense / layer definitions
    features["layer_count"] = code.count("nn.Linear") + code.count("Dense(") + code.count("nn.Conv")

    return features
