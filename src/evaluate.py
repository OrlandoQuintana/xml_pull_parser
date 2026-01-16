def build_experiments(base_params):
    EXPERIMENTS = []

    # ---- Baseline ----
    EXPERIMENTS.append({
        **base_params,
        "name": "baseline"
    })

    # ---- Sweep scale_pos_weight ----
    for spw in [1, 2, 5, 10, 20, 50, 100]:
        EXPERIMENTS.append({
            **base_params,
            "scale_pos_weight": spw,
            "name": f"spw_{spw}"
        })

    # ---- Sweep max_depth ----
    for depth in [3, 4, 6, 8, 10, 12]:
        EXPERIMENTS.append({
            **base_params,
            "max_depth": depth,
            "name": f"depth_{depth}"
        })

    # ---- Sweep learning rate eta ----
    for eta in [0.3, 0.1, 0.05, 0.02, 0.01]:
        EXPERIMENTS.append({
            **base_params,
            "eta": eta,
            "name": f"eta_{eta}"
        })

    # ---- Sweep subsample ----
    for ss in [0.5, 0.7, 0.9, 1.0]:
        EXPERIMENTS.append({
            **base_params,
            "subsample": ss,
            "name": f"subsample_{ss}"
        })

    # ---- Sweep colsample_bytree ----
    for cs in [0.5, 0.7, 0.9, 1.0]:
        EXPERIMENTS.append({
            **base_params,
            "colsample_bytree": cs,
            "name": f"colsample_{cs}"
        })

    # ---- Sweep max_bin ----
    for mb in [64, 128, 256]:
        EXPERIMENTS.append({
            **base_params,
            "max_bin": mb,
            "name": f"maxbin_{mb}"
        })

    # ---- Sweep min_child_weight ----
    for mcw in [1, 5, 10, 20]:
        EXPERIMENTS.append({
            **base_params,
            "min_child_weight": mcw,
            "name": f"mcw_{mcw}"
        })

    return EXPERIMENTS