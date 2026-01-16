def evaluate(bst, dtest, params, top_n=25, run_dir=None):
    """
    Evaluates the model, prints key metrics, and optionally saves:
      - metrics.json
      - preds.npy
      - labels.npy
      - feature_importance_gain.json
      - params.json
    """

    from sklearn.metrics import average_precision_score, roc_auc_score
    import numpy as np
    import json

    if run_dir is not None:
        run_dir.mkdir(parents=True, exist_ok=True)

    # --- Predictions ---
    preds = bst.predict(dtest)
    labels = dtest.get_label()

    # --- Metrics ---
    prauc = float(average_precision_score(labels, preds))
    rocauc = float(roc_auc_score(labels, preds))
    event_rate = float(labels.mean())
    lift = float(prauc / event_rate)

    print("\n=============================")
    print("         FINAL TEST METRICS")
    print("=============================")
    print(f"Event rate (baseline PR-AUC): {event_rate:.6f}")
    print(f"PR-AUC:                       {prauc:.6f}")
    print(f"ROC-AUC:                      {rocauc:.6f}")
    print(f"PR-AUC lift over baseline:    {lift:.2f}x")
    print("=============================\n")

    # --- Save metrics + params ---
    if run_dir:
        metrics = {
            "event_rate": event_rate,
            "prauc": prauc,
            "rocauc": rocauc,
            "lift": lift,
        }
        json.dump(metrics, open(run_dir / "metrics.json", "w"), indent=2)
        json.dump(params, open(run_dir / "params.json", "w"), indent=2)

        np.save(run_dir / "preds.npy", preds)
        np.save(run_dir / "labels.npy", labels)

    # --- Feature Importances ---
    importance = sorted(
        bst.get_score(importance_type="gain").items(),
        key=lambda x: x[1],
        reverse=True
    )

    print("\n=============================")
    print(f" TOP {top_n} FEATURE IMPORTANCES (gain)")
    print("=============================")

    for feat, score in importance[:top_n]:
        print(f"{feat:40s} gain={score:.6f}")

    if run_dir:
        json.dump(
            [{"feature": f, "gain": float(s)} for f, s in importance],
            open(run_dir / "feature_importance_gain.json", "w"),
            indent=2
        )

    return {
        "prauc": prauc,
        "rocauc": rocauc,
        "event_rate": event_rate,
        "lift": lift,
        "feature_importance": importance,
    }