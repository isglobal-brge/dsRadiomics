#!/usr/bin/env python3
"""nnU-Net v2 inference runner for dsRadiomics.

Uses site-registered nnU-Net model packs for segmentation.
"""
import argparse, json, os, sys


def find_images(input_dir):
    registry_path = "/var/lib/dsimaging/registry.yaml"
    dataset_id = os.environ.get("DSJOBS_CFG_DATASET_ID", "")
    if os.path.exists(registry_path):
        try:
            import yaml
            registry = yaml.safe_load(open(registry_path))
            for ds_id, entry in registry.items():
                if dataset_id and ds_id != dataset_id:
                    continue
                manifest = yaml.safe_load(open(entry["manifest"]))
                root = manifest.get("assets", {}).get("images", {}).get("root")
                if root and os.path.isdir(root):
                    return [(os.path.join(root, f), os.path.splitext(f)[0])
                            for f in sorted(os.listdir(root)) if not f.startswith(".")]
        except Exception:
            pass
    return [(os.path.join(input_dir, f), os.path.splitext(f)[0])
            for f in sorted(os.listdir(input_dir))
            if not f.startswith(".") and os.path.isfile(os.path.join(input_dir, f))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--model", required=True)
    args = parser.parse_args()

    models_dir = os.environ.get("DSRADIOMICS_MODELS", "/var/lib/dsradiomics/models")
    model_path = os.path.join(models_dir, "nnunetv2", args.model)

    print(f"nnU-Net v2 inference")
    print(f"  Model: {args.model}")
    print(f"  Model path: {model_path}")

    if not os.path.isdir(model_path):
        print(f"ERROR: Model not found at {model_path}", file=sys.stderr)
        print("Install with: dsRadiomics::install_model('nnunetv2', '<model_name>')", file=sys.stderr)
        sys.exit(1)

    images = find_images(args.input)
    print(f"  Found {len(images)} images")
    os.makedirs(args.output, exist_ok=True)

    # nnU-Net prediction
    from nnunetv2.inference.predict_from_raw_data import nnUNetPredictor

    predictor = nnUNetPredictor()
    predictor.initialize_from_trained_model_folder(model_path)

    # nnU-Net expects a specific input format -- create temp folder
    import shutil, tempfile
    tmpdir = tempfile.mkdtemp()
    try:
        for img_path, sample_id in images:
            shutil.copy(img_path, os.path.join(tmpdir, f"{sample_id}_0000.nii.gz"))
        predictor.predict_from_files(tmpdir, args.output)
    finally:
        shutil.rmtree(tmpdir)

    summary = {"n_total": len(images), "model": args.model}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Done: {len(images)} images processed")


if __name__ == "__main__":
    main()
