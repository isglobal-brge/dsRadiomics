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
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
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

    # Merge CLI args with env vars (dsJobs sets DSJOBS_CFG_* from config)
    image = args.image or os.environ.get("DSJOBS_CFG_IMAGE")
    sample_id = getattr(args, "sample_id", None) or os.environ.get("DSJOBS_CFG_SAMPLE_ID")

    if image:
        sid = sample_id or os.path.splitext(os.path.basename(image))[0]
        images = [(image, sid)]
        print(f"  Single-image mode: {sid}")
    else:
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

    # Write seg_manifest.json
    seg_manifest = {"provider": "nnunetv2", "model": args.model, "samples": {}}
    for img_path, sid in images:
        mask_path = os.path.join(args.output, f"{sid}.nii.gz")
        if os.path.exists(mask_path):
            seg_manifest["samples"][sid] = {
                "sample_id": sid, "primary_mask": mask_path,
                "mask_files": [mask_path], "status": "done"
            }
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(seg_manifest, f, indent=2)

    print(f"  Done: {len(images)} images processed")


if __name__ == "__main__":
    main()
