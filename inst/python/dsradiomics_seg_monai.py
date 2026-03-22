#!/usr/bin/env python3
"""MONAI bundle inference runner for dsRadiomics.

Uses MONAI Model Zoo bundles for segmentation.
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
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    models_dir = os.environ.get("DSRADIOMICS_MODELS", "/var/lib/dsradiomics/models")
    bundle_dir = os.path.join(models_dir, "monai", args.bundle)

    print(f"MONAI bundle inference")
    print(f"  Bundle: {args.bundle}")
    print(f"  Bundle path: {bundle_dir}")

    if not os.path.isdir(bundle_dir):
        print(f"ERROR: Bundle not found at {bundle_dir}", file=sys.stderr)
        print("Install with: dsRadiomics::install_model('monai', '<bundle_name>')", file=sys.stderr)
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

    from monai.bundle import run

    results = []
    for img_path, sample_id in images:
        try:
            print(f"  Inferring: {sample_id}")
            out_path = os.path.join(args.output, f"{sample_id}_seg.nii.gz")
            run(
                runner_id="inference",
                meta_file=os.path.join(bundle_dir, "configs", "metadata.json"),
                config_file=os.path.join(bundle_dir, "configs", "inference.json"),
                logging_file=os.path.join(bundle_dir, "configs", "logging.conf"),
                bundle_root=bundle_dir,
                image=img_path,
                output_dir=os.path.dirname(out_path),
            )
            results.append({"sample_id": sample_id, "status": "done"})
        except Exception as e:
            print(f"  FAILED {sample_id}: {e}", file=sys.stderr)
            results.append({"sample_id": sample_id, "status": "failed", "error": str(e)})

    summary = {"n_total": len(images), "n_done": sum(1 for r in results if r["status"] == "done"),
               "n_failed": sum(1 for r in results if r["status"] == "failed"), "bundle": args.bundle}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # Write seg_manifest.json
    seg_manifest = {"provider": "monai", "bundle": args.bundle, "samples": {}}
    for r in results:
        sid = r["sample_id"]
        if r["status"] == "done":
            mask_path = os.path.join(args.output, f"{sid}_seg.nii.gz")
            seg_manifest["samples"][sid] = {
                "sample_id": sid, "primary_mask": mask_path,
                "mask_files": [mask_path], "status": "done"
            }
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(seg_manifest, f, indent=2)

    print(f"  Done: {summary['n_done']}/{summary['n_total']}")


if __name__ == "__main__":
    main()
