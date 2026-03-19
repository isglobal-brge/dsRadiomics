#!/usr/bin/env python3
"""TotalSegmentator inference runner for dsRadiomics.

Reads images from the dsImaging registry and produces mask NIfTI files.
"""
import argparse, json, os, sys, glob


def find_images(input_dir):
    """Find images from dsImaging registry or input dir."""
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
                image_root = manifest.get("assets", {}).get("images", {}).get("root")
                if image_root and os.path.isdir(image_root):
                    return [(os.path.join(image_root, f), os.path.splitext(f)[0])
                            for f in sorted(os.listdir(image_root))
                            if not f.startswith(".")]
        except Exception as e:
            print(f"  Registry warning: {e}")

    # Fallback
    return [(os.path.join(input_dir, f), os.path.splitext(f)[0])
            for f in sorted(os.listdir(input_dir))
            if not f.startswith(".") and os.path.isfile(os.path.join(input_dir, f))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task", default="total")
    args = parser.parse_args()

    print(f"TotalSegmentator inference")
    print(f"  Task: {args.task}")

    models_dir = os.environ.get("DSRADIOMICS_MODELS", "/var/lib/dsradiomics/models")
    os.environ["TOTALSEG_WEIGHTS_PATH"] = os.path.join(models_dir, "totalsegmentator", args.task)

    images = find_images(args.input)
    print(f"  Found {len(images)} images")
    os.makedirs(args.output, exist_ok=True)

    from totalsegmentator.python_api import totalsegmentator

    results = []
    for img_path, sample_id in images:
        try:
            print(f"  Segmenting: {sample_id}")
            out_dir = os.path.join(args.output, sample_id)
            os.makedirs(out_dir, exist_ok=True)
            totalsegmentator(img_path, out_dir, task=args.task)
            results.append({"sample_id": sample_id, "status": "done"})
        except Exception as e:
            print(f"  FAILED {sample_id}: {e}", file=sys.stderr)
            results.append({"sample_id": sample_id, "status": "failed", "error": str(e)})

    summary = {"n_total": len(images), "n_done": sum(1 for r in results if r["status"] == "done"),
               "n_failed": sum(1 for r in results if r["status"] == "failed"), "task": args.task}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Done: {summary['n_done']}/{summary['n_total']} ({summary['n_failed']} failed)")


if __name__ == "__main__":
    main()
