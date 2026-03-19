#!/usr/bin/env python3
"""LungMask inference runner for dsRadiomics.

Produces lung/lobe segmentation masks from CT images.
Models: R231, LTRCLobes, LTRCLobes_R231, R231CovidWeb
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
    parser.add_argument("--model", default="R231")
    args = parser.parse_args()

    print(f"LungMask inference")
    print(f"  Model: {args.model}")

    images = find_images(args.input)
    print(f"  Found {len(images)} images")
    os.makedirs(args.output, exist_ok=True)

    import SimpleITK as sitk
    from lungmask import LMInferer

    inferer = LMInferer(modelname=args.model)
    results = []
    for img_path, sample_id in images:
        try:
            print(f"  Segmenting: {sample_id}")
            image = sitk.ReadImage(img_path)
            mask = inferer.apply(image)
            # Save mask as NIfTI
            mask_sitk = sitk.GetImageFromArray(mask)
            mask_sitk.CopyInformation(image)
            out_path = os.path.join(args.output, f"{sample_id}_lungmask.nii.gz")
            sitk.WriteImage(mask_sitk, out_path)
            results.append({"sample_id": sample_id, "status": "done"})
        except Exception as e:
            print(f"  FAILED {sample_id}: {e}", file=sys.stderr)
            results.append({"sample_id": sample_id, "status": "failed", "error": str(e)})

    summary = {"n_total": len(images), "n_done": sum(1 for r in results if r["status"] == "done"),
               "n_failed": sum(1 for r in results if r["status"] == "failed"), "model": args.model}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Done: {summary['n_done']}/{summary['n_total']}")


if __name__ == "__main__":
    main()
