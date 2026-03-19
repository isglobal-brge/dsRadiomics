#!/usr/bin/env python3
"""dsRadiomics feature extraction runner.

Reads the dsImaging registry to find image/mask roots for the dataset.
Falls back to matching files in the input directory.
"""

import argparse
import json
import os
import sys


def find_pairs_from_roots(image_root, mask_root):
    """Match image-mask pairs by filename."""
    if not os.path.isdir(image_root) or not os.path.isdir(mask_root):
        return []
    images = {os.path.splitext(f)[0]: os.path.join(image_root, f) for f in os.listdir(image_root)}
    pairs = []
    for f in os.listdir(mask_root):
        name = os.path.splitext(f)[0]
        if name in images:
            pairs.append((images[name], os.path.join(mask_root, f), name))
    return sorted(pairs, key=lambda x: x[2])


def find_dataset_roots(dataset_id=None):
    """Read dsImaging registry to find image/mask roots."""
    registry_path = "/var/lib/dsimaging/registry.yaml"
    if not os.path.exists(registry_path):
        return None, None

    try:
        import yaml
        registry = yaml.safe_load(open(registry_path))
        for ds_id, entry in registry.items():
            if dataset_id and ds_id != dataset_id:
                continue
            manifest_path = entry.get("manifest", "")
            if not os.path.exists(manifest_path):
                continue
            manifest = yaml.safe_load(open(manifest_path))
            assets = manifest.get("assets", {})
            image_root = assets.get("images", {}).get("root")
            mask_root = assets.get("masks", {}).get("root")
            if image_root and mask_root:
                return image_root, mask_root
    except Exception as e:
        print(f"  Warning: registry read failed: {e}")
    return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--settings", default=None)
    args = parser.parse_args()

    print("dsRadiomics extraction")

    # Find image/mask roots from dsImaging registry
    dataset_id = os.environ.get("DSJOBS_CFG_DATASET_ID", "")
    image_root, mask_root = find_dataset_roots(dataset_id or None)

    if image_root and mask_root:
        print(f"  Image root: {image_root}")
        print(f"  Mask root: {mask_root}")
        pairs = find_pairs_from_roots(image_root, mask_root)
    else:
        # Fallback: look for *image*/*label* in input dir
        pairs = []
        for f in os.listdir(args.input):
            if "image" in f.lower():
                for m in os.listdir(args.input):
                    if "label" in m.lower() or "mask" in m.lower():
                        pairs.append((os.path.join(args.input, f), os.path.join(args.input, m), os.path.splitext(f)[0]))
                        break

    print(f"  Found {len(pairs)} image-mask pairs")
    if not pairs:
        print("ERROR: No image-mask pairs found", file=sys.stderr)
        sys.exit(1)

    from radiomics import featureextractor
    import pandas as pd

    if args.settings and args.settings != "default" and os.path.exists(args.settings):
        extractor = featureextractor.RadiomicsFeatureExtractor(args.settings)
    else:
        extractor = featureextractor.RadiomicsFeatureExtractor()

    results = []
    for img, mask, sid in pairs:
        try:
            print(f"  Extracting: {sid}")
            result = extractor.execute(img, mask)
            features = {k: float(v) for k, v in result.items() if not k.startswith("diagnostics")}
            features["sample_id"] = sid
            results.append(features)
        except Exception as e:
            print(f"  FAILED {sid}: {e}", file=sys.stderr)

    if not results:
        print("ERROR: No features extracted", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(results)
    os.makedirs(args.output, exist_ok=True)
    try:
        df.to_parquet(os.path.join(args.output, "radiomics.parquet"), index=False)
        fmt = "parquet"
    except Exception:
        df.to_csv(os.path.join(args.output, "radiomics.csv"), index=False)
        fmt = "csv"

    summary = {"n_samples": len(results), "n_features": len(df.columns)-1,
               "format": fmt, "columns": list(df.columns)}
    with open(os.path.join(args.output, "extraction_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"  Saved: {summary['n_samples']} samples x {summary['n_features']} features ({fmt})")


if __name__ == "__main__":
    main()
