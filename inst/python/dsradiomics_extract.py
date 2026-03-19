#!/usr/bin/env python3
"""dsRadiomics feature extraction runner.

Called by dsJobs artifact runner with:
  python -m dsradiomics_extract --input <dir> --output <dir> --settings <file>

Input dir must contain:
  - images/ (directory with image files)
  - masks/ (directory with mask files, matched by filename)
  OR
  - manifest.json with image_path, mask_path per sample

Output dir receives:
  - radiomics.parquet (feature table)
  - radiomics.csv (fallback)
  - extraction_summary.json
"""

import argparse
import json
import os
import sys
import traceback

def find_matched_pairs(input_dir):
    """Find image-mask pairs from input directory."""
    pairs = []

    # Check for manifest
    manifest_path = os.path.join(input_dir, "manifest.json")
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        if "samples" in manifest:
            for s in manifest["samples"]:
                pairs.append((s["image_path"], s["mask_path"], s.get("sample_id", os.path.basename(s["image_path"]))))
            return pairs

    # Auto-match by filename
    images_dir = os.path.join(input_dir, "images")
    masks_dir = os.path.join(input_dir, "masks")

    if not os.path.isdir(images_dir):
        # Single image/mask pair (test mode)
        for f in os.listdir(input_dir):
            if "image" in f.lower():
                img = os.path.join(input_dir, f)
                for m in os.listdir(input_dir):
                    if "label" in m.lower() or "mask" in m.lower():
                        mask = os.path.join(input_dir, m)
                        pairs.append((img, mask, os.path.splitext(f)[0]))
                        break
        return pairs

    image_files = {os.path.splitext(f)[0]: os.path.join(images_dir, f)
                   for f in os.listdir(images_dir)}
    mask_files = {os.path.splitext(f)[0]: os.path.join(masks_dir, f)
                  for f in os.listdir(masks_dir)}

    for name in sorted(set(image_files) & set(mask_files)):
        pairs.append((image_files[name], mask_files[name], name))

    return pairs


def extract_features(pairs, settings_path=None):
    """Run PyRadiomics on all pairs."""
    from radiomics import featureextractor

    if settings_path and os.path.exists(settings_path):
        extractor = featureextractor.RadiomicsFeatureExtractor(settings_path)
    else:
        extractor = featureextractor.RadiomicsFeatureExtractor()

    results = []
    for image_path, mask_path, sample_id in pairs:
        try:
            result = extractor.execute(image_path, mask_path)
            features = {k: float(v) for k, v in result.items()
                       if not k.startswith("diagnostics")}
            features["sample_id"] = sample_id
            results.append(features)
        except Exception as e:
            print(f"  FAILED {sample_id}: {e}", file=sys.stderr)

    return results


def save_results(results, output_dir):
    """Save as Parquet (preferred) or CSV."""
    import pandas as pd
    df = pd.DataFrame(results)

    os.makedirs(output_dir, exist_ok=True)

    try:
        df.to_parquet(os.path.join(output_dir, "radiomics.parquet"), index=False)
        fmt = "parquet"
    except Exception:
        df.to_csv(os.path.join(output_dir, "radiomics.csv"), index=False)
        fmt = "csv"

    summary = {
        "n_samples": len(results),
        "n_features": len(df.columns) - 1,  # minus sample_id
        "format": fmt,
        "columns": list(df.columns)
    }
    with open(os.path.join(output_dir, "extraction_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--settings", default=None)
    args = parser.parse_args()

    print(f"dsRadiomics extraction")
    print(f"  Input: {args.input}")
    print(f"  Output: {args.output}")
    print(f"  Settings: {args.settings or 'default'}")

    pairs = find_matched_pairs(args.input)
    print(f"  Found {len(pairs)} image-mask pairs")

    if not pairs:
        print("ERROR: No image-mask pairs found", file=sys.stderr)
        sys.exit(1)

    results = extract_features(pairs, args.settings)
    print(f"  Extracted features from {len(results)}/{len(pairs)} samples")

    if not results:
        print("ERROR: No features extracted", file=sys.stderr)
        sys.exit(1)

    summary = save_results(results, args.output)
    print(f"  Saved: {summary['n_samples']} samples x {summary['n_features']} features ({summary['format']})")


if __name__ == "__main__":
    main()
