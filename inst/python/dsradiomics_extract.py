#!/usr/bin/env python3
"""dsRadiomics feature extraction runner.

Reads the dsImaging registry to find image/mask roots for the dataset.
Falls back to matching files in the input directory.
"""

import argparse
import json
import os
import sys


def _strip_extensions(filename):
    """Strip known compound extensions (.nii.gz) and simple extensions."""
    for ext in (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm"):
        if filename.lower().endswith(ext):
            return filename[: -len(ext)]
    return os.path.splitext(filename)[0]


def find_pairs_from_roots(image_root, mask_root):
    """Match image-mask pairs by filename."""
    if not os.path.isdir(image_root) or not os.path.isdir(mask_root):
        return []
    images = {_strip_extensions(f): os.path.join(image_root, f)
              for f in os.listdir(image_root) if not f.startswith(".")}
    pairs = []
    for f in os.listdir(mask_root):
        if f.startswith("."):
            continue
        name = _strip_extensions(f)
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


def _find_mask_from_manifest(input_dir, sample_id):
    """Read seg_manifest.json and return primary_mask for the sample.

    This is the canonical path for production. The manifest is written
    by the segmentation step and provides an explicit, unambiguous
    mapping from sample_id to mask file(s).
    """
    manifest_path = os.path.join(input_dir, "seg_manifest.json")
    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        sample_entry = manifest.get("samples", {}).get(sample_id)
        if sample_entry and sample_entry.get("primary_mask"):
            mask = sample_entry["primary_mask"]
            if os.path.exists(mask):
                return mask
    except Exception as e:
        print(f"  Warning: seg_manifest.json read failed: {e}", file=sys.stderr)
    return None


def _find_mask_for_sample(input_dir, sample_id):
    """Find mask file for a sample in the input directory.

    Searches with increasing generality:
    1. Files containing sample_id AND 'mask'/'label' in name
    2. Files inside a subdirectory named after sample_id (TotalSegmentator output)
    3. Any NIfTI file containing sample_id in its name
    4. If only one NIfTI file exists, use it (unambiguous single-image case)
    """
    if not os.path.isdir(input_dir):
        return None

    # Strategy 1: explicit mask/label naming
    for f in os.listdir(input_dir):
        fpath = os.path.join(input_dir, f)
        if os.path.isfile(fpath) and sample_id in f:
            if "mask" in f.lower() or "label" in f.lower():
                return fpath

    # Strategy 2: subdirectory matching sample_id (e.g. TotalSegmentator)
    subdir = os.path.join(input_dir, sample_id)
    if os.path.isdir(subdir):
        nifti_files = [f for f in os.listdir(subdir)
                       if f.endswith((".nii.gz", ".nii"))]
        if nifti_files:
            return os.path.join(subdir, sorted(nifti_files)[0])

    # Strategy 3: any NIfTI containing sample_id
    for f in os.listdir(input_dir):
        fpath = os.path.join(input_dir, f)
        if os.path.isfile(fpath) and sample_id in f:
            if f.endswith((".nii.gz", ".nii", ".nrrd", ".mha")):
                return fpath

    # Strategy 4: single unambiguous NIfTI file
    nifti_all = [f for f in os.listdir(input_dir)
                 if os.path.isfile(os.path.join(input_dir, f))
                 and f.endswith((".nii.gz", ".nii", ".nrrd", ".mha"))]
    if len(nifti_all) == 1:
        return os.path.join(input_dir, nifti_all[0])

    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--settings", default=None)
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--mask", default=None,
                        help="Single mask path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    # Merge CLI args with env vars (dsJobs sets DSJOBS_CFG_* from config)
    image = args.image or os.environ.get("DSJOBS_CFG_IMAGE")
    mask = args.mask or os.environ.get("DSJOBS_CFG_MASK")
    sample_id = getattr(args, "sample_id", None) or os.environ.get("DSJOBS_CFG_SAMPLE_ID")

    print("dsRadiomics extraction")

    # Single-image mode
    if image:
        sid = sample_id or os.path.splitext(os.path.basename(image))[0]
        if not mask:
            # Canonical path: read seg_manifest.json from segmentation output
            mask = _find_mask_from_manifest(args.input, sid)
        if not mask:
            # Fallback: heuristic search (backward compat / manual mask setups)
            print(f"  Note: no seg_manifest.json found, using heuristic mask search",
                  file=sys.stderr)
            mask = _find_mask_for_sample(args.input, sid)
        if not mask:
            print(f"ERROR: No mask found for {sid}", file=sys.stderr)
            sys.exit(1)
        pairs = [(image, mask, sid)]
        print(f"  Single-image mode: {sid}")
    else:
        # Collection mode (original behavior)
        dataset_id = os.environ.get("DSJOBS_CFG_DATASET_ID", "")
        image_root, mask_root = find_dataset_roots(dataset_id or None)

        if image_root and mask_root:
            print(f"  Image root: {image_root}")
            print(f"  Mask root: {mask_root}")
            pairs = find_pairs_from_roots(image_root, mask_root)
        else:
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
            features = {}
            for k, v in result.items():
                if k.startswith("diagnostics"):
                    continue
                try:
                    features[k] = float(v)
                except (TypeError, ValueError):
                    features[k] = str(v)
            features["sample_id"] = sid
            results.append(features)
        except Exception as e:
            print(f"  FAILED {sid}: {e}", file=sys.stderr)

    if not results:
        print("ERROR: No features extracted", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(results)
    os.makedirs(args.output, exist_ok=True)
    df.to_parquet(os.path.join(args.output, "radiomics.parquet"), index=False)

    summary = {"n_samples": len(results), "n_features": len(df.columns)-1,
               "format": "parquet", "columns": list(df.columns)}
    with open(os.path.join(args.output, "extraction_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"  Saved: {summary['n_samples']} samples x {summary['n_features']} features (parquet)")


if __name__ == "__main__":
    main()
