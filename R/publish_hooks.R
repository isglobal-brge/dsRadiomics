# Module: dsJobs Publisher Hook
# Called by dsJobs when a publish_asset step with publish_kind = "radiomics_asset"
# or "imaging_asset" completes.

#' Radiomics asset publisher (dsJobs plugin)
#' @keywords internal
.radiomics_publisher <- function(job_id, step, output_dir, db) {
  dataset_id <- step$dataset_id
  asset_name <- step$asset_name
  asset_type <- step$asset_type %||% "feature_table"

  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    warning("dsImaging required for asset publishing.", call. = FALSE)
    return(list(status = "skipped"))
  }

  # Compute derivation hash from step config
  deriv_hash <- NULL
  if (!is.null(step$config)) {
    deriv_hash <- dsImaging::compute_derivation_hash(
      dataset_id = dataset_id,
      runner = step$runner %||% "pyradiomics",
      config = step$config
    )
  }

  # Register as derived asset
  asset_id <- dsImaging::register_derived_asset(
    dataset_id = dataset_id,
    kind = asset_type,
    path_or_root = output_dir,
    derivation_hash = deriv_hash,
    provenance = list(
      runner = step$runner %||% "pyradiomics",
      job_id = job_id,
      config = step$config
    ),
    created_by_job = job_id,
    description = step$description %||% paste(asset_type, "from job", job_id),
    alias = step$alias
  )

  list(status = "published", asset_id = asset_id,
       dataset_id = dataset_id, kind = asset_type)
}
