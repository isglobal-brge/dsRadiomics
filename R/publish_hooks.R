# Module: dsJobs Publisher Hooks
# Called by dsJobs when a publish_asset step completes.

#' Radiomics asset publisher (dsJobs plugin) -- collection-level
#' @keywords internal
.radiomics_publisher <- function(job_id, step, output_dir, db) {
  dataset_id <- step$dataset_id
  asset_name <- step$asset_name
  asset_type <- step$asset_type %||% "feature_table"

  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    warning("dsImaging required for asset publishing.", call. = FALSE)
    return(list(status = "skipped"))
  }

  deriv_hash <- NULL
  if (!is.null(step$config)) {
    deriv_hash <- dsImaging::compute_derivation_hash(
      dataset_id = dataset_id,
      runner = step$runner %||% "pyradiomics",
      config = step$config
    )
  }

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

#' Per-image result publisher (dsJobs plugin)
#'
#' Called when a per-image job completes its publish step.
#' Four responsibilities:
#'   1. Record item as completed in the generation
#'   2. Atomically increment completed_n counter
#'   3. Register per-image result as individual asset (cross-user dedup)
#'   4. Auto-submit next batch of pending images (server-side drip feed)
#'
#' Step 4 is what makes the system "fire and forget": the user kicks off
#' the first batch, then the server self-sustains by submitting more work
#' as slots free up. No client connection required.
#' @keywords internal
.radiomics_image_publisher <- function(job_id, step, output_dir, db) {
  config <- step$config
  generation_id <- config$generation_id
  sample_id <- config$sample_id
  dataset_id <- config$dataset_id
  spec_hash <- config$spec_hash

  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    warning("dsImaging required for per-image publishing.", call. = FALSE)
    return(list(status = "skipped"))
  }

  # Find the artifact path (output from previous extraction step)
  artifact_relpath <- NULL
  if (!is.null(output_dir) && dir.exists(output_dir)) {
    files <- list.files(output_dir, recursive = TRUE)
    parquet <- files[grepl("\\.parquet$", files)]
    csv <- files[grepl("\\.csv$", files)]
    nifti <- files[grepl("\\.nii(\\.gz)?$", files)]
    artifact_relpath <- if (length(parquet) > 0) parquet[1]
                        else if (length(csv) > 0) csv[1]
                        else if (length(nifti) > 0) nifti[1]
                        else if (length(files) > 0) files[1]
                        else NULL
    if (!is.null(artifact_relpath))
      artifact_relpath <- file.path(output_dir, artifact_relpath)
  }

  # Single atomic transaction: item status + counter + asset registration
  asset_reg <- NULL
  if (!is.null(spec_hash) && !is.null(artifact_relpath)) {
    asset_reg <- list(
      dataset_id = dataset_id,
      kind = "per_image_result",
      path_or_root = artifact_relpath,
      derivation_hash = spec_hash,
      provenance = list(type = "per_image", job_id = job_id,
                         generation_id = generation_id, sample_id = sample_id),
      created_by_job = job_id
    )
  }

  dsImaging::complete_item_atomic(
    generation_id = generation_id,
    sample_id = sample_id,
    status = "completed",
    artifact_relpath = artifact_relpath,
    register_asset = asset_reg
  )

  # 4. Server-side drip feed: auto-submit next batch of pending images
  tryCatch(
    .drip_feed_next_batch(generation_id, dataset_id),
    error = function(e) NULL  # never let drip-feed failure block the publisher
  )

  list(status = "published", generation_id = generation_id,
       sample_id = sample_id, job_id = job_id)
}

#' Auto-submit next batch of pending images from the generation spec
#'
#' Called from the publisher hook after each job completes.
#' Checks if there are pending items that haven't been submitted yet,
#' and submits a batch if there are free slots.
#'
#' The generation's spec_json stores the full orchestration config
#' (segmenter, profile, fingerprints for pending items). The drip feeder
#' reads this to know what to submit next.
#' @keywords internal
.drip_feed_next_batch <- function(generation_id, dataset_id) {
  if (!requireNamespace("dsJobs", quietly = TRUE)) return(invisible(NULL))

  gen <- dsImaging::get_generation(generation_id)
  if (is.null(gen) || !gen$state %in% c("RUNNING", "PENDING")) return(invisible(NULL))

  # Check how many per-image jobs for this generation are currently active
  dsjobs_db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(dsjobs_db), add = TRUE)
  active_n <- DBI::dbGetQuery(dsjobs_db,
    "SELECT COUNT(*) AS n FROM jobs
     WHERE state IN ('PENDING','RUNNING') AND tags LIKE ?",
    params = list(paste0("%", generation_id, "%")))$n

  max_inflight <- as.integer(getOption("dsradiomics.max_inflight", 30L))
  if (active_n >= max_inflight) return(invisible(NULL))

  # Atomically claim pending items -- prevents duplicate submissions
  # when multiple publisher hooks fire concurrently
  batch_size <- min(
    as.integer(getOption("dsradiomics.batch_size", 10L)),
    max_inflight - active_n)
  if (batch_size <= 0) return(invisible(NULL))

  batch_ids <- dsImaging::claim_pending_items(
    generation_id, batch_size,
    claimer_id = paste0("drip_", Sys.getpid()))
  if (length(batch_ids) == 0) return(invisible(NULL))

  # Read the generation spec to reconstruct submission params
  spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(spec)) return(invisible(NULL))

  segmenter <- spec$segmenter
  profile_name <- spec$profile
  processor <- spec$processor

  # We need fingerprints for these samples -- read from content_fingerprints
  fp_db <- dsImaging::.asset_db_connect()
  on.exit(dsImaging::.asset_db_close(fp_db), add = TRUE)
  fps <- DBI::dbGetQuery(fp_db,
    paste0("SELECT sample_id, fingerprint FROM content_fingerprints
            WHERE dataset_id = ? AND sample_id IN (",
           paste(rep("?", length(batch_ids)), collapse = ","), ")"),
    params = c(list(dataset_id), as.list(batch_ids)))
  fingerprints <- stats::setNames(as.list(fps$fingerprint), fps$sample_id)

  # Resolve paths and submit
  image_root <- .resolve_image_root(dataset_id)
  mask_root <- .resolve_mask_root(dataset_id, segmenter)

  seg_runner <- switch(segmenter$provider,
    existing_mask_asset = NULL,
    totalsegmentator = "totalsegmentator_infer",
    lungmask = "lungmask_infer",
    nnunetv2_predict = "nnunetv2_predict",
    monai_bundle_infer = "monai_bundle_infer",
    NULL)

  # Reconstruct profile as a list with name and bin_width
  profile <- list(name = profile_name, bin_width = 25L)

  for (sid in batch_ids) {
    fp <- fingerprints[[sid]]
    if (is.null(fp)) next

    spec_hash <- dsImaging::compute_image_derivation_hash(
      fingerprint = fp,
      processor = processor,
      params = list(profile = profile_name, bin_width = profile$bin_width)
    )

    # Cross-user dedup
    existing <- dsImaging::find_asset_by_hash(dataset_id, spec_hash)
    if (!is.null(existing)) {
      dsImaging::complete_item_atomic(generation_id, sid, "completed",
        artifact_relpath = existing)
      next
    }

    image_path <- .resolve_sample_image(image_root, sid)
    if (is.null(image_path)) {
      dsImaging::complete_item_atomic(generation_id, sid, "failed",
        error = "Image file not found")
      next
    }

    settings_path <- .resolve_profile_path(profile_name)

    steps <- list()
    steps[[1]] <- list(
      type = "emit", output_name = "image_config",
      value = list(image_path = image_path, sample_id = sid,
                    dataset_id = dataset_id, generation_id = generation_id))

    if (!is.null(seg_runner)) {
      seg_config <- segmenter
      seg_config$image <- image_path
      seg_config$sample_id <- sid
      seg_config$generation_id <- generation_id
      steps[[length(steps) + 1]] <- list(
        type = "segment", runner = seg_runner,
        name = "segment_single", config = seg_config)
    }

    extract_config <- list(
      name = profile_name,
      image = image_path, sample_id = sid,
      generation_id = generation_id,
      settings_file = settings_path %||% "default")
    if (!is.null(mask_root)) {
      mp <- .resolve_sample_mask(mask_root, sid)
      if (!is.null(mp)) extract_config$mask <- mp
    }
    steps[[length(steps) + 1]] <- list(
      type = "extract", runner = "pyradiomics_extract",
      name = "extract_single", config = extract_config)

    steps[[length(steps) + 1]] <- list(
      type = "publish_asset",
      publish_kind = "radiomics_image_result",
      config = list(generation_id = generation_id, sample_id = sid,
                     dataset_id = dataset_id, spec_hash = spec_hash))

    job_spec <- list(
      label = "dsRadiomics_image",
      tags = c("per_image", dataset_id, sid, generation_id),
      visibility = "private", steps = steps,
      .owner = gen$owner_id)

    tryCatch({
      spec_enc <- jsonlite::base64_enc(charToRaw(as.character(
        jsonlite::toJSON(job_spec, auto_unbox = TRUE, null = "null"))))
      dsJobs::jobSubmitDS(spec_enc)
    }, error = function(e) {
      dsImaging::complete_item_atomic(generation_id, sid, "failed",
        error = paste("Drip-feed submit failed:", conditionMessage(e)))
    })
  }

  invisible(NULL)
}
