# Module: Per-Image Collection Orchestrator
# Server-side DS methods for per-image job deduplication.
#
# Flow:
#   1. radiomicsScanCollectionDS  -- fingerprint + diff + create generation
#   2. radiomicsSubmitBatchDS     -- create N dsJobs jobs for pending images
#   3. radiomicsCollectionStatusDS -- query generation progress (with failure sync)
#   4. radiomicsPublishCollectionDS -- aggregate per-image outputs

# ---------------------------------------------------------------------------
# 1. Scan: fingerprint images, diff vs completed, create/reuse generation
# ---------------------------------------------------------------------------

#' @export
radiomicsScanCollectionDS <- function(dataset_id_enc, segmenter_enc,
                                      profile_enc, visibility_enc) {
  dataset_id <- .dsr_decode(dataset_id_enc)
  segmenter  <- .dsr_decode(segmenter_enc)
  profile    <- .dsr_decode(profile_enc)
  visibility <- .dsr_decode(visibility_enc)

  if (!requireNamespace("dsImaging", quietly = TRUE))
    stop("dsImaging required", call. = FALSE)

  # Resolve image root from manifest
  image_root <- .resolve_image_root(dataset_id)
  if (is.null(image_root) || !dir.exists(image_root))
    stop("Cannot resolve image root for dataset: ", dataset_id, call. = FALSE)

  # Fingerprint all images (with strong content hash for dedup)
  fp_result <- dsImaging::compute_collection_fingerprints(dataset_id, image_root,
    compute_content_hash = TRUE)

  # Build processor identity for derivation hashing
  processor <- paste0(segmenter$provider, "_", segmenter$task %||% "default")

  # Collection-level hash uses content_hashes (strong) when available
  hash_source <- if (length(fp_result$content_hashes) > 0)
    fp_result$content_hashes else fp_result$fingerprints
  collection_hash <- dsImaging::compute_derivation_hash(
    dataset_id = dataset_id,
    processor = processor,
    profile = profile$name,
    bin_width = profile$bin_width,
    image_hashes = sort(unlist(hash_source))
  )

  # Claim or reuse generation
  pending_ids <- c(fp_result$new, fp_result$changed)
  total_n <- fp_result$total

  gen_result <- dsImaging::claim_or_reuse_generation(
    dataset_id = dataset_id,
    kind = "radiomics_collection",
    derivation_hash = collection_hash,
    visibility = visibility,
    owner_id = .dsr_owner_id(),
    expected_n = total_n,
    spec = list(
      processor = processor,
      profile = profile$name,
      segmenter = segmenter
    )
  )

  if (gen_result$action == "reuse_asset") {
    return(list(
      action = "reuse_asset",
      asset_id = gen_result$asset_id,
      total = total_n, done = total_n, pending = 0L
    ))
  }

  generation_id <- gen_result$generation_id

  if (gen_result$action == "reuse_generation") {
    # Resume: sync failed jobs first, then check which items are pending
    .sync_failed_jobs(generation_id)
    items <- dsImaging::get_generation_items(generation_id)
    done_ids <- items$sample_id[items$status == "completed"]
    failed_ids <- items$sample_id[items$status == "failed"]
    pending_ids <- items$sample_id[items$status == "pending"]
    return(list(
      action = "resume",
      generation_id = generation_id,
      total = total_n,
      done = length(done_ids),
      failed = length(failed_ids),
      pending = length(pending_ids),
      pending_ids = pending_ids,
      fingerprints = fp_result$fingerprints,
      content_hashes = fp_result$content_hashes
    ))
  }

  # New generation: register all items
  for (sid in names(fp_result$fingerprints)) {
    status <- if (sid %in% pending_ids) "pending" else "completed"
    dsImaging::record_item_status(generation_id, sid, status)
  }

  dsImaging::update_generation(generation_id,
    state = "RUNNING",
    completed_n = length(fp_result$unchanged))

  list(
    action = "run_new",
    generation_id = generation_id,
    total = total_n,
    done = length(fp_result$unchanged),
    pending = length(pending_ids),
    pending_ids = pending_ids,
    fingerprints = fp_result$fingerprints,
    content_hashes = fp_result$content_hashes
  )
}

# ---------------------------------------------------------------------------
# 2. Submit batch: create per-image dsJobs jobs for pending samples
# ---------------------------------------------------------------------------

#' @export
radiomicsSubmitBatchDS <- function(generation_id_enc, sample_ids_enc,
                                    segmenter_enc, profile_enc,
                                    dataset_id_enc, fingerprints_enc,
                                    content_hashes_enc = NULL) {
  generation_id  <- .dsr_decode(generation_id_enc)
  sample_ids     <- .dsr_decode(sample_ids_enc)
  segmenter      <- .dsr_decode(segmenter_enc)
  profile        <- .dsr_decode(profile_enc)
  dataset_id     <- .dsr_decode(dataset_id_enc)
  fingerprints   <- .dsr_decode(fingerprints_enc)
  content_hashes <- if (!is.null(content_hashes_enc))
    .dsr_decode(content_hashes_enc) else list()

  if (!requireNamespace("dsJobs", quietly = TRUE))
    stop("dsJobs required", call. = FALSE)

  image_root <- .resolve_image_root(dataset_id)
  mask_root <- .resolve_mask_root(dataset_id, segmenter)
  processor <- paste0(segmenter$provider, "_", segmenter$task %||% "default")

  # Map segmenter to runner
  seg_runner <- switch(segmenter$provider,
    existing_mask_asset = NULL,
    totalsegmentator = "totalsegmentator_infer",
    lungmask = "lungmask_infer",
    nnunetv2_predict = "nnunetv2_predict",
    monai_bundle_infer = "monai_bundle_infer",
    stop("Unknown segmenter provider: ", segmenter$provider, call. = FALSE))

  submitted <- list()

  for (sid in sample_ids) {
    fp <- fingerprints[[sid]]
    if (is.null(fp)) next

    # Per-image derivation hash: prefer content_hash (strong) over fingerprint (fast)
    ch <- content_hashes[[sid]]
    spec_hash <- dsImaging::compute_image_derivation_hash(
      content_hash = ch,
      fingerprint = fp,
      processor = processor,
      params = list(profile = profile$name, bin_width = profile$bin_width)
    )

    # Check if this exact image+params was already done (cross-user)
    existing <- dsImaging::find_asset_by_hash(dataset_id, spec_hash)

    if (!is.null(existing)) {
      dsImaging::complete_item_atomic(generation_id, sid, "completed",
        artifact_relpath = existing)
      submitted[[sid]] <- list(status = "reused", asset_id = existing)
      next
    }

    # Resolve image path
    image_path <- .resolve_sample_image(image_root, sid, dataset_id = dataset_id)
    if (is.null(image_path)) {
      dsImaging::complete_item_atomic(generation_id, sid, "failed",
        error = "Image file not found")
      submitted[[sid]] <- list(status = "failed", error = "Image not found")
      next
    }

    # Build per-image job steps (using dsJobs step format)
    steps <- list()

    # Step 1: emit config (session plane) -- stores metadata for the pipeline
    steps[[length(steps) + 1]] <- list(
      type = "emit",
      output_name = "image_config",
      value = list(
        image_path = image_path,
        sample_id = sid,
        dataset_id = dataset_id,
        generation_id = generation_id
      )
    )

    # Step 2: segment_single (artifact plane)
    if (!is.null(seg_runner)) {
      seg_config <- segmenter
      seg_config$image <- image_path
      seg_config$sample_id <- sid
      seg_config$generation_id <- generation_id
      steps[[length(steps) + 1]] <- list(
        type = "segment",
        runner = seg_runner,
        name = "segment_single",
        config = seg_config
      )
    }

    # Step 3: extract_single (artifact plane)
    # For extraction, pass mask explicitly if using existing masks,
    # otherwise the script finds it in the input dir (seg output)
    # Resolve profile name to actual YAML file path
    settings_path <- .resolve_profile_path(profile$name)
    extract_config <- c(profile, list(
      image = image_path,
      sample_id = sid,
      generation_id = generation_id,
      settings_file = settings_path %||% "default"
    ))
    if (!is.null(mask_root)) {
      mask_path <- .resolve_sample_mask(mask_root, sid)
      if (!is.null(mask_path))
        extract_config$mask <- mask_path
    }
    steps[[length(steps) + 1]] <- list(
      type = "extract",
      runner = "pyradiomics_extract",
      name = "extract_single",
      config = extract_config
    )

    # Step 4: publish per-image result (session plane)
    steps[[length(steps) + 1]] <- list(
      type = "publish_asset",
      publish_kind = "radiomics_image_result",
      config = list(
        generation_id = generation_id,
        sample_id = sid,
        dataset_id = dataset_id,
        spec_hash = spec_hash
      )
    )

    # Submit via jobSubmitDS (the proper dsJobs API)
    job_spec <- list(
      label = "dsRadiomics_image",
      tags = c("per_image", dataset_id, sid, generation_id),
      visibility = "private",
      steps = steps,
      .owner = .dsr_owner_id()
    )

    tryCatch({
      spec_enc <- .dsr_encode(job_spec)
      result <- dsJobs::jobSubmitDS(spec_enc)
      submitted[[sid]] <- list(status = "submitted", job_id = result$job_id)
    }, error = function(e) {
      dsImaging::complete_item_atomic(generation_id, sid, "failed",
        error = paste("Submit failed:", conditionMessage(e)))
      submitted[[sid]] <<- list(status = "submit_failed",
                                 error = conditionMessage(e))
    })
  }

  list(
    generation_id = generation_id,
    submitted = length(submitted),
    results = submitted
  )
}

# ---------------------------------------------------------------------------
# 3. Status: query progress of a generation (with failure synchronization)
# ---------------------------------------------------------------------------

#' @export
radiomicsCollectionStatusDS <- function(generation_id_enc) {
  generation_id <- .dsr_decode(generation_id_enc)

  # Sync failed dsJobs jobs -> mark asset_items as failed
  .sync_failed_jobs(generation_id)

  gen <- dsImaging::get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)

  items <- dsImaging::get_generation_items(generation_id)
  completed_ids <- items$sample_id[items$status == "completed"]
  failed_ids <- items$sample_id[items$status == "failed"]
  pending_ids <- items$sample_id[items$status == "pending"]
  claimed_ids <- items$sample_id[items$status == "claimed"]
  running_ids <- items$sample_id[items$status == "running"]

  # pending + claimed + running = "not yet done"
  not_done <- length(pending_ids) + length(claimed_ids) + length(running_ids)

  list(
    generation_id = generation_id,
    state = gen$state,
    total = as.integer(gen$expected_n %||% nrow(items)),
    completed = length(completed_ids),
    failed = length(failed_ids),
    pending = length(pending_ids),
    claimed = length(claimed_ids),
    running = length(running_ids),
    failed_samples = if (length(failed_ids) > 0) failed_ids else NULL,
    is_done = not_done == 0L
  )
}

# ---------------------------------------------------------------------------
# 4. Publish: aggregate per-image outputs into collection asset
# ---------------------------------------------------------------------------

#' @export
radiomicsPublishCollectionDS <- function(generation_id_enc, dataset_id_enc,
                                          allow_partial_enc) {
  generation_id  <- .dsr_decode(generation_id_enc)
  dataset_id     <- .dsr_decode(dataset_id_enc)
  allow_partial  <- .dsr_decode(allow_partial_enc)

  # Final sync to catch any late failures
  .sync_failed_jobs(generation_id)

  gen <- dsImaging::get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)

  items <- dsImaging::get_generation_items(generation_id)
  completed <- items[items$status == "completed", ]
  failed <- items[items$status == "failed", ]
  pending <- items[items$status == "pending", ]

  total <- nrow(items)
  n_failed <- nrow(failed)
  n_completed <- nrow(completed)
  n_pending <- nrow(pending)

  if (n_pending > 0)
    stop(n_pending, " items still pending. Wait for completion.", call. = FALSE)

  if (n_completed == 0)
    stop("No completed items to publish.", call. = FALSE)

  if (n_failed > 0 && !isTRUE(allow_partial)) {
    fail_pct <- n_failed / total
    if (fail_pct > 0.05)
      stop("Too many failures (", n_failed, "/", total,
           ", ", round(fail_pct * 100, 1), "%). ",
           "Use allow_partial=TRUE to publish anyway.", call. = FALSE)
  }

  # Build collection-level output directory
  output_root <- file.path(
    getOption("dsjobs.home", "/var/lib/dsjobs"), "publish",
    paste0("collection_", generation_id))
  dir.create(output_root, recursive = TRUE, showWarnings = FALSE)

  # Write collection manifest
  manifest <- list(
    generation_id = generation_id,
    dataset_id = dataset_id,
    total = total,
    completed = n_completed,
    failed = n_failed,
    failed_samples = if (n_failed > 0) failed$sample_id else character(0),
    item_artifacts = as.list(
      stats::setNames(completed$artifact_relpath, completed$sample_id))
  )
  writeLines(
    jsonlite::toJSON(manifest, auto_unbox = TRUE, pretty = TRUE),
    file.path(output_root, "collection_manifest.json"))

  # Adjust expected_n before publish to account for failures
  # publish_generation checks completed_n == expected_n
  if (n_failed > 0) {
    dsImaging::update_generation(generation_id, expected_n = n_completed)
  }

  provenance <- list(
    type = "per_image_collection",
    generation_id = generation_id,
    total = total,
    completed = n_completed,
    failed = n_failed
  )

  asset_id <- dsImaging::publish_generation(
    generation_id = generation_id,
    path_or_root = output_root,
    description = paste0("Radiomics collection: ", n_completed, "/", total,
                          " images"),
    provenance = provenance
  )

  if (is.null(asset_id)) {
    stop("publish_generation returned NULL. Generation may be in PARTIAL state.",
         call. = FALSE)
  }

  list(
    asset_id = asset_id,
    generation_id = generation_id,
    total = total,
    completed = n_completed,
    failed = n_failed,
    failed_samples = if (n_failed > 0) failed$sample_id else character(0)
  )
}

# ---------------------------------------------------------------------------
# Failure synchronization
# ---------------------------------------------------------------------------

#' Sync dsJobs failure states back to asset_items
#'
#' For items still "pending" in the generation, checks whether their
#' corresponding dsJobs jobs have FAILED. If so, marks the item as failed.
#' This closes the gap where a dsJobs job fails but the publisher hook
#' never runs (because the job died before reaching the publish step).
#'
#' @keywords internal
.sync_failed_jobs <- function(generation_id) {
  if (!requireNamespace("dsJobs", quietly = TRUE)) return(invisible(NULL))

  pending_items <- dsImaging::get_generation_items(generation_id, status = "pending")
  if (nrow(pending_items) == 0) return(invisible(NULL))

  # Query dsJobs for all failed jobs tagged with this generation
  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  failed_jobs <- DBI::dbGetQuery(db,
    "SELECT job_id, tags, error_message FROM jobs
     WHERE state = 'FAILED' AND tags LIKE ?",
    params = list(paste0("%", generation_id, "%")))

  if (nrow(failed_jobs) == 0) return(invisible(NULL))

  # Extract sample_ids from tags and mark items as failed
  for (i in seq_len(nrow(failed_jobs))) {
    tags <- strsplit(failed_jobs$tags[i], ",")[[1]]
    # Tags format: "per_image,dataset_id,sample_id,generation_id"
    # The sample_id is the one that's in our pending items
    for (sid in pending_items$sample_id) {
      if (sid %in% tags) {
        err_msg <- failed_jobs$error_message[i] %||% "dsJobs job failed"
        dsImaging::complete_item_atomic(generation_id, sid, "failed",
          error = err_msg)
        break
      }
    }
  }
  invisible(NULL)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Decode a base64-JSON parameter from DataSHIELD transport
#' @keywords internal
.dsr_decode <- function(x) {
  if (is.character(x) && length(x) == 1) {
    raw <- tryCatch(jsonlite::base64_dec(x), error = function(e) NULL)
    if (!is.null(raw)) {
      return(jsonlite::fromJSON(rawToChar(raw), simplifyVector = TRUE))
    }
    return(tryCatch(jsonlite::fromJSON(x, simplifyVector = TRUE),
                     error = function(e) x))
  }
  x
}

#' Encode a value for dsJobs internal submission
#' @keywords internal
.dsr_encode <- function(x) {
  json <- jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
  jsonlite::base64_enc(charToRaw(as.character(json)))
}

#' Get current owner_id
#' @keywords internal
.dsr_owner_id <- function() {
  tryCatch(dsJobs:::.get_owner_id(), error = function(e) {
    Sys.getenv("USER", Sys.info()[["user"]] %||% "unknown")
  })
}

#' Resolve dataset and return full resolved context
#' @keywords internal
.resolve_ds <- function(dataset_id) {
  tryCatch(dsImaging::resolve_dataset(dataset_id), error = function(e) NULL)
}

#' Resolve image root URI from manifest
#' @keywords internal
.resolve_image_root <- function(dataset_id) {
  resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved)) return(NULL)
  tryCatch({
    manifest <- dsImaging::parse_manifest(resolved$manifest_uri, resolved$backend)
    manifest$assets$images$uri
  }, error = function(e) NULL)
}

#' Resolve mask root URI when using existing masks
#' @keywords internal
.resolve_mask_root <- function(dataset_id, segmenter) {
  if (!identical(segmenter$provider, "existing_mask_asset")) return(NULL)
  resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved)) return(NULL)
  tryCatch({
    manifest <- dsImaging::parse_manifest(resolved$manifest_uri, resolved$backend)
    mask_alias <- segmenter$mask_asset %||% "masks"
    manifest$assets[[mask_alias]]$uri
  }, error = function(e) NULL)
}

#' Resolve a single sample's image file path
#'
#' Checks sample_manifests first (supports DICOM series, bundles).
#' Falls back to directory scanning for backward compat.
#' @keywords internal
.resolve_sample_image <- function(image_root, sample_id, dataset_id = NULL) {
  # 1. Try sample manifest (canonical for multi-file samples)
  if (!is.null(dataset_id)) {
    primary <- tryCatch(
      dsImaging::get_sample_primary_path(dataset_id, sample_id),
      error = function(e) NULL)
    if (!is.null(primary) && file.exists(primary)) return(primary)
  }

  # 2. Fallback: directory scan (single-file samples)
  if (is.null(image_root) || !dir.exists(image_root)) return(NULL)
  files <- list.files(image_root, full.names = TRUE)
  for (f in files) {
    base <- basename(f)
    name <- sub("\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm)$", "", base,
                ignore.case = TRUE)
    if (name == sample_id) return(f)
  }
  NULL
}

#' Resolve a single sample's mask file path
#' @keywords internal
.resolve_sample_mask <- function(mask_root, sample_id) {
  if (is.null(mask_root) || !dir.exists(mask_root)) return(NULL)
  files <- list.files(mask_root, full.names = TRUE, recursive = TRUE)
  for (f in files) {
    base <- basename(f)
    name <- sub("\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm)$", "", base,
                ignore.case = TRUE)
    if (grepl(sample_id, name, fixed = TRUE)) return(f)
  }
  NULL
}

#' Resolve a profile name to its YAML file path
#' @keywords internal
.resolve_profile_path <- function(profile_name) {
  if (is.null(profile_name)) return(NULL)
  # Check inst/profiles/ in dsRadiomics
  profiles_dir <- system.file("profiles", package = "dsRadiomics")
  if (nzchar(profiles_dir)) {
    candidates <- list.files(profiles_dir, full.names = TRUE)
    for (f in candidates) {
      if (grepl(profile_name, basename(f), fixed = TRUE)) return(f)
    }
  }
  # If profile_name is already a valid path, use it
  if (file.exists(profile_name)) return(profile_name)
  NULL
}
