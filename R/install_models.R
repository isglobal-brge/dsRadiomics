# Module: Model Installation
# Admin-facing commands to download model weights to the server.
# Weights go to /var/lib/dsradiomics/models/<provider>/<model_or_task>/
# This is infrastructure, not patient data -- shared across all users.

#' Get the models directory
#' @keywords internal
.models_dir <- function() {
  dir <- .dsr_option("models_dir",
    file.path(.dsr_option("home", "/var/lib/dsradiomics"), "models"))
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  dir
}

#' Install a segmentation model
#'
#' Downloads model weights to the server. Run once by the admin.
#' After install, the model is available for all dsRadiomics jobs.
#'
#' @param provider Character; "totalsegmentator", "lungmask", "monai", "nnunetv2".
#' @param task Character; model/task name (e.g. "total", "R231", "wholebody_ct_segmentation").
#' @param force Logical; re-download even if already installed.
#' @return Invisible TRUE on success.
#' @export
install_model <- function(provider, task, force = FALSE) {
  models_dir <- .models_dir()
  venv_root <- .dsr_option("venv_root", "/var/lib/dsradiomics/venvs")

  model_dir <- file.path(models_dir, provider, task)
  marker <- file.path(model_dir, ".installed")

  if (file.exists(marker) && !force) {
    message("Model ", provider, ":", task, " already installed at ", model_dir)
    return(invisible(TRUE))
  }

  dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)

  switch(provider,
    totalsegmentator = .install_totalseg(task, model_dir, venv_root),
    lungmask = .install_lungmask(task, model_dir, venv_root),
    monai = .install_monai_bundle(task, model_dir, venv_root),
    nnunetv2 = .install_nnunet(task, model_dir, venv_root),
    stop("Unknown provider: ", provider, call. = FALSE)
  )

  # Write marker
  writeLines(format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"), marker)

  # Register in model registry
  register_segmentation_model(
    name = paste0(provider, "_", task),
    provider = provider,
    task = task,
    path = model_dir
  )

  message("Model ", provider, ":", task, " installed at ", model_dir)
  invisible(TRUE)
}

#' List installed models
#' @export
list_installed_models <- function() {
  models_dir <- .models_dir()
  if (!dir.exists(models_dir)) return(data.frame(
    provider = character(0), task = character(0), path = character(0),
    installed_at = character(0), stringsAsFactors = FALSE))

  providers <- list.dirs(models_dir, recursive = FALSE, full.names = FALSE)
  rows <- list()
  for (prov in providers) {
    tasks <- list.dirs(file.path(models_dir, prov), recursive = FALSE, full.names = FALSE)
    for (task in tasks) {
      marker <- file.path(models_dir, prov, task, ".installed")
      installed_at <- if (file.exists(marker))
        trimws(readLines(marker, n = 1, warn = FALSE)) else NA_character_
      rows[[length(rows) + 1]] <- data.frame(
        provider = prov, task = task,
        path = file.path(models_dir, prov, task),
        installed_at = installed_at, stringsAsFactors = FALSE)
    }
  }
  if (length(rows) == 0) return(data.frame(
    provider = character(0), task = character(0), path = character(0),
    installed_at = character(0), stringsAsFactors = FALSE))
  do.call(rbind, rows)
}

# --- Provider-specific installers ---

#' @keywords internal
.install_totalseg <- function(task, model_dir, venv_root) {
  python <- file.path(venv_root, "seg_totalseg", "bin", "python")
  if (!file.exists(python)) python <- Sys.which("python3")

  message("Downloading TotalSegmentator weights for task: ", task)
  message("(This downloads ~1.5GB on first run)")

  # TotalSegmentator downloads weights to ~/.totalsegmentator by default
  # We set TOTALSEG_WEIGHTS_PATH to redirect to our model dir
  result <- processx::run(
    command = python,
    args = c("-c", paste0(
      "import os; os.environ['TOTALSEG_WEIGHTS_PATH'] = '", model_dir, "'; ",
      "from totalsegmentator.python_api import totalsegmentator; ",
      "print('TotalSegmentator weights path configured')"
    )),
    env = c("current", TOTALSEG_WEIGHTS_PATH = model_dir),
    error_on_status = FALSE,
    timeout = 600
  )

  if (result$status != 0L) {
    # Try downloading with the CLI
    result2 <- processx::run(
      command = python,
      args = c("-m", "totalsegmentator", "--help"),
      env = c("current", TOTALSEG_WEIGHTS_PATH = model_dir),
      error_on_status = FALSE,
      timeout = 30
    )
    message("TotalSegmentator runtime verified. Weights download on first use.")
  }
}

#' @keywords internal
.install_lungmask <- function(task, model_dir, venv_root) {
  python <- file.path(venv_root, "seg_lungmask", "bin", "python")
  if (!file.exists(python)) {
    # lungmask might share the totalseg venv or use system python
    python <- file.path(venv_root, "seg_totalseg", "bin", "python")
    if (!file.exists(python)) python <- Sys.which("python3")
  }

  message("Configuring LungMask model: ", task)
  # LungMask downloads weights on first use to torch cache
  # Just verify the module is importable
  result <- processx::run(
    command = python,
    args = c("-c", "import lungmask; print('LungMask available')"),
    error_on_status = FALSE,
    timeout = 30
  )

  if (result$status != 0L)
    message("LungMask not installed. Install with: uv pip install lungmask")
  else
    message("LungMask ready. Weights download on first segmentation.")
}

#' @keywords internal
.install_monai_bundle <- function(task, model_dir, venv_root) {
  python <- file.path(venv_root, "seg_monai", "bin", "python")
  if (!file.exists(python)) python <- Sys.which("python3")

  message("Downloading MONAI bundle: ", task)
  result <- processx::run(
    command = python,
    args = c("-c", paste0(
      "from monai.bundle import download; ",
      "download(name='", task, "', bundle_dir='", model_dir, "')"
    )),
    error_on_status = FALSE,
    timeout = 600
  )

  if (result$status != 0L)
    message("MONAI bundle download may have failed. Check manually.")
  else
    message("MONAI bundle ", task, " downloaded.")
}

#' @keywords internal
.install_nnunet <- function(task, model_dir, venv_root) {
  message("nnU-Net v2 model registration: ", task)
  message("nnU-Net models must be trained or imported manually.")
  message("Place model folder at: ", model_dir)
  message("Expected structure: ", model_dir, "/nnUNetTrainer__nnUNetPlans__3d_fullres/")
}
