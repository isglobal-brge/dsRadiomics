# Module: Runner Registration
# dsRadiomics registers its runners into DSJOBS_HOME/runners/ at load time.
# dsJobs reads the YAML and executes -- it never knows about dsRadiomics.

#' Register dsRadiomics runners in DSJOBS_HOME
#'
#' Called from .onLoad. Writes runner YAMLs with absolute paths to the
#' dsRadiomics venvs and Python scripts.
#'
#' @keywords internal
.register_radiomics_runners <- function() {
  dsjobs_home <- getOption("dsjobs.home",
    getOption("default.dsjobs.home", "/var/lib/dsjobs"))
  runners_dir <- file.path(dsjobs_home, "runners")
  if (!dir.exists(runners_dir)) {
    tryCatch(dir.create(runners_dir, recursive = TRUE), error = function(e) NULL)
  }
  if (!dir.exists(runners_dir)) return(invisible(NULL))

  venv_root <- .dsr_option("venv_root", "/var/lib/dsradiomics/venvs")
  scripts_dir <- system.file("python", package = "dsRadiomics")

  # PyRadiomics extraction runner
  .write_runner_yaml(runners_dir, "pyradiomics_extract", list(
    name = "pyradiomics_extract",
    plane = "artifact",
    resource_class = "cpu_heavy",
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsradiomics_extract.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--settings", "{settings_file}"
    ),
    timeout_secs = 3600L,
    allowed_params = c("settings_file", "mask_asset", "image_asset",
                        "dataset_id", "label_channel", "force2D", "voxel_based",
                        "profile_name", "bin_width", "feature_classes",
                        "name", "normalize", "resampled_spacing", "image_types")
  ))

  # TotalSegmentator runner
  .write_runner_yaml(runners_dir, "totalsegmentator_infer", list(
    name = "totalsegmentator_infer",
    plane = "artifact",
    resource_class = "gpu_optional",
    command = "python",
    python = file.path(venv_root, "seg_totalseg", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsradiomics_seg_totalseg.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--task", "{task}"
    ),
    timeout_secs = 7200L,
    allowed_params = c("task", "fast", "roi_subset", "statistics",
                        "image_asset", "provider")
  ))

  # nnU-Net v2 runner
  .write_runner_yaml(runners_dir, "nnunetv2_predict", list(
    name = "nnunetv2_predict",
    plane = "artifact",
    resource_class = "gpu_optional",
    command = "python",
    python = file.path(venv_root, "seg_nnunetv2", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsradiomics_seg_nnunet.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--model", "{model_name}"
    ),
    timeout_secs = 7200L,
    allowed_params = c("model_name", "fold", "checkpoint", "step_size",
                        "image_asset", "provider")
  ))

  # MONAI bundle runner
  .write_runner_yaml(runners_dir, "monai_bundle_infer", list(
    name = "monai_bundle_infer",
    plane = "artifact",
    resource_class = "gpu_optional",
    command = "python",
    python = file.path(venv_root, "seg_monai", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsradiomics_seg_monai.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--bundle", "{bundle_name}"
    ),
    timeout_secs = 7200L,
    allowed_params = c("bundle_name", "bundle_path", "device",
                        "image_asset", "provider")
  ))
}

#' Write a runner YAML to DSJOBS_HOME/runners/
#' @keywords internal
.write_runner_yaml <- function(runners_dir, name, config) {
  path <- file.path(runners_dir, paste0(name, ".yml"))
  if (requireNamespace("yaml", quietly = TRUE)) {
    writeLines(yaml::as.yaml(config), path)
  } else {
    writeLines(jsonlite::toJSON(config, auto_unbox = TRUE, pretty = TRUE), path)
  }
}
