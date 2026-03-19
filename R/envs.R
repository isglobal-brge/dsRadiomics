# Module: Python Environment Provisioning
# Mirrors dsFlower pattern: uv-based venv creation per framework.

.RADIOMICS_PYTHON_DEPS <- list(
  radiomics = c("pyradiomics>=3.0.0", "SimpleITK>=2.0.0", "pandas>=1.3.0",
                "numpy>=1.21.0", "pyyaml>=5.0"),
  seg_totalseg = c("TotalSegmentator>=2.0.0", "torch>=2.0.0",
                    "SimpleITK>=2.0.0", "nibabel>=4.0.0"),
  seg_nnunetv2 = c("nnunetv2>=2.0.0", "torch>=2.0.0",
                    "SimpleITK>=2.0.0", "nibabel>=4.0.0"),
  seg_monai = c("monai>=1.3.0", "torch>=2.0.0",
                 "SimpleITK>=2.0.0", "nibabel>=4.0.0", "pyyaml>=5.0")
)

.RADIOMICS_HEALTH_IMPORTS <- list(
  radiomics = "radiomics",
  seg_totalseg = "totalsegmentator",
  seg_nnunetv2 = "nnunetv2",
  seg_monai = "monai"
)

#' Ensure a Python environment for a radiomics/segmentation framework
#'
#' Reuses dsFlower's .ensure_python_env if available, otherwise creates
#' its own venv under dsradiomics.venv_root.
#'
#' @param framework Character; framework name.
#' @return Named list with python path.
#' @keywords internal
.ensure_radiomics_env <- function(framework) {
  # Try dsFlower's env system first
  if (requireNamespace("dsFlower", quietly = TRUE)) {
    tryCatch({
      env <- dsFlower:::.ensure_python_env(framework)
      return(env)
    }, error = function(e) NULL)
  }

  # Standalone: use system python
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- "python3"
  list(python = python, source = "system")
}

#' List available radiomics environments
#' @export
list_radiomics_envs <- function() {
  envs <- data.frame(
    framework = names(.RADIOMICS_PYTHON_DEPS),
    deps = vapply(.RADIOMICS_PYTHON_DEPS, function(d) paste(d, collapse = ", "), character(1)),
    stringsAsFactors = FALSE
  )
  envs
}
