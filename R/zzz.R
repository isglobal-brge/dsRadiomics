# Module: Package Hooks
# dsRadiomics on load:
# 1. Registers its artifact runners into DSJOBS_HOME/runners/
#    (dsJobs reads the YAMLs -- it never imports dsRadiomics)
# 2. Registers a publisher plugin in dsJobs for "radiomics_asset" kind
#    (dsJobs calls this when a publish step completes)
# Datasets are registered as dsImaging resources, NOT here.

`%||%` <- function(x, y) if (is.null(x)) y else x

.dsradiomics_env <- new.env(parent = emptyenv())

#' @keywords internal
.dsr_option <- function(name, default = NULL) {
  getOption(paste0("dsradiomics.", name),
    getOption(paste0("default.dsradiomics.", name), default))
}

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # 1. Register runners into DSJOBS_HOME/runners/
  tryCatch(.register_radiomics_runners(), error = function(e) NULL)

  # 2. Register publisher plugin in dsJobs
  if (requireNamespace("dsJobs", quietly = TRUE)) {
    tryCatch(
      dsJobs::register_dsjobs_publisher("radiomics_asset", .radiomics_publisher),
      error = function(e) NULL)
  }
}
