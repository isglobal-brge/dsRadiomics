# Module: Package Hooks

`%||%` <- function(x, y) if (is.null(x)) y else x

.dsradiomics_env <- new.env(parent = emptyenv())

#' @keywords internal
.dsr_option <- function(name, default = NULL) {
  getOption(paste0("dsradiomics.", name),
    getOption(paste0("default.dsradiomics.", name), default))
}

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Register dsJobs publisher plugin if dsJobs available
  if (requireNamespace("dsJobs", quietly = TRUE)) {
    tryCatch(
      dsJobs::register_dsjobs_publisher("radiomics_asset", .radiomics_publisher),
      error = function(e) NULL)
  }
}
