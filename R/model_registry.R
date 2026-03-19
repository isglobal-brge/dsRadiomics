# Module: Segmentation Model Registry
# Tracks available models, their locations, and metadata.
# Models are installed by admin, not auto-downloaded.

#' Get model registry path
#' @keywords internal
.model_registry_path <- function() {
  .dsr_option("model_registry",
    file.path(.dsr_option("home", "/var/lib/dsradiomics"), "models"))
}

#' List installed segmentation models
#' @export
list_segmentation_models <- function() {
  registry_dir <- .model_registry_path()
  if (!dir.exists(registry_dir)) {
    return(data.frame(name = character(0), provider = character(0),
      task = character(0), path = character(0), stringsAsFactors = FALSE))
  }

  manifests <- list.files(registry_dir, pattern = "\\.json$", full.names = TRUE)
  if (length(manifests) == 0) {
    return(data.frame(name = character(0), provider = character(0),
      task = character(0), path = character(0), stringsAsFactors = FALSE))
  }

  rows <- lapply(manifests, function(f) {
    tryCatch({
      m <- jsonlite::fromJSON(f, simplifyVector = FALSE)
      data.frame(name = m$name %||% basename(f),
        provider = m$provider %||% "unknown",
        task = m$task %||% "segmentation",
        path = m$path %||% dirname(f),
        stringsAsFactors = FALSE)
    }, error = function(e) NULL)
  })
  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0) return(data.frame(
    name = character(0), provider = character(0),
    task = character(0), path = character(0), stringsAsFactors = FALSE))
  do.call(rbind, rows)
}

#' Get a segmentation model config by name
#' @keywords internal
.get_model_config <- function(model_name) {
  registry_dir <- .model_registry_path()
  manifest_path <- file.path(registry_dir, paste0(model_name, ".json"))
  if (!file.exists(manifest_path)) return(NULL)
  jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
}

#' Register a segmentation model (admin utility)
#' @export
register_segmentation_model <- function(name, provider, task, path,
                                         python_deps = NULL, extra = list()) {
  registry_dir <- .model_registry_path()
  dir.create(registry_dir, recursive = TRUE, showWarnings = FALSE)

  manifest <- c(list(name = name, provider = provider, task = task,
    path = path, python_deps = python_deps,
    registered_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")), extra)

  manifest_path <- file.path(registry_dir, paste0(name, ".json"))
  writeLines(jsonlite::toJSON(manifest, auto_unbox = TRUE, pretty = TRUE),
             manifest_path)
  invisible(manifest_path)
}
