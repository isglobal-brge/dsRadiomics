# Module: Radiomics Profile Registry
# Manages PyRadiomics extraction profiles (YAML settings files).

#' List available radiomics profiles
#' @export
list_radiomics_profiles <- function() {
  # Bundled profiles
  bundled_dir <- system.file("profiles", package = "dsRadiomics")
  profiles <- character(0)
  if (nzchar(bundled_dir) && dir.exists(bundled_dir)) {
    profiles <- sub("\\.yaml$", "", list.files(bundled_dir, "\\.yaml$"))
  }

  # Admin-installed profiles
  admin_dir <- file.path(.dsr_option("home", "/var/lib/dsradiomics"), "profiles")
  if (dir.exists(admin_dir)) {
    profiles <- c(profiles, sub("\\.yaml$", "", list.files(admin_dir, "\\.yaml$")))
  }

  unique(profiles)
}

#' Get a radiomics profile path
#' @keywords internal
.get_profile_path <- function(profile_name) {
  if (!grepl("^[a-zA-Z0-9_]+$", profile_name)) return(NULL)

  # Admin override
  admin_path <- file.path(.dsr_option("home", "/var/lib/dsradiomics"),
    "profiles", paste0(profile_name, ".yaml"))
  if (file.exists(admin_path)) return(admin_path)

  # Bundled
  bundled_path <- system.file("profiles", paste0(profile_name, ".yaml"),
    package = "dsRadiomics")
  if (nzchar(bundled_path) && file.exists(bundled_path)) return(bundled_path)

  NULL
}

#' Read a radiomics profile as a list
#' @export
read_radiomics_profile <- function(profile_name) {
  path <- .get_profile_path(profile_name)
  if (is.null(path)) stop("Profile not found: ", profile_name, call. = FALSE)
  if (requireNamespace("yaml", quietly = TRUE)) {
    yaml::read_yaml(path)
  } else {
    jsonlite::fromJSON(readLines(path, warn = FALSE), simplifyVector = FALSE)
  }
}
