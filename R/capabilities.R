# Module: Capabilities + Admin DS methods

#' Get Radiomics Capabilities
#'
#' DataSHIELD AGGREGATE method.
#'
#' @return Named list of capabilities.
#' @export
radiomicsCapabilitiesDS <- function() {
  list(
    dsradiomics_version = as.character(utils::packageVersion("dsRadiomics")),
    profiles = list_radiomics_profiles(),
    models = tryCatch(list_installed_models(), error = function(e) data.frame()),
    envs = tryCatch(list_radiomics_envs(), error = function(e) data.frame()),
    admin_enabled = .radiomics_admin_enabled()
  )
}

#' Install a Segmentation Model (admin only)
#'
#' DataSHIELD AGGREGATE method. Protected by dsjobs.admin_key.
#' Downloads model weights to the server.
#'
#' @param admin_key_encoded Character; B64-encoded admin key.
#' @param provider Character; "totalsegmentator", "lungmask", "monai", "nnunetv2".
#' @param task Character; model/task name.
#' @return Named list with install status.
#' @export
radiomicsInstallModelDS <- function(admin_key_encoded, provider, task) {
  .verify_radiomics_admin(admin_key_encoded)

  result <- tryCatch({
    install_model(provider, task)
    list(status = "installed", provider = provider, task = task)
  }, error = function(e) {
    list(status = "failed", provider = provider, task = task,
         error = conditionMessage(e))
  })

  result
}

#' List Installed Models (no admin needed)
#'
#' DataSHIELD AGGREGATE method.
#'
#' @return Data.frame of installed models.
#' @export
radiomicsListModelsDS <- function() {
  list_installed_models()
}

# --- Admin verification (reuses dsjobs.admin_key) ---

#' @keywords internal
.radiomics_admin_enabled <- function() {
  key <- .dsj_option_safe("admin_key")
  !is.null(key) && nzchar(key)
}

#' @keywords internal
.verify_radiomics_admin <- function(admin_key_encoded) {
  expected <- .dsj_option_safe("admin_key")

  if (is.null(expected) || !nzchar(expected))
    stop("Admin access is not enabled. Set dsjobs.admin_key option.", call. = FALSE)

  # Decode B64
  decoded <- tryCatch({
    d <- jsonlite::base64_dec(gsub("^B64:", "", admin_key_encoded))
    parsed <- jsonlite::fromJSON(rawToChar(d), simplifyVector = FALSE)
    parsed$.admin_key
  }, error = function(e) admin_key_encoded)

  if (is.null(decoded) || !nzchar(decoded))
    stop("Access denied: admin_key required.", call. = FALSE)
  if (!identical(decoded, expected))
    stop("Access denied: invalid admin_key.", call. = FALSE)
}

#' Read dsjobs option safely (dsRadiomics doesn't import dsJobs)
#' @keywords internal
.dsj_option_safe <- function(name) {
  getOption(paste0("dsjobs.", name),
    getOption(paste0("default.dsjobs.", name), NULL))
}
