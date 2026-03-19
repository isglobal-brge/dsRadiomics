# Module: Capabilities (DataSHIELD aggregate method)

#' Get Radiomics Capabilities
#'
#' DataSHIELD AGGREGATE method. Returns available profiles, models,
#' and environment status.
#'
#' @return Named list of capabilities.
#' @export
radiomicsCapabilitiesDS <- function() {
  list(
    dsradiomics_version = as.character(utils::packageVersion("dsRadiomics")),
    profiles = list_radiomics_profiles(),
    models = tryCatch(list_segmentation_models(), error = function(e) data.frame()),
    envs = tryCatch(list_radiomics_envs(), error = function(e) data.frame())
  )
}
