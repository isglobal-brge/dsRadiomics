# Module: Python Environment Provisioning

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
