test_that("model registry works with temp dir", {
  tmp <- tempfile("models_")
  dir.create(tmp)
  on.exit(unlink(tmp, recursive = TRUE))
  withr::local_options(list(dsradiomics.home = tmp))

  # Initially empty
  models <- list_segmentation_models()
  expect_equal(nrow(models), 0)

  # Register a model
  dir.create(file.path(tmp, "models"), recursive = TRUE)
  register_segmentation_model(
    name = "test_unet",
    provider = "nnunetv2_predict",
    task = "lung_segmentation",
    path = "/data/models/test_unet"
  )

  # Now listed
  models <- list_segmentation_models()
  expect_equal(nrow(models), 1)
  expect_equal(models$name, "test_unet")
  expect_equal(models$provider, "nnunetv2_predict")
})
