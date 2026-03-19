test_that("bundled profiles are listed", {
  profiles <- list_radiomics_profiles()
  expect_true(length(profiles) >= 4)
  expect_true("ibsi_ct_3d_v1" %in% profiles)
  expect_true("ibsi_mr_3d_v1" %in% profiles)
  expect_true("ibsi_force2d_v1" %in% profiles)
  expect_true("voxel_map_firstorder_v1" %in% profiles)
})

test_that("profiles can be read", {
  if (requireNamespace("yaml", quietly = TRUE)) {
    p <- read_radiomics_profile("ibsi_ct_3d_v1")
    expect_true(is.list(p))
    expect_true("setting" %in% names(p))
    expect_true("featureClass" %in% names(p))
    expect_equal(p$setting$binWidth, 25)
    expect_false(p$setting$force2D)
  }
})

test_that("reading nonexistent profile errors", {
  expect_error(read_radiomics_profile("nonexistent_profile"), "not found")
})
