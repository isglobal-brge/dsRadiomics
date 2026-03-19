test_that("radiomics envs are listed", {
  envs <- list_radiomics_envs()
  expect_true(is.data.frame(envs))
  expect_true(nrow(envs) >= 4)
  expect_true("radiomics" %in% envs$framework)
  expect_true("seg_totalseg" %in% envs$framework)
})
