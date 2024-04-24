#' bq_get
#'
#' @description wrapper helpers to communicate with Bigquery
#'
#' @param query SQL query to execute
#' @param project,dataset,table str name of the entity that will perform the job
#' @param location `r lifecycle::badge("deprecated")` use the `convert_sf` argument instead
#' @param convert_sf convert all wkt columns (GEOGRAPHY in BigQuery) to sfc and return a sf object
#' @param data df to upload.
#' @param create_disposition,write_disposition,quiet options to set in the job
#' @param steps_by number of rows to upload in each step.
#' @param ... other arguments to pass to bigrquery::bq_table_upload or bigrquery::bq_table_download
#' @param geom_column name of the geometry column to be converted to GEOGRAPHY
#'
#' @return invisible.
#'
#' @importFrom lifecycle deprecated
#'
#' @export
bq_get <- function(query, project = NULL, location = deprecated(), convert_sf = TRUE, ...){

  if (lifecycle::is_present(location)) {
    lifecycle::deprecate_warn("0.0.16", "bq_get(location = )", "bq_get(convert_sf = TRUE)")
  }

  # Fetch data from BQ
  res <- bigrquery::bq_project_query(project, query) %>%
    bigrquery::bq_table_download(...) %>%
    tibble::as_tibble()

  # Get column classes & check for geometry column
  column_classes <- res %>% purrr::map(~class(.x) %>% purrr::pluck(1))
  has_wkt <- any(column_classes == 'wk_wkt')

  # If geometry then convert to sf
  if( all(convert_sf, has_wkt) ){

    # Warn if not sf installed
    if (!requireNamespace("sf", quietly = TRUE)) {
      warning("Cannot convert wkt column to sf. Please install sf package")
      return( res )
    }

    # Convert to sf
    res_sf <- res %>%
      dplyr::mutate(
        dplyr::across(tidyselect::where(wk::is_wk_wkt), ~sf::st_as_sfc(.x) %>% sf::st_set_crs(4326) )
      ) %>%
      sf::st_as_sf()

    return(res_sf)
  }

  # If not geometry then return tibble
  else{
    return(res)
  }


}

#' @export
#' @rdname bq_get
bq_post <- function(data, project, dataset, table,
                    create_disposition = 'CREATE_IF_NEEDED',
                    write_disposition = 'WRITE_TRUNCATE',
                    quiet = F){

  bigrquery::bq_table(
    project = project,
    dataset = dataset,
    table = table
  ) %>%
    bigrquery::bq_table_upload(
      values = data,
      create_disposition = create_disposition,
      write_disposition = write_disposition,
      quiet = quiet,
      fields = bigrquery::as_bq_fields(data)
    )

}

#' @export
#' @rdname bq_get
bq_post_steps <- function(data, steps_by = 100000, ...){

  steps <- seq(0, nrow(data), by=steps_by)
  steps <- c(steps, nrow(data))
  for(.row in 2:length(steps)){


    # .row <- 3
    init <- steps[.row-1]+1
    end <- steps[.row]
    cat('Uploading from: ', init, ' to: ', end, '\n')
    data_sample <- data[init:end, ]

    data_sample %>% vktools::bq_post(...)

  }

}

#' @export
#' @rdname bq_get
bq_create_geometry <- function(df, geom_column, project, dataset, table, ...){

  # Create auxiliar Table with geometry as STRING
  table_aux <- paste0(table, '_aux')
  geom_column_expr <- rlang::enexpr(geom_column)

  df %>%
    dplyr::mutate('{{geom_column}}' := as.character( {{geom_column}} ) ) %>%
    vktools::bq_post(project, dataset, table_aux, ...)

  # Create new table parsing geometry as GEOGRAPHY
  create_table_query <- glue::glue('CREATE TABLE `{project}.{dataset}.{table}` as (
SELECT
* except({geom_column_expr}),
st_geogfromtext({geom_column_expr}, make_valid=>true) as {geom_column_expr},
FROM `{project}.{dataset}.{table_aux}`)
')

  bigrquery::bq_project_query(project, create_table_query)

  # Remove auxiliar table
  bigrquery::bq_project_query(
    project,
    glue::glue('DROP TABLE `{project}.{dataset}.{table_aux}`')
  )

}
