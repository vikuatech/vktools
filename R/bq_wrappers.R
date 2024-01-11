#' bq_get
#'
#' @description wrapper helpers to communicate with Bigquery
#'
#' @param query SQL query to execute
#' @param project,dataset,table str name of the entity that will perform the job
#' @param location str name of the wkt column to convert to sf.
#' @param data df to upload.
#' @param create_disposition,write_disposition,quiet options to set in the job
#' @param steps_by number of rows to upload in each step.
#' @param ... other arguments to pass to bigrquery::bq_table_upload or bigrquery::bq_table_download
#' @param geom_column name of the geometry column to be converted to GEOGRAPHY
#'
#' @return invisible.
#'
#' @export
bq_get <- function(query, project = NULL, location = NULL, ...){

  res <- bigrquery::bq_project_query(project, query) %>%
    bigrquery::bq_table_download(...)

  if(!is.null(location)){

    if (!requireNamespace("sf", quietly = TRUE)) {
      stop("sf required: install that first") # nocov
    }

    ret <- res %>%
      sf::st_as_sf(wkt = location, crs = 'WGS84')
  }
  else{
    ret <- tibble::as_tibble(res)
  }

  return(ret)
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
