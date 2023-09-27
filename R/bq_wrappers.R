#' bq_get
#'
#' @description wrapper helpers to communicate with Bigquery
#'
#' @param query SQL query to execute
#' @param dataset str name of the dataset that will perform the job
#' @param location str name of the wkt column to convert to sf. When bq_post the column is a geometry in a `data`_sf object and it will be converted to wkt
#' @param data df to upload. In `sf_to_wk` data must be a sf object
#' @param project str name of the project that will perform the job
#' @param table str name of the table that will perform the job
#' @param create_disposition,write_disposition,quiet options to set in the job
#'
#' @return invisible.
#'
#' @export
bq_get <- function(query, dataset = NULL, location = NULL){

  res <- bigrquery::bq_project_query(dataset, query) %>%
    bigrquery::bq_table_download()

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
sf_to_wk <- function(data, location = geometry){
  data %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      "{{location}}" := sf::st_as_text({{location}}) %>% wk::wkt()
    )
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
