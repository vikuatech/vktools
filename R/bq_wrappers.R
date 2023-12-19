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
