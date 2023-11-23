#' sf_df_to_list
#'
#' @description wrapper helpers to apply sf functions
#'
#' @param data_sf sf dataframe
#' @param names_from column of the field that will be passing to list names. Names must be uniques
#' @param crs crs to apply to the new list elements
#' @param location geometry field to be converted to wkt
#'
#' @return invisible.
#'
#' @export
sf_df_to_list <- function(data_sf, names_from = NULL, crs = 4326){

  data_list <- data_sf %>%
    sf::st_geometry() %>%
    purrr::map(~sf::st_geometry(.x) %>% sf::st_set_crs(crs))

  if(!is.null(names_from)){

    names_ <- data_sf %>% dplyr::pull({{names_from}})
    if(any(duplicated(names_))){
      cat('There are duplicated names in names_from')
      break
    }

    data_list <- data_list %>% purrr::set_names(names_)
  }

  return(data_list)
}

#' @export
#' @rdname sf_df_to_list
sf_to_wk <- function(data_sf, location = geometry){
  data_sf %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      "{{location}}" := sf::st_as_text({{location}}) %>% wk::wkt()
    )
}
