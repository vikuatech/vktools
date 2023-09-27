#' get_google_places
#'
#' @description Wrapper of googleway::get_places to paginate and get all possible results.
#'
#' @param ... parameters to pass to googleway::get_places
#' @param places_list list of places returned by calling `get_google_places` in a map routine
#'
#' @details
#' `get_google_places` cleans the response to get a tibble of places and attributes:
#' name, place_id, types, vicinity, rating, user_ratings_total, lat, lng
#' `get_google_places_pagination` returns a list with max 3 responses (Google Places API return max 3 resposes of 20 places each)
#' `bind_places` is a cleaning wrapper to standarize responses of `get_google_places` into a tibble
#'
#' @return `get_google_places` returns tibble. `get_google_places_pagination` returns response list. `bind_places` returns a tibble
#'
#' @export
get_google_places <- function(...){

tryCatch({
  suppressWarnings({
    places_list <- get_google_places_pagination(...)
  })

  places_list %>%
    dplyr::bind_rows() %>%
    tidyr::unnest(geometry) %>%
    tidyr::unnest(location) %>%
    dplyr::select(name, place_id, types, vicinity, rating, user_ratings_total, lat, lng)

},
error = function(e){
  NA
})

}

#' @export
#' @rdname get_google_places
get_google_places_pagination <- function(...){

  places_list <- list()
  page_token <- NULL
  while (TRUE) {

    tryCatch({
      response <- googleway::google_places(..., page_token = page_token)
    },
    error = function(e){
      response <- googleway::google_places(..., page_token = page_token)
    })


    places_list <- c(places_list, list(response$results))

    # Si no hay más páginas, finaliza el bucle
    if (is.null(response$next_page_token)) {
      break
    }

    # Espera un momento para permitir que el token de página se active
    Sys.sleep(2)

    page_token <- response$next_page_token
  }

  return(places_list)
}

#' @export
#' @rdname get_google_places
bind_places <- function(places_list){
  places_list %>%
    purrr::discard(is.na) %>%
    purrr::map(dplyr::bind_rows) %>%
    dplyr::bind_rows() %>%
    dplyr::distinct(place_id, .keep_all = T) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(types = jsonlite::toJSON(types)) %>%
    dplyr::ungroup() %>%
    sf::st_as_sf(coords = c('lng', 'lat'), crs = sf::st_crs(4326))
}
