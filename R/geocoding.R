#' geocoding
#'
#' @description apply geocode to an address text via google Geocode API
#'
#' @param address_df dataframe with the column string *address*
#' @param address_column name of the column containning addresses
#' @param address_str string Address to geocode
#' @param id,lat,lng,place_id values to update in *address_df* by id
#'
#' @return
#' geocode_address, geocode_apply and geocode_update return the same address_df with lat, lng and place_id columns
#' geocode_google_safe return a list with lat, lng and place_id
#'
#' @export
geocode_address <- function(address_df, address_column){

  address_df %>%
    dplyr::filter(!is.na({{address_column}})) %>%
    dplyr::mutate(
      response = purrr::map({{address_column}}, ~geocode_google_safe(.x)),
      coords = purrr::map(response, ~.x$results$geometry$location),
      place_id = purrr::map(response, ~.x$results$place_id)
    ) %>%
    tidyr::unnest(coords, keep_empty = T, names_repair = tidyr::tidyr_legacy) %>%
    tidyr::unnest(place_id, keep_empty = T, names_repair = tidyr::tidyr_legacy) %>%
    dplyr::mutate(place_id = as.character(place_id))

}

#' @export
#' @rdname geocode_address
geocode_apply <- function(address_df){

  df_cols <- names(address_df)
  stopifnot('row_id' %in% df_cols)

  if(any(c('lat', 'lng') %in% df_cols)){

    print('Geocode: lat-lng present in dataset')
    due_geocode <- address_df %>%
      dplyr::filter(is.na(lat) | is.na(lng))

    already_geocoded <- address_df %>%
      dplyr::filter(!is.na(lat) & !is.na(lng))


    if(nrow(already_geocoded) == 0){
      already_geocoded <- already_geocoded %>% dplyr::select(-lat, -lng)
    }

    if(nrow(due_geocode) == 0){
      print('Geocode: No values to geocode')
      addresses_geocoded <- address_df
    }
    else{
      print('Geocode: geocoding missing coords and rbind')
      new_geocoded <- due_geocode %>%
        dplyr::select(-lat, -lng) %>%
        geocode_address(address)
      addresses_geocoded <- dplyr::bind_rows(already_geocoded, new_geocoded)
    }

  }
  else{
    print('No lat-lng columns. Geocode All records')
    addresses_geocoded <- geocode_address(address_df, address)
  }

  addresses_geocoded %>%
    dplyr::group_by(row_id) %>%
    dplyr::slice_head(n = 1) %>%
    dplyr::ungroup()

}

#' @export
#' @rdname geocode_address
geocode_google_safe <- function(address_str){
  tryCatch({
    googleway::google_geocode(address_str)
  }, error = function(e){

    list(
      results = list(
        geometry = tibble::tibble(
          location = tibble::tibble(lat = NA_real_, lng = NA_real_)
        ),
        place_id = NA_character_
      )
    )

  })
}

#' @export
#' @rdname geocode_address
geocode_update <- function(address_df, id, lat, lng, place_id){

  cat('Saving Manually Geocoded row_id: ', id, '\n')

  replacement <- tibble::tibble(
    row_id = id,
    lat = lat,
    lng = lng,
    place_id = place_id
  )

  address_df %>%
    dplyr::rows_update(
      replacement,
      by = 'row_id'
    )
}
