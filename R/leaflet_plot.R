#' geocoding
#'
#' @description quick leaflet map with feature type discovery
#'
#' @param data_sf dataframe with the column string *address*
#'
#' @return
#' geocode_address, geocode_apply and geocode_update return the same address_df with lat, lng and place_id columns
#' geocode_google_safe return a list with lat, lng and place_id
#'
#' @export
leaflet_plot <- function(data_sf){

  geom_type <- sf::st_geometry_type(data_sf) %>% as.character() %>% unique()

  if(length(geom_type) > 1){
    geom_type <- 'POLYGON'
  }

  addTYPE <- switch(
    geom_type,
    POLYGON = leaflet::addPolygons,
    MULTIPOLYGON = leaflet::addPolygons,
    POINT = leaflet::addCircles,
    LINESTRING = leaflet::addPolylines
  )

  data_sf %>%
    leaflet::leaflet() %>%
    leaflet::addTiles() %>%
    addTYPE()
}

#' @export
#' @rdname leaflet_plot
map_polygons <- function(table_sf, variable_color, variable_label,
                         legend_title = NULL, legend_prefix = NULL, legend_suffix = NULL,
                         color_palette = 'YlOrRd'){

  variable_color_values <- table_sf %>% dplyr::pull({{variable_color}})
  variable_label_values <- table_sf %>% dplyr::pull({{variable_label}})

  color_value <- leaflet::colorNumeric(color_palette, variable_color_values)
  table_sf %>%
    leaflet::leaflet() %>%
    leaflet::addTiles() %>%
    leaflet::addProviderTiles(leaflet::providers$CartoDB.Positron) %>%
    leaflet::addPolygons(
      weight = 1,
      fillOpacity = 0.6,
      label = lapply(variable_label_values, htmltools::HTML),
      fillColor = color_value(variable_color_values),
      color = '#444444',
      highlightOptions = leaflet::highlightOptions(color = "white", weight = 2, bringToFront = TRUE)
    ) %>%
    leaflet::addLegend(
      position = 'bottomleft',
      pal = color_value,
      values = variable_color_values,
      title = legend_title,
      labFormat = leaflet::labelFormat(prefix = legend_prefix, suffix = legend_suffix),
      na.label = 'No Disponible'
    )

}
