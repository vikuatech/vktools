#' leaflet_plot
#'
#' @description quick leaflet map with feature type discovery
#'
#' @param data_sf sf dataframe
#' @param variable_color,variable_label fields to use in map
#' @param palette,reverse settings in leaflet::colorNumeric
#' @param legend_title,legend_prefix,legend_suffix,weight,fillOpacity,color,highlightOptions settings. view ?leaflet() fro more help
#'
#' @return
#' leaflet map viewer
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
map_polygons <- function(data_sf, variable_color, variable_label,
                         palette = 'YlOrRd', reverse = FALSE,
                         legend_title = NULL, legend_prefix = NULL, legend_suffix = NULL,
                         weight = 1, fillOpacity = 0.6, color = '#444444',
                         highlightOptions = leaflet::highlightOptions(color = "white", weight = 2, bringToFront = TRUE)
                         ){

  variable_color_values <- data_sf %>% dplyr::pull({{variable_color}})
  variable_label_values <- data_sf %>% dplyr::pull({{variable_label}})

  color_value <- leaflet::colorNumeric(palette, variable_color_values, reverse = reverse)
  data_sf %>%
    leaflet::leaflet() %>%
    leaflet::addTiles() %>%
    leaflet::addProviderTiles(leaflet::providers$CartoDB.Positron) %>%
    leaflet::addPolygons(
      label = lapply(variable_label_values, htmltools::HTML),
      fillColor = color_value(variable_color_values),
      weight = weight, fillOpacity = fillOpacity, color = color,
      highlightOptions = highlightOptions
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
