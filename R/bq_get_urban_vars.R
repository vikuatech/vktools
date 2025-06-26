#' bq_get_urban_vars
#'
#' @description Perform queries against BQ tables to get urban variables for a selected coordinate
#'
#' @param coord sf object wit lat-lon coordinate in WGS84 CRS
#' @param buffer numeric buffer in meters to apply to the coordinate
#' @param get_hex logical if TRUE return the values by hexagons, otherwise aggregates values to a single joined hexagons
#'
#' @return invisible.
#'
#'
#' @export
bq_get_population <- function(coord, buffer = NULL, get_hex = TRUE){

  coord_str <- sf::st_as_text(coord) %>% parse_text_to_bqgeography(buffer)

  if(get_hex){
    select_str <- "fid, population, geometry"
  }
  else{
    select_str <- "
    sum(population) as population,
    sum(population)/(0.64*count(distinct fid)) as population_density,
    st_union_agg(geometry) as geometry
    "
  }

  .q <- glue::glue("
with
poly as (
  select {coord_str} as polygon
)

select
{select_str}
from `reporting-338116.assets.world_kontur_population`
where
st_intersects(
  (select polygon from poly),
  geometry
)
  ")

vktools::bq_get(.q, 'reporting-338116')

}

#' @export
#' @rdname bq_get_population
bq_get_competitors <- function(coord, buffer = NULL){

  coord_str <- sf::st_as_text(coord) %>% parse_text_to_bqgeography(buffer)

  .q <- glue::glue("
  with
point_buffer as (
  select {coord_str} as buffer
),

competitors as (
  select
  name,
  place_id,
  geometry,
  search_type as type
  from `reporting-338116.assets.venezuela_google_places`
  where search_type in ('bakery', 'supermarket')
)

select
*
from competitors
where
  st_intersects(
    (select buffer from point_buffer),
    geometry
  )
  ")

vktools::bq_get(.q, 'reporting-338116')

}

#' @export
#' @rdname bq_get_population
bq_get_wawa_movements <- function(coord, buffer = NULL){

  coord_str <- sf::st_as_text(coord) %>% parse_text_to_bqgeography(buffer)

  .q <- glue::glue("
with

point_buffer as (
  select {coord_str} as buffer
),

request as (
  select
  riderId,
  st_geogpoint(scheduledPickupLocation_lon, scheduledPickupLocation_lat) as pickupLocation,
  st_geogpoint(scheduledDropoffLocation_lon, scheduledDropoffLocation_lat) as dropoffLocation,
  datetime_sub(pickupCompletedTs, INTERVAL 4 HOUR) as pickupCompletedTs,
  datetime_sub(dropoffCompletedTs, INTERVAL 4 HOUR) as dropoffCompletedTs,
  'pickup' as type
  from `reporting-346217.sparelabs.requests`
  where
    status = 'completed'
    and pickupCompletedTs > datetime_sub(current_datetime, INTERVAL 1 YEAR)

),

pickups as (
  select
  riderId,
  pickupLocation,
  dropoffLocation,
  pickupCompletedTs,
  dropoffCompletedTs,
  'pickup' as type
  from request
  where
    st_intersects(
      (select buffer from point_buffer),
      pickupLocation
    )
),

dropoffs as (
  select
  riderId,
  pickupLocation,
  dropoffLocation,
  pickupCompletedTs,
  dropoffCompletedTs,
  'dropoff' as type
  from request
  where
    st_intersects(
      (select buffer from point_buffer),
      dropoffLocation
    )
)

select * from pickups
union all
select * from dropoffs
  ")

vktools::bq_get(.q, 'reporting-338116')

}

#' @export
#' @rdname bq_get_population
bq_get_homicidios <- function(coord, buffer = NULL){

  coord_str <- sf::st_as_text(coord) %>% parse_text_to_bqgeography(buffer)

  .q <- glue::glue("
with

# Buffer
point_buffer as (
  select {coord_str} as buffer
),

# Particiones de Parroquias dentro del Buffer
parroquias as (
  select
  id,
  PARROQUIA,
  ESTADO,
  MUNICIPIO,
  st_intersection(
    (select buffer from point_buffer),
    geometry
  ) as geometry
  from `reporting-338116.polygons.parroquias_venezuela`
  where st_intersects(
    (select buffer from point_buffer),
    geometry
  )
),

# Calcular area y join tasa
tasa as (
  select
  parroquias.geometry,
  st_area(parroquias.geometry) as area,
  st_area(parroquias.geometry)/(sum(st_area(parroquias.geometry)) OVER()) as area_pct,
  hom.tasa_muertes_violentas_por100khab as tasa_muertes_violentas
  from parroquias
  left join `reporting-338116.assets.caracas_ovv_homicidio_parroquias` hom on hom.parroquia_id = parroquias.id
)

# Calcular share por municipio
select
st_union_agg(geometry) as geometry,
sum(area_pct*tasa_muertes_violentas) as tasa_muertes_violentas
from tasa
")

vktools::bq_get(.q, 'reporting-338116')

}

#' @export
#' @rdname bq_get_population
bq_get_property_value <- function(coord, buffer = NULL, property_type = NULL){

  if(is.null(property_type)){
    where_clause <- ''
  }
  else{
    property_type_vec <- paste("'", property_type, "'", collapse = ",")
    where_clause <- glue::glue("WHERE property_type in ({property_type_vec})")
  }

  coord_str <- sf::st_as_text(coord) %>% parse_text_to_bqgeography(buffer)

  .q <- glue::glue("
with

zones as (
  select
  zona
  from `reporting-338116.assets.venezuela_rah_properties_zones`
  where st_intersects(
    {coord_str},
    geometry
  )
)

  select
    avg(if(contract = 'Venta', price, null)) as avg_precio_m2_venta,
    sum(if(contract = 'Venta', 1, 0)) as n_properties_venta,
    avg(if(contract = 'Alquiler', price, null)) as avg_precio_m2_alquiler,
    sum(if(contract = 'Alquiler', 1, 0)) as n_properties_alquiler,
    sum(if(property_type = 'Comercial', 1, 0)) as n_properties_comercial
  from zones
  left join `reporting-338116.assets.venezuela_rah_properties` prod on prod.zona = zones.zona
  {where_clause}
")

vktools::bq_get(.q, 'reporting-338116')

}

parse_text_to_bqgeography <- function(coord_text, buffer = NULL){
  if(!is.null(buffer)){
    zone <- glue::glue("ST_BUFFER(ST_GEOGFROMTEXT('{coord_text}'), {buffer})")
  }
  else{
    zone <- glue::glue("ST_GEOGFROMTEXT('{coord_text}')")
  }

  return(zone)
}
