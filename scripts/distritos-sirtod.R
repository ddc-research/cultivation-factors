# Load necessary libraries
library(httr2)       # HTTP requests
library(tidyverse)   # Data manipulation and visualization
library(furrr)       # Parallel processing

# Set up parallel processing
plan(multisession)

cli::cli_inform("Getting variable list")

# Make a POST request to a URL and parse the JSON response into a tibble
sirtod <- request("https://systems.inei.gob.pe/SIRTOD/app/consulta/arboltematico") |>
	req_method(method = "POST") |>
	req_timeout(15) |>
	req_perform() |>
	resp_body_string() |>
	jsonlite::fromJSON() |>
	as_tibble()

# Filter rows with non-missing 'codigoIndicador'
sirtod_pre_auto <- sirtod |>
	filter(!is.na(codigoIndicador))

# Define a function to get the parent nodes recursively
get_parents <- function(x) {
	parent <- sirtod |>
		filter(idTema == x)
	
	if (nrow(parent) == 0) "" else paste0(get_parents(parent$idPadre), " ;; ", parent$nombreTema)
}

# Apply 'get_parents' function to create a column 'arbol' representing the tree structure
sirtod_parents <- sirtod_pre_auto |>
	filter(idTema != 1) |>
	select(idTema, idPadre) |>
	mutate(
		arbol = future_map_chr(idPadre, get_parents, .progress = TRUE),
		arbol = str_remove(arbol, "^ ;; ")
	)

# Define a function to retrieve data for a given indicator and parameters
get_sirtod_data <- function(indicador, tipo_ubigeo = 3, desde_anio = 2001, hasta_anio = 2022) {
	request("https://systems.inei.gob.pe/SIRTOD/app/consulta/getTableDataYear") |>
		req_url_query(indicador_listado = indicador) |>
		req_url_query(tipo_ubigeo = tipo_ubigeo) |>
		req_url_query(desde_anio = desde_anio) |>
		req_url_query(hasta_anio = hasta_anio) |>
		req_url_query(ubigeo_listado = "") |>
		req_url_query(idioma = "ES") |>
		req_perform() |>
		resp_body_string() |>
		jsonlite::fromJSON() |>
		as_tibble()
}

cli::cli_inform("Starting download")

# Create a column 'query' by applying the 'get_sirtod_data' function to each 'codigoIndicador'
sirtod_download <- sirtod_pre_auto |>
	mutate(query = future_map(codigoIndicador, safely(get_sirtod_data), .progress = TRUE))

# Join the parent information and filter valid data
sirtod_valid_data <- sirtod_download |>
	left_join(select(sirtod_parents, idTema, arbol)) |>
	mutate(n_columnas = map_dbl(query, ~ncol(.x$result))) |>
	filter(n_columnas > 0) |>
	select(codigoIndicador, arbol, query) |>
	mutate(query = map(query, "result")) |>
	unnest(cols = query) |>
	mutate(
		año = parse_number(año),
		dato = parse_number(dato)
	)

# Prepare for DB insertion
db_lugares <- sirtod_valid_data |> 
	distinct(ubigeo, departamento, provincia, distrito)

db_indicadores <- sirtod_valid_data |> 
	distinct(codigoIndicador, arbol, indicador)

db_valores <- sirtod_valid_data |> 
	select(codigoIndicador, ubigeo, año, dato)

cli::cli_inform("Writing to disk")

# Write the valid data to an .rds file
write_rds(db_lugares, "data-raw/sirtod-lugares.rds", compress = "xz")
write_rds(db_indicadores, "data-raw/sirtod-indicadores.rds", compress = "xz")
write_rds(db_valores, "data-raw/sirtod-valores.rds", compress = "xz")

# Sound to inform completion
walk(1:10, ~ {
	Sys.sleep(1) 
	beepr::beep(2)
})
