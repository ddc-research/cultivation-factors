library(perutranspaeconomica)
library(tidyverse)

departamentos <- seguimiento_ep() |> 
	elegir_periodo_anual(2012:2022) |> 
	elegir_quien_gasta(
		nivel = "M",
		goblocal_o_manc = "M",
		departamento = "todos",
	) |> 
	consultar()

dict_departamentos <- departamentos |> 
	distinct(cod_departamento, departamento = desc_departamento) |> 
	as_tibble()

obtener_provincias <- function(periodo, departamento) {
	cli::cli_inform("Año: {periodo}, dep: {departamento}, prov: todos")
	
	seguimiento_ep() |> 
		elegir_periodo_anual(periodo = periodo) |> 
		elegir_quien_gasta(
			nivel = "M",
			goblocal_o_manc = "M",
			departamento = departamento,
			provincia = "todos"
		) |> 
		consultar() |> 
		suppressMessages()
}

cli::cli_inform("Inicio de obtención de provincias")
iterado_provincias <- map2(departamentos$periodo, departamentos$cod_departamento, obtener_provincias)

provincias <- bind_rows(iterado_provincias)

dict_provincias <- provincias |> 
	as_tibble() |> 
	distinct(cod_departamento = departamento, cod_provincia, provincia = desc_provincia) |> 
	left_join(dict_departamentos) |> 
	mutate(cod_provincia = str_extract(cod_provincia, "\\d{2}$"))

obtener_distritos <- function(periodo, departamento, provincia) {
	cli::cli_inform("Año: {periodo}, dep: {departamento}, prov {provincia}, Distrito: todos")
	
	seguimiento_ep() |> 
		elegir_periodo_anual(periodo = periodo) |> 
		elegir_quien_gasta(
			nivel = "M",
			goblocal_o_manc = "M",
			departamento = departamento,
			provincia = provincia,
			municipalidad = "todos"
		) |> 
		consultar() |> 
		suppressMessages()
}

cli::cli_inform("Inicio de obtención de distritos")
iterado_distritos <- pmap(
	.l = list(
		periodo = provincias$periodo,
		departamento = provincias$departamento,
		provincia = str_extract(provincias$cod_provincia, "\\d{2}$")
	),
	.f = obtener_distritos,
	.progress = TRUE
)

distritos <- bind_rows(iterado_distritos)

municipios_ejecucion_presupuestal <- distritos |>
	as_tibble() |>
	select(-municipalidad) |> 
	rename(
		cod_departamento = departamento,
		cod_provincia = provincia,
		municipalidad = desc_municipalidad,
	) |>
	left_join(dict_provincias)

writexl::write_xlsx(municipios_ejecucion_presupuestal, "data-raw/municipios-ejecucion-presupuestal.xlsx")
