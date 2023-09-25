library(perutranspaeconomica)
library(tidyverse)

municipios <- readxl::read_excel("data-raw/municipios-ejecucion-presupuestal.xlsx")

dict_municipios <- municipios |> 
	mutate(cod_municipalidad = str_extract(cod_municipalidad, "\\d{6}$")) |> 
	distinct(periodo, cod_departamento, cod_provincia, cod_municipalidad, municipalidad, provincia, departamento)
	

pre_query <- municipios |>
	select(
		periodo,
		municipalidad = cod_municipalidad,
		provincia = cod_provincia,
		departamento = cod_departamento,
		goblocal_o_manc,
		nivel
	) |>
	mutate(municipalidad = str_extract(municipalidad, "\\d{6}$"))
	
get_info_rubros <- function(periodo, nivel, goblocal_o_manc, departamento, provincia, municipalidad) {
	iniciar_transparencia_economica(modulo = "ingreso") |>
		elegir_periodo_anual(periodo) |>
		elegir_institucion(
			nivel = nivel,
			goblocal_o_manc = goblocal_o_manc,
			departamento = departamento,
			provincia = provincia,
			municipalidad = municipalidad
		) |>
		elegir_origen(fuente_financiamiento = "5", rubro = "todos") |>
		consultar() |> 
		suppressMessages()
}

municipios_rubros <- pre_query |>
	pmap(get_info_rubros, .progress = TRUE)

municipios_rubros_united <- municipios_rubros |> 
	list_rbind()

municipios_total_recaudacion <- municipios |> 
	distinct(periodo, nivel, goblocal_o_manc, departamento = cod_departamento, provincia = cod_provincia) |> 
	pmap(safely(function(periodo, nivel, goblocal_o_manc, departamento, provincia) {
		iniciar_transparencia_economica(modulo = "ingreso") |> 
			elegir_periodo_anual(periodo) |> 
			elegir_institucion(
				nivel = nivel,
				goblocal_o_manc = goblocal_o_manc,
				departamento = departamento,
				provincia = provincia,
				municipalidad = "todos"
			) |> 
			consultar() |> 
			suppressMessages()
	}), .progress = TRUE)

municipios_total_recaudacion_united <- municipios_total_recaudacion |> 
	map("result") |> 
	list_rbind() |> 
	mutate(cod_municipalidad = str_extract(cod_municipalidad, "\\d{6}$")) |> 
	select(periodo, cod_municipalidad, total_recaudado = recaudado)

municipios_recaudacion <- municipios_rubros_united |> 
	select(periodo, rubro = desc_rubro, recaudado, cod_municipalidad = municipalidad) |> 
	filter(!str_detect(rubro, "FONDO")) |> 
	mutate(rubro = if_else(str_detect(rubro, "CANON"), "canon", "impuestos")) |> 
	pivot_wider(id_cols = c(periodo, cod_municipalidad), names_from = rubro, values_from = recaudado) |> 
	left_join(municipios_total_recaudacion_united) |> 
	left_join(dict_municipios)

writexl::write_xlsx(municipios_recaudacion, "data-raw/municipios-recaudacion.xlsx")	
