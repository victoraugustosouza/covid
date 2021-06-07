SELECT `pacientes`.`data_nascimento`, `sexos`.`nome` as `sexo`, `racas`.`nome` as `raca`,  `nacionalidades`.`nome` as `nacionalidade`,
`paises`.`nome` as `pais`, `estados`.`nome` as `estado_vacina`,  `municipios`.`nome` as `municipio_vacina` , `registros`.`data_aplicacao` as `data_aplicacao`,
`registros`.`tipo_de_dose_id` as `tipo_dose`, `vacinas`.`nome` as `vacina`,`vacinas`.`id` as `vacina_id`, `pacientes`.`id` as `id do paciente`
FROM `pacientes`
LEFT JOIN `sexos` ON `sexos`.`id`= `pacientes`.`sexo_id`
LEFT JOIN `racas` ON `racas`.`id`= `pacientes`.`raca_id`
LEFT JOIN `nacionalidades` ON `nacionalidades`.`id`= `pacientes`.`nacionalidade_id`
LEFT JOIN `paises` ON `paises`.`id`= `pacientes`.`pais_id`
LEFT JOIN `registros` ON `registros`.`paciente_id`= `pacientes`.`id`
LEFT JOIN `estabelecimentos` ON `registros`.`estabelecimento_id`= `estabelecimentos`.`id`
LEFT JOIN `municipios` ON `municipios`.`id`=  `estabelecimentos`.`municipio_id`
LEFT JOIN `estados` ON `estados`.`id`= `estabelecimentos`.`estado_id`
LEFT JOIN `vacinas` ON `vacinas`.`id`=`registros`.`vacina_id`
