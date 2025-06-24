WITH
	-- CTE 1: Límites de cargos por legajo
	limites_cargos AS (SELECT nro_legaj,
	                          CASE WHEN MIN( fec_alta ) > '2025-05-01'::DATE
		                               THEN DATE_PART( 'day', MIN( fec_alta )::TIMESTAMP )
		                               ELSE DATE_PART( 'day', timestamp '2025-05-01')::INTEGER END AS minimo,
	                          MAX( CASE WHEN fec_baja > '2025-05-31'::DATE OR fec_baja IS NULL THEN
	                               DATE_PART( 'day', timestamp '2025-05-31')::INTEGER ELSE
	                               DATE_PART( 'day', fec_baja::TIMESTAMP ) END )                   AS maximo
	                   FROM mapuche.dh03
	                   WHERE (fec_baja IS NULL OR fec_baja >= '2025-05-01'::DATE)
		                  AND nro_legaj IN ($legajos_in)
	                   GROUP BY nro_legaj),

	-- CTE 2: Cargos con licencias vigentes (problemas)
	cargos_con_licencia_problematica AS (SELECT DISTINCT dh05.nro_cargo
	                                     FROM mapuche.dh05
		                                          JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
	                                     WHERE mapuche.map_es_licencia_vigente( dh05.nro_licencia )
		                                    AND (dl02.es_maternidad IS TRUE OR (NOT dl02.es_remunerada OR
		                                                                        (dl02.es_remunerada AND dl02.porcremuneracion = '0')))),

	-- CTE 3: Todos los cargos activos básicos
	cargos_base AS (SELECT dh03.nro_legaj,
	                       dh03.nro_cargo,
	                       CASE WHEN fec_alta <= '2025-05-01'::DATE THEN DATE_PART( 'day', timestamp '2025-05-01')::INTEGER
	                                                                 ELSE DATE_PART( 'day', fec_alta::TIMESTAMP ) END AS inicio_cargo,
	                       CASE WHEN fec_baja > '2025-05-31'::DATE OR fec_baja IS NULL THEN date_part('day', TIMESTAMP '2025-05-31')::INTEGER
                ELSE date_part('day', fec_baja::TIMESTAMP)
            END AS final_cargo,
            CASE 
                WHEN dh03.nro_cargo IN (SELECT nro_cargo FROM cargos_con_licencia_problematica)
                THEN FALSE ELSE TRUE 
            END AS sin_licencia
	                FROM mapuche.dh03
	                WHERE (fec_baja IS NULL OR fec_baja >= '2025-05-01'::DATE) AND nro_legaj IN ($legajos_in)),

	-- CTE 4: Cargos con licencias vigentes detalladas
	cargos_con_licencias AS (SELECT dh03.nro_legaj,
	                                dh03.nro_cargo,
	                                CASE WHEN fec_alta <= '2025-05-01'::DATE
		                                     THEN DATE_PART( 'day', timestamp '2025-05-01')::INTEGER
		                                     ELSE DATE_PART( 'day', fec_alta::TIMESTAMP ) END AS inicio_cargo,
	                                CASE WHEN fec_baja > '2025-05-31'::DATE OR fec_baja IS NULL THEN date_part('day', TIMESTAMP '2025-05-31')::INTEGER
                ELSE date_part('day', fec_baja::TIMESTAMP)
            END AS final_cargo,
            CASE
                WHEN fec_desde <= '2025-05-01'::DATE 
                THEN date_part('day', TIMESTAMP '2025-05-01')::INTEGER
                ELSE date_part('day', fec_desde::TIMESTAMP)
            END AS inicio_lic,
            CASE
                WHEN fec_hasta > '2025-05-31'::DATE OR fec_hasta IS NULL 
                THEN date_part('day', TIMESTAMP '2025-05-31')::INTEGER
                ELSE date_part('day', fec_hasta::TIMESTAMP)
            END AS final_lic,
            CASE
                WHEN dl02.es_maternidad THEN 5::INTEGER
                ELSE CASE
                    WHEN dl02.es_remunerada THEN 1::INTEGER
                    ELSE 13::INTEGER
                END
            END AS condicion
	                         FROM mapuche.dh03 JOIN mapuche.dh05
	                         ON dh05.nro_cargo = dh03.nro_cargo LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
	                         WHERE (fec_baja IS NULL
		                         OR fec_baja >= '2025-05-01'::DATE)
		                        AND (fec_desde <= '2025-05-31'::DATE
		                        AND fec_hasta >= '2025-05-01'::DATE)
		                        AND dh03.nro_legaj IN ($legajos_in)
		                        AND dh03.nro_cargo NOT IN ( SELECT nro_cargo FROM mapuche.dh05 dh05_sub JOIN mapuche.dl02 dl02_sub ON (dh05_sub.nrovarlicencia = dl02_sub.nrovarlicencia) WHERE dh05_sub.nro_cargo = dh03.nro_cargo
		                        AND mapuche.map_es_licencia_vigente(dh05_sub.nro_licencia)
		                        AND (dh05_sub.fec_desde
		                          < mapuche.map_get_fecha_inicio_periodo() - 1)
		                        AND (dl02_sub.es_maternidad IS TRUE
		                         OR (NOT dl02_sub.es_remunerada
		                         OR (dl02_sub.es_remunerada
		                        AND dl02_sub.porcremuneracion = '0'))) ))

-- Resultado final: Todo combinado
SELECT 'limites' AS tipo_dato,
       lc.nro_legaj,
       NULL      AS nro_cargo,
       lc.minimo AS inicio,
       lc.maximo AS final,
       NULL      AS inicio_lic,
       NULL      AS final_lic,
       NULL      AS condicion,
       NULL      AS sin_licencia
FROM limites_cargos lc

UNION ALL

SELECT 'cargo_sin_licencia' AS tipo_dato,
       cb.nro_legaj,
       cb.nro_cargo,
       cb.inicio_cargo      AS inicio,
       cb.final_cargo       AS final,
       NULL                 AS inicio_lic,
       NULL                 AS final_lic,
       NULL                 AS condicion,
       cb.sin_licencia
FROM cargos_base cb
WHERE cb.sin_licencia = TRUE

UNION ALL

SELECT 'cargo_con_licencia' AS tipo_dato,
       ccl.nro_legaj,
       ccl.nro_cargo,
       ccl.inicio_cargo     AS inicio,
       ccl.final_cargo      AS final,
       ccl.inicio_lic,
       ccl.final_lic,
       ccl.condicion,
       NULL                 AS sin_licencia
FROM cargos_con_licencias ccl

ORDER BY nro_legaj, tipo_dato, nro_cargo;