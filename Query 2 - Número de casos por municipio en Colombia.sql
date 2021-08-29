SELECT nombre_municipio, count(*) as num_casos
FROM "trabajo1-trusted".trusted
GROUP BY nombre_municipio
ORDER BY num_casos
DESC
