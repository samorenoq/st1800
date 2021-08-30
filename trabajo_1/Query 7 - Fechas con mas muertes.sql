SELECT fecha_de_muerte, count(*) as num_casos
FROM refined
GROUP BY fecha_de_muerte
ORDER BY num_casos
DESC
