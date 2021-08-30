SELECT recuperado, count(*) as num_casos
FROM refined
GROUP BY recuperado
ORDER BY num_casos
DESC
