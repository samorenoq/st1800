SELECT edad, count(*) as num_casos
FROM refined
GROUP BY edad
ORDER BY num_casos
DESC