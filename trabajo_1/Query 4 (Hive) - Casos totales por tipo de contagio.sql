SELECT tipo_de_contagio, COUNT(*) as total_casos
FROM trusted
GROUP BY tipo_de_contagio
ORDER BY total_casos
DESC;