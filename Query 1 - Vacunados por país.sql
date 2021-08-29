SELECT location, MAX(people_fully_vaccinated) AS total_vaccinated, ROUND(MAX(people_fully_vaccinated_per_hundred),2) AS total_vaccinated_per_hundred
FROM trabajo1.vacunaciones
GROUP BY location
ORDER BY total_vaccinated_per_hundred
DESC;