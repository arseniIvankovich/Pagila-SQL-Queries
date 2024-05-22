--1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.

SELECT category.name, COUNT(film.film_id) AS number_of_films FROM category
LEFT JOIN film_category ON category.category_id = film_category.category_id
LEFT JOIN film ON film.film_id = film_category.film_id
GROUP BY category.category_id, category.name
ORDER BY number_of_films DESC;



--2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

SELECT  actor.first_name, actor.last_name, COUNT(rent.rental_id) AS rental_count
FROM actor
JOIN film_actor fa ON actor.actor_id = fa.actor_id
JOIN film ON fa.film_id = film.film_id
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental rent ON inventory.inventory_id = rent.inventory_id
GROUP BY actor.actor_id,actor.first_name, actor.last_name
ORDER BY rental_count DESC
LIMIT 10;

--Через окононнки
SELECT DISTINCT act.first_name, act.last_name, COUNT(rent.rental_id) OVER (PARTITION BY act.actor_id, act.first_name, act.last_name)
	AS rental_count
FROM "actor" AS act
JOIN "film_actor" fa ON act.actor_id = fa.actor_id
JOIN "film" ON fa.film_id = "film".film_id
JOIN "inventory" ON "film".film_id = "inventory".film_id
JOIN "rental" rent ON "inventory".inventory_id = rent.inventory_id
ORDER BY rental_count DESC
LIMIT 10;

--3. Вывести категорию фильмов, на которую потратили больше всего денег.

SELECT category.name, SUM(payment.amount) AS sum_spend
FROM category
JOIN film_category fc ON category.category_id = fc.category_id
JOIN film ON fc.film_id = film.film_id
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental rent ON inventory.inventory_id = rent.inventory_id
JOIN payment ON payment.rental_id = rent.rental_id
GROUP BY category.category_id, category.name
ORDER BY sum_spend DESC
LIMIT 1;

--4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

SELECT * FROM film
WHERE NOT EXISTS (
	SELECT 1
	FROM inventory 
	WHERE inventory.film_id = film.film_id
)

--5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

SELECT actor.first_name, actor.last_name 
	FROM 
(
SELECT actor.actor_id, DENSE_RANK() 
OVER(ORDER BY COUNT(fc.film_id) DESC) AS row_rank
FROM category 
JOIN film_category fc ON fc.category_id = category.category_id
JOIN film ON fc.film_id = film.film_id
JOIN film_actor fa ON fa.film_id = film.film_id
JOIN actor ON actor.actor_id = fa.actor_id
WHERE category.name = 'Children' 
GROUP BY actor.actor_id
) AS inner_query
JOIN actor ON actor.actor_id = inner_query.actor_id
WHERE row_rank IN (1,2,3)

--6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

SELECT  DISTINCT city, SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY city.city_id) AS active_customer,
	SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY city) AS inactive_customer
	FROM city
JOIN address ON address.city_id = city.city_id
JOIN customer ON customer.address_id = address.address_id
ORDER BY inactive_customer DESC

--7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. 
--То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

WITH category_hours AS (
    SELECT 
        category.name AS category_name,
        city.city AS city_name,
       SUM(EXTRACT(HOUR FROM (rent.return_date - rent.rental_date)))
	AS rent_sum
    FROM category
    JOIN film_category fc ON fc.category_id = category.category_id
    JOIN film ON fc.film_id = film.film_id
    JOIN inventory ON inventory.film_id = film.film_id
    JOIN rental rent ON rent.inventory_id = inventory.inventory_id
    JOIN customer ON customer.customer_id = rent.customer_id
    JOIN address ON address.address_id = customer.address_id
    JOIN city ON city.city_id = address.city_id
	GROUP BY category_name, city_name
)
SELECT category_name,city_name,rent_sum FROM(
SELECT category_name,city_name,rent_sum,  DENSE_RANK() OVER(PARTITION BY city_name ORDER BY rent_sum DESC) AS rank_ FROM category_hours 
) inner_query
WHERE rank_ = 1 AND city_name ILIKE 'A%'
UNION ALL
SELECT category_name,city_name,rent_sum FROM(
SELECT category_name,city_name,rent_sum,  DENSE_RANK() OVER(PARTITION BY city_name ORDER BY rent_sum DESC) AS rank_ FROM category_hours 
) inner_query
WHERE rank_ = 1 AND city_name LIKE '%-%'
