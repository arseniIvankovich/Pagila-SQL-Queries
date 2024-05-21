# Pagila-SQL-Queries

 1. Display the number of films in each category, sort them in descending order.

 ```sql
 SELECT category.name, COUNT(film.film_id) AS number_of_films FROM category
LEFT JOIN film_category ON category.category_id = film_category.category_id
LEFT JOIN film ON film.film_id = film_category.film_id
GROUP BY category.category_id, category.name
ORDER BY number_of_films DESC;
```

2. Display the 10 actors whose films were rented the most, sort in descending order.
```sql
SELECT  actor.first_name, actor.last_name, COUNT(rent.rental_id) AS rental_count
FROM actor
JOIN film_actor fa ON actor.actor_id = fa.actor_id
JOIN film ON fa.film_id = film.film_id
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental rent ON inventory.inventory_id = rent.inventory_id
GROUP BY actor.actor_id,actor.first_name, actor.last_name
ORDER BY rental_count DESC
LIMIT 10;
```
through window functions
```sql
SELECT DISTINCT act.first_name, act.last_name, COUNT(rent.rental_id) OVER (PARTITION BY act.actor_id, act.first_name, act.last_name)
	AS rental_count
FROM "actor" AS act
JOIN "film_actor" fa ON act.actor_id = fa.actor_id
JOIN "film" ON fa.film_id = "film".film_id
JOIN "inventory" ON "film".film_id = "inventory".film_id
JOIN "rental" rent ON "inventory".inventory_id = rent.inventory_id
ORDER BY rental_count DESC
LIMIT 10;
```

3. Display the category of films that you spent the most money on.
```sql
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
```
4. Print the names of the films that are not in the inventory. Write a query without using the IN operator.
```sql
SELECT * FROM film
WHERE NOT EXISTS (
	SELECT 1
	FROM inventory 
	WHERE inventory.film_id = film.film_id
)
```
5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
```sql
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
WHERE row_rank IN (1,2,3);
```
6. Display the cities with the number of active and inactive customers (active — customer.active = 1). Sort by the number of inactive clients in descending order.
```sql
SELECT  DISTINCT city, SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY city.city_id) AS active_customer,
	SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY city) AS inactive_customer
	FROM city
JOIN address ON address.city_id = city.city_id
JOIN customer ON customer.address_id = address.address_id
ORDER BY inactive_customer DESC;
```
7. Display the category of films that has the largest number of total rental hours in cities (customer.address_id in this city), and which begin with the letter "a".Do the same for cities that have the “-” symbol. Write everything in one request.
```sql
WITH category_hours AS (
SELECT category.name AS category_name, city.city AS city_name,
	EXTRACT(HOUR FROM (rent.return_date - rent.rental_date)) AS rent_in_hours
	FROM category
JOIN film_category fc ON fc.category_id = category.category_id
JOIN film ON fc.film_id = film.film_id
JOIN inventory ON inventory.film_id = film.film_id
JOIN rental rent ON rent.inventory_id = inventory.inventory_id
JOIN customer ON customer.customer_id = rent.customer_id
JOIN address ON address.address_id = customer.address_id
JOIN city ON city.city_id = address.city_id
)

(
SELECT category_name, 
SUM(rent_in_hours) AS total_rent_in_hours
FROM category_hours
WHERE city_name ILIKE 'A%'
GROUP BY category_name
ORDER BY total_rent_in_hours DESC
LIMIT 1
)
UNION ALL 
(
SELECT category_name,
SUM(rent_in_hours) AS total_rent_in_hours
FROM category_hours
WHERE city_name LIKE '%-%'
GROUP BY category_name
ORDER BY total_rent_in_hours DESC
LIMIT 1
	);
```
