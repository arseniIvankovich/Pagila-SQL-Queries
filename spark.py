import os
import pyspark.sql.dataframe as pdf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum, dense_rank, when, count, lit
from dotenv import load_dotenv
from pyspark.sql.window import Window

URL="jdbc:postgresql://localhost:5432/pagila"

load_dotenv()

db_properties = {}
db_properties['user']=os.environ["DATABASE_USER"]
db_properties['password']=os.environ["DATABASE_PASSWORD"]

def spark_session() -> SparkSession:
    
    """_summary_
    Return spark session
    """
    
    spark = (
        SparkSession.builder 
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .appName("PySpark_Postgres").getOrCreate()
    )

    return spark


def film_and_category(category_df: pdf.DataFrame, film_category_df: pdf.DataFrame, 
                  film_df: pdf.DataFrame) -> pdf.DataFrame:
    
    """_summary_
    Display the number of films in each category, sort them in descending order.
    
    Args:
        category_df (pyspark.sql.dataframe.DataFrame): dataframe of category table in postgre database
        film_category_df (pyspark.sql.dataframe.DataFrame):  dataframe of film_category table in postgre database
        film_df (pyspark.sql.dataframe.DataFrame):  dataframe of film table in postgre database
        
    Returns:
        pyspark.sql.dataframe.DataFrame:  result dataframe
    """
    
    joined_df = (
            category_df
            .join(film_category_df, category_df.category_id == film_category_df.category_id,'left')
            .join(film_df, film_category_df.film_id == film_df.film_id, 'left')
    )

    result_df = (
        joined_df
        .groupBy(category_df.name) 
        .agg(count(film_df.film_id).alias('number_of_films'))
        .orderBy(col('number_of_films').desc())
    )
    
                            
    return result_df

def ten_actors_most_rented_count(actor_df: pdf.DataFrame, film_actor_df: pdf.DataFrame, 
                                 film_df: pdf.DataFrame, inventory_df: pdf.DataFrame, 
                                 rental_df: pdf.DataFrame) -> pdf.DataFrame:
    
    """_summary_
    Display the 10 actors whose films were rented the most, sort in descending order.
    
    Args:
        actor_df (pyspark.sql.dataframe.DataFrame): dataframe of actor table in postgre database
        film_actor_df (pyspark.sql.dataframe.DataFrame):  dataframe of film_actor table in postgre database
        film_df (pyspark.sql.dataframe.DataFrame):  dataframe of film table in postgre database
        inventory_df (pyspark.sql.dataframe.DataFrame):  dataframe of inventory table in postgre database
        rental_df (pyspark.sql.dataframe.DataFrame):  dataframe of rental table in postgre database
        
    Returns:
        pyspark.sql.dataframe.DataFrame:  result dataframe
    """
    
    joined_df = (
            actor_df.join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id)
            .join(film_df, film_actor_df.film_id == film_df.film_id)
            .join(inventory_df, film_df.film_id == inventory_df.film_id)
            .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id)
    )
    
    
    result_df = (
        joined_df
        .groupBy(actor_df.first_name, actor_df.last_name, actor_df.actor_id)
        .agg(count(rental_df.rental_id).alias('rental_count'))
        .orderBy(col('rental_count').desc())
    )
    
                        
    return result_df.select("first_name", "last_name", "rental_count").limit(10)
    
def category_most_spend_money(category_df: pdf.DataFrame, film_category_df: pdf.DataFrame, 
                              film_df: pdf.DataFrame, inventory_df: pdf.DataFrame, 
                              rental_df: pdf.DataFrame, payment_df: pdf.DataFrame) -> pdf.DataFrame:
    
    """_summary_
    Display the category of films that you spent the most money on.
    
    Args:
        category_df (pyspark.sql.dataframe.DataFrame): dataframe of category table in postgre database
        film_category_df (pyspark.sql.dataframe.DataFrame):  dataframe of film_category table in postgre database
        film_df (pyspark.sql.dataframe.DataFrame):  dataframe of film table in postgre database
        inventory_df (pyspark.sql.dataframe.DataFrame):  dataframe of inventory table in postgre database
        rental_df (pyspark.sql.dataframe.DataFrame):  dataframe of rental table in postgre database
        payment_df (pyspark.sql.dataframe.DataFrame):  dataframe of payment table in postgre database

    Returns:
        pyspark.sql.dataframe.DataFrame:  result dataframe
    """
    
    joined_df = (
        category_df
        .join(film_category_df, category_df.category_id == film_category_df.category_id)
        .join(film_df, film_category_df.film_id == film_df.film_id)
        .join(inventory_df, film_df.film_id == inventory_df.film_id)
        .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id)
        .join(payment_df, payment_df.rental_id == rental_df.rental_id)
    )
    
    
    result_df = (
        joined_df
        .groupBy(category_df.category_id, category_df.name)
        .agg(sum(payment_df.amount).alias('sum_spend'))
        .orderBy(col('sum_spend').desc()
    )
)
    return result_df.select(category_df.name, 'sum_spend').limit(1)

def film_not_in_inventory(film_df: pdf.DataFrame, inventory_df: pdf.DataFrame) -> pdf.DataFrame:
    """_summary_
    Display the names of the films that are not in the inventory. Write a query without using the IN operator.
    Args:
        film_df (pyspark.sql.dataframe.DataFrame):  dataframe of film table in postgre database
        inventory_df (pyspark.sql.dataframe.DataFrame):  dataframe of inventory table in postgre database

    Returns:
        pyspark.sql.dataframe.DataFrame:  result dataframe
    """
    
    result_df = (
            film_df
            .join(inventory_df, film_df.film_id == inventory_df.film_id,'left_anti')
    )
    
    return result_df

def top_3_actor_chikdren_category(category_df: pdf.DataFrame, film_category_df: pdf.DataFrame, 
                              film_df: pdf.DataFrame, actor_df: pdf.DataFrame, 
                              film_actor_df: pdf.DataFrame)-> pdf.DataFrame:
    """_summary_
    Display the top 3 actors who appeared most in films in the "Children" category. 
    If several actors have the same number of films, output all of them.
    
    Args:
        category_df (pyspark.sql.dataframe.DataFrame): dataframe of category table in postgre database
        film_category_df (pyspark.sql.dataframe.DataFrame):  dataframe of film_category table in postgre database
        film_df (pyspark.sql.dataframe.DataFrame):  dataframe of film table in postgre database
        actor_df (pyspark.sql.dataframe.DataFrame): dataframe of actor table in postgre database
        film_actor_df (pyspark.sql.dataframe.DataFrame):  dataframe of film_actor table in postgre database

    Returns:
        pyspark.sql.dataframe.DataFrame:  result dataframe
    """
    
    joined_df = (
        category_df
        .join(film_category_df, category_df.category_id == film_category_df.category_id)
        .join(film_df, film_category_df.film_id == film_df.film_id)
        .join(film_actor_df, film_df.film_id == film_actor_df.film_id)
        .join(actor_df, film_actor_df.actor_id == actor_df.actor_id)
    )
    
                        
    children_df = joined_df.filter(category_df.name == 'Children')

    actor_film_count_df = (
        children_df
        .groupBy(actor_df.actor_id)
        .agg(count(film_df.film_id).alias('film_count'))
    )
    

    window = Window.partitionBy(lit(0)).orderBy(col('film_count').desc())
    ranked_df = actor_film_count_df.withColumn('row_rank', dense_rank().over(window))

    top_ranked_df = ranked_df.filter(col('row_rank').isin(1, 2, 3))

    result_df = (
        top_ranked_df
        .join(actor_df, top_ranked_df.actor_id == actor_df.actor_id)
        .select('first_name', 'last_name')
    )
    
    return result_df

def city_with_active_inactive_customers(city_df: pdf.DataFrame, address_df: pdf.DataFrame, 
                              customer_df: pdf.DataFrame)-> pdf.DataFrame:
    """_summary_
    Display the cities with the number of active and inactive customers (active — customer.active = 1). 
    Sort by the number of inactive clients in descending order.
    
    Args:
        city_df (pyspark.sql.dataframe.DataFrame):  dataframe of city table in postgre database
        address_df (pyspark.sql.dataframe.DataFrame):  dataframe of address table in postgre database
        customer_df (pyspark.sql.dataframe.DataFrame):  dataframe of customer table in postgre database
    Returns:
        pyspark.sql.dataframe.DataFrame: result dataframe
    """
    
    joined_df = (
        city_df 
        .join(address_df, city_df.city_id == address_df.city_id)
        .join(customer_df, address_df.address_id == customer_df.address_id)
    )
    

    window_spec = Window.partitionBy(city_df.city_id)

    joined_df = joined_df.withColumn(
        "active_customer",
        sum(when(customer_df.active == 1, 1).otherwise(0)).over(window_spec)
    ).withColumn(
        "inactive_customer",
        sum(when(customer_df.active == 0, 1).otherwise(0)).over(window_spec)
    )

    result_df = (
        joined_df
        .select(city_df.city,col("active_customer"), col("inactive_customer"))
        .distinct()
        .orderBy(col("inactive_customer").desc())
    )
    
    
    return result_df

def city_with_highest_rent(category_df: pdf.DataFrame, film_category_df: pdf.DataFrame, 
                        film_df: pdf.DataFrame, inventory_df: pdf.DataFrame, 
                        rental_df: pdf.DataFrame, customer_df: pdf.DataFrame,
                        city_df: pdf.DataFrame, address_df: pdf.DataFrame) -> pdf.DataFrame:
    """_summary_
   Display the category of films that has the largest number of total 
   rental hours in cities (customer.address_id in this city), 
   and which begin with the letter "a".
   Do the same for cities that have the “-” symbol. Write everything in one request.
    
    Args:
        category_df (pyspark.sql.dataframe.DataFrame): dataframe of category table in postgre database
        film_category_df (pyspark.sql.dataframe.DataFrame):  dataframe of film_category table in postgre database
        film_df (pyspark.sql.dataframe.DataFrame):  dataframe of film table in postgre database
        inventory_df (pyspark.sql.dataframe.DataFrame):  dataframe of inventory table in postgre database
        rental_df (pyspark.sql.dataframe.DataFrame):  dataframe of rental table in postgre database
        city_df (pyspark.sql.dataframe.DataFrame):  dataframe of city table in postgre database
        address_df (pyspark.sql.dataframe.DataFrame):  dataframe of address table in postgre database
        customer_df (pyspark.sql.dataframe.DataFrame):  dataframe of customer table in postgre database
    Returns:
        pyspark.sql.dataframe.DataFrame: result dataframe
    """
    
    joined_df = (
        category_df 
        .join(film_category_df, category_df.category_id == film_category_df.category_id)
        .join(film_df, film_category_df.film_id == film_df.film_id)
        .join(inventory_df, film_df.film_id == inventory_df.film_id)
        .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id)
        .join(customer_df, rental_df.customer_id == customer_df.customer_id)
        .join(address_df, customer_df.address_id == address_df.address_id)
        .join(city_df, address_df.city_id == city_df.city_id)
    )
    
    category_rent_df = (
        joined_df
        .groupBy(category_df.name) 
        .agg(
        sum(when(city_df.city.like("a%"), film_df.rental_duration).otherwise(0)).alias("rent_a"),
        sum(when(city_df.city.like("%-%"), film_df.rental_duration).otherwise(0)).alias("rent_")
        )
    )
    
    rent_a_top_df = (
        category_rent_df
        .orderBy(col("rent_a").desc())
        .limit(1) 
        .select(col("name").alias("category_name"), col("rent_a").alias("rent"))
    )
    

    rent__top_df = (
        category_rent_df
        .orderBy(col("rent_").desc())
        .limit(1)
        .select(col("name").alias("category_name"), col("rent_").alias("rent"))
    )
    
    result_df = rent_a_top_df.unionAll(rent__top_df)
    
    return result_df
    
    
def main():
    
    """
    _summary_
    base function that runs spark etl 
    """
    
    spark = spark_session()    
    
    category_df = spark.read.jdbc(url=URL,table="category",properties=db_properties)
    film_category_df = spark.read.jdbc(url=URL,table="film_category",properties=db_properties)
    film_df =  spark.read.jdbc(url=URL,table="film",properties=db_properties)
    actor_df = spark.read.jdbc(url=URL,table="actor",properties=db_properties)
    film_actor_df = spark.read.jdbc(url=URL,table="film_actor",properties=db_properties)
    inventory_df = spark.read.jdbc(url=URL,table="inventory",properties=db_properties)
    rental_df = spark.read.jdbc(url=URL,table="rental",properties=db_properties)
    payment_df = spark.read.jdbc(url=URL,table="payment",properties=db_properties)
    city_df = spark.read.jdbc(url=URL,table="city",properties=db_properties)
    address_df = spark.read.jdbc(url=URL,table="address",properties=db_properties)
    customer_df = spark.read.jdbc(url=URL,table="customer",properties=db_properties)

    

    film_and_category(category_df, film_category_df, film_df).show()
    ten_actors_most_rented_count(actor_df, film_actor_df, film_df, inventory_df, rental_df).show()
    category_most_spend_money(category_df,film_category_df, film_df, inventory_df, rental_df, payment_df).show()
    film_not_in_inventory(film_df, inventory_df).show(6)
    top_3_actor_chikdren_category(category_df, film_category_df, film_df, actor_df, film_actor_df).show()
    city_with_active_inactive_customers(city_df, address_df, customer_df).show()
    city_with_highest_rent(category_df, film_category_df, film_df, inventory_df,\
                           rental_df, customer_df, city_df, address_df).show()
    
if __name__ == "__main__":
    main()
