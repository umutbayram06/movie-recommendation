from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, col, collect_list, row_number
from pyspark.sql.types import StructType, IntegerType, FloatType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
import psycopg
from cbf_functions import fetch_liked_movies_by_user, make_compute_user_profile_udf, cosine_similarity_udf
from cf_functions import train_and_update_cf_recommendations
from config import conn_string
import schedule
import threading
import time
import traceback


spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingTest") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .config("spark.jars", "jars/postgresql-42.7.7.jar") \
    .getOrCreate()
      
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.codegen.wholeStage", "false")

# File paths
ratings_path = "data/ratings.csv"
movies_path = "data/movies.csv"
genome_scores_path = "data/genome-scores.csv"
genome_tags_path = "data/genome-tags.csv"

# Load datasets
ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
genome_scores_df = spark.read.csv(genome_scores_path, header=True, inferSchema=True)
genome_tags_df = spark.read.csv(genome_tags_path, header=True, inferSchema=True)

# Pivot: rows → one row per movieId with tagId as columns
pivoted_df = genome_scores_df.groupBy("movieId") \
    .pivot("tagId") \
    .avg("relevance") \
    .na.fill(0)

# Get tag columns
tag_columns = pivoted_df.columns[1:]  # exclude movieId

# Assemble into feature vector
assembler = VectorAssembler(inputCols=tag_columns, outputCol="features")
movie_features_df = assembler.transform(pivoted_df).select("movieId", "features")

# Cache for performance
movie_features_df.cache()
movie_features_df.count()  # trigger caching

# Collect and convert to dictionary: movieId → list of floats
movie_features_dict = {
    row["movieId"]: row["features"].toArray().tolist()
    for row in movie_features_df.collect()
}

movie_features_broadcast = spark.sparkContext.broadcast(movie_features_dict)

def kafka_handler(batch_df, batch_id):
    if not batch_df.isEmpty():
        records = batch_df.select("userId", "movieId", "rating").collect()
        
        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                
                user_ids_set = set()

                for row in records:
                    user_id = row["userId"]
                    movie_id = row["movieId"]
                    rating = float(row["rating"])

                    user_ids_set.add(user_id)

                    cur.execute("""
                        INSERT INTO ratings (userid, movieid, rating, timestamp)
                        VALUES (%s, %s, %s, FLOOR(EXTRACT(EPOCH FROM NOW())))
                        ON CONFLICT (userid, movieid) DO UPDATE SET
                            rating = EXCLUDED.rating,
                            timestamp = FLOOR(EXTRACT(EPOCH FROM NOW()))
                    """, (user_id, movie_id, rating))

                conn.commit()
                
                user_ids = list(user_ids_set)
                
                for user_id in user_ids:
                    # Fetch liked movies (rating >= 4.0)
                    liked_df = fetch_liked_movies_by_user(user_id, spark)
                    liked_ids_df = liked_df.groupBy("userId").agg(collect_list("movieId").alias("liked_movie_ids"))
                    compute_user_profile_udf = make_compute_user_profile_udf(movie_features_broadcast)
                    
                    profiles_df = liked_ids_df.withColumn("profile", compute_user_profile_udf(col("liked_movie_ids")))
                    
                    # movie_features_df = movieId, features
                    # profiles_df = userId, profile
                    
                    joined = profiles_df.crossJoin(movie_features_df)
                    # Exclude liked movies
                    joined = joined.join(liked_df, on=["userId", "movieId"], how="left_anti")
                    
                    # Compute similarity
                    scored_df = joined.withColumn("cbf_score", cosine_similarity_udf(col("profile"), col("features")))
                    
                    windowSpec = Window.partitionBy("userId").orderBy(col("cbf_score").desc())
                    top_n_df = scored_df.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") <= 10)

                    enriched_df = top_n_df.join(movies_df, on="movieId", how="left")
                    
                    rows = enriched_df.select("userId", "movieId", "cbf_score", "title", "genres").collect()
                    
                    cur.execute("""
                    DELETE FROM recommendations_cbf
                    WHERE user_id = %s
                    """, (user_id,))
                    
                    for row in rows:
                        cur.execute("""
                        INSERT INTO recommendations_cbf (user_id, movie_id, cbf_score, title, genres)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, movie_id) DO UPDATE SET
                            cbf_score = EXCLUDED.cbf_score,
                            title = EXCLUDED.title,
                            genres = EXCLUDED.genres
                        """, (
                            user_id,
                            row["movieId"],
                            row["cbf_score"],
                            row["title"],
                            row["genres"]
                        ))
                        
                    conn.commit()  
                    


def run_cf_periodically():
    # Run once immediately
    try:
        train_and_update_cf_recommendations(spark, movies_df)
    except Exception:
        traceback.print_exc()
    
    schedule.every(3).minutes.do(lambda: train_and_update_cf_recommendations(spark, movies_df))

    while True:
        try:
            schedule.run_pending()
            time.sleep(5)
        except Exception:
            traceback.print_exc()
 

 
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ratings") \
    .option("startingOffsets", "earliest") \
    .load()
    
schema = StructType() \
    .add("userId", IntegerType()) \
    .add("movieId", IntegerType()) \
    .add("rating", FloatType())
    

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")
    

query = parsed_df.writeStream \
    .foreachBatch(kafka_handler) \
    .outputMode("update") \
    .start()
    

cf_thread = threading.Thread(target=run_cf_periodically, daemon=True)
cf_thread.start()


query.awaitTermination()
 

"""
{"userId": 1, "movieId":2, "rating": 3}
{"userId": 2, "movieId":2, "rating": 5}
{"userId": 2, "movieId":3, "rating": 5}
{"userId": 2, "movieId":4, "rating": 5}
"""