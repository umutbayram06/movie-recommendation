from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode
import psycopg
from config import jdbc_url, conn_props, conn_string

def train_and_update_cf_recommendations(spark, movies_df):
    # 1. Load latest ratings from PostgreSQL
    ratings_df = spark.read.jdbc(
        url=jdbc_url,
        table="ratings",
        properties=conn_props
    ).select(
        col("userid").cast("int").alias("userId"),
        col("movieid").cast("int").alias("movieId"),
        col("rating").cast("float").alias("rating")
    )

    # 2. Train ALS model
    als = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
        implicitPrefs=False,
        rank=10,
        maxIter=10,
        regParam=0.1
    )
    model = als.fit(ratings_df)


    user_subset = ratings_df.select("userId").filter((col("userId") >= 1) & (col("userId") <= 20)).distinct()
    # 3. Generate top 10 recommendations per user
    user_recs = model.recommendForUserSubset(user_subset, 10)
    
    recs_exploded = user_recs.select(
        col("userId"),
        explode("recommendations").alias("rec")
    ).select(
        col("userId"),
        col("rec.movieId").alias("movieId"),
        col("rec.rating").alias("cf_score")
    )

    # 5. Join with movie metadata
    enriched_recs = recs_exploded.join(
        movies_df,
        on="movieId",
        how="left"
    )

    # 6. Collect and insert into PostgreSQL
    rows = enriched_recs.select("userId", "movieId", "cf_score", "title", "genres").collect()

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            # Delete all existing CF recommendations
            cur.execute("DELETE FROM recommendations_cf")

            for row in rows:
                cur.execute("""
                    INSERT INTO recommendations_cf (user_id, movie_id, cf_score, title, genres)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, movie_id) DO UPDATE SET
                        cf_score = EXCLUDED.cf_score,
                        title = EXCLUDED.title,
                        genres = EXCLUDED.genres
                """, (
                    row["userId"],
                    row["movieId"],
                    row["cf_score"],
                    row["title"],
                    row["genres"]
                ))

        conn.commit()
