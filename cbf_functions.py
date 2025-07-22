from config import jdbc_url, conn_props
import math
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType

def fetch_liked_movies_by_user(user_id, spark):
    query = f"(SELECT userid, movieid FROM ratings WHERE userid = {user_id} AND rating >= 4.0) AS user_likes"

    liked_df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=conn_props
    )
    
    return liked_df

def make_compute_user_profile_udf(movie_features_broadcast):
    @udf(returnType=ArrayType(FloatType()))
    def _compute_user_profile_udf(liked_movie_ids):
        features = movie_features_broadcast.value
        vectors = [features[movie_id] for movie_id in liked_movie_ids if movie_id in features]
        if not vectors:
            return None
        profile = [float(sum(x) / len(vectors)) for x in zip(*vectors)]
        return profile
    return _compute_user_profile_udf


@udf(returnType=FloatType())
def cosine_similarity_udf(profile_vec, movie_vec):
    dot = sum(a * b for a, b in zip(profile_vec, movie_vec))
    norm1 = math.sqrt(sum(a * a for a in profile_vec))
    norm2 = math.sqrt(sum(b * b for b in movie_vec))
    return float(dot / (norm1 * norm2)) if norm1 and norm2 else 0.0