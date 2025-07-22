import pandas as pd
import psycopg
from config import conn_string

def copy_ratings():
    # Load CSV into DataFrame
    df = pd.read_csv("data/ratings.csv")
    df.columns = [col.lower() for col in df.columns]  # ensure lowercase column names

    # Connect and insert
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            # Create the table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ratings (
                    userid INTEGER,
                    movieid INTEGER,
                    rating DOUBLE PRECISION,
                    timestamp BIGINT
                );
            """)

            # Insert rows using executemany
            cur.executemany(
                "INSERT INTO ratings (userid, movieid, rating, timestamp) VALUES (%s, %s, %s, %s)",
                df.itertuples(index=False, name=None)
            )

        conn.commit()


def copy_movies():
    # Load CSV into DataFrame
    df = pd.read_csv("data/movies.csv")
    df.columns = [col.lower() for col in df.columns]  # ensure lowercase column names

    # Connect and insert
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            # Create the table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS movies (
                    movieid INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    genres TEXT
                );
            """)

            # Insert rows using executemany
            cur.executemany(
                "INSERT INTO movies (movieid, title, genres) VALUES (%s, %s, %s)",
                df.itertuples(index=False, name=None)
            )

        conn.commit()



copy_movies()