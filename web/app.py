from flask import Flask, render_template, request, redirect, url_for, make_response, jsonify
from db import get_connection
from kafka_producer import send_rating_to_kafka

app = Flask(__name__)

@app.route('/sign-in')
def index():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT userid, username FROM users")
            users = cur.fetchall()
    return render_template('index.html', users=users)

@app.route("/sign-in/", methods=["POST"])
def sign_in():
    user_id = request.args.get("user_id")
    if not user_id:
        return "Missing user_id", 400

    response = make_response(redirect(f"/movies/users/{user_id}"))
    response.set_cookie("userid", user_id)
    return response

@app.route("/movies/users/<int:user_id>")
def user_recommendations(user_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Fetch recommendations from collaborative filtering table
            cur.execute("""
                SELECT title, genres
                FROM recommendations_cf
                WHERE user_id = %s
            """, (user_id,))
            cf_recs = cur.fetchall()

            # Fetch recommendations from content-based filtering table
            cur.execute("""
                SELECT title, genres
                FROM recommendations_cbf
                WHERE user_id = %s
            """, (user_id,))
            cbf_recs = cur.fetchall()

    return render_template("recommendations.html", user_id=user_id, cf_recs=cf_recs, cbf_recs=cbf_recs)

@app.route("/movies", methods=["GET"])
def movies_page():
    # Just serve the HTML page without movies data
    user_id = request.cookies.get("userid") or request.args.get("user_id")
    return render_template("movies.html", user_id=user_id)

@app.route("/movies/api", methods=["GET"])
def movies_api():
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 20))
    genre = request.args.get("genre", "").lower()
    user_id = request.cookies.get("userid") or request.args.get("user_id")

    with get_connection() as conn:
        with conn.cursor() as cur:
            if genre:
                cur.execute("""
                    SELECT movieid, title, genres FROM movies
                    WHERE LOWER(genres) LIKE %s
                    ORDER BY title OFFSET %s LIMIT %s
                """, (f"%{genre}%", offset, limit))
            else:
                cur.execute("""
                    SELECT movieid, title, genres FROM movies
                    ORDER BY title OFFSET %s LIMIT %s
                """, (offset, limit))

            movies = cur.fetchall()

            user_ratings = {}
            if user_id:
                cur.execute("SELECT movieid, rating FROM ratings WHERE userid = %s AND movieid = ANY(%s)",
                            (user_id, [m["movieid"] for m in movies]))
                user_ratings = {r["movieid"]: r["rating"] for r in cur.fetchall()}

    return {"movies": movies, "user_ratings": user_ratings}


@app.route("/rate", methods=["PUT"])
def rate_movie():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON body"}), 400

    user_id = request.cookies.get("userid", type=int)
    movie_id = data.get("movieid")
    rating = data.get("rating")

    if not (user_id and movie_id and rating is not None):
        return jsonify({"error": "userid, movieid, and rating are required"}), 400

    send_rating_to_kafka(user_id, movie_id, rating)

    
    # with get_connection() as conn:
    #     with conn.cursor() as cur:
    #         # Upsert rating (insert or update)
    #         cur.execute("""
    #             INSERT INTO ratings (userid, movieid, rating, timestamp)
    #             VALUES (%s, %s, %s, EXTRACT(EPOCH FROM NOW())::BIGINT)
    #             ON CONFLICT (userid, movieid) DO UPDATE SET rating = EXCLUDED.rating, timestamp = EXCLUDED.timestamp
    #         """, (user_id, movie_id, rating))
    #     conn.commit()
    
    return jsonify({"message": "Rating saved"}), 200

    
if __name__ == '__main__':
    app.run(debug=True)

