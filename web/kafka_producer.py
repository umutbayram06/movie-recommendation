from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_rating_to_kafka(userid, movieid, rating):
    message = {"userId": userid, "movieId": movieid, "rating": rating}
    producer.send("ratings", message)
    producer.flush()  # ensure delivery
