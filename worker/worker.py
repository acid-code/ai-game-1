import json
import requests
import time
from confluent_kafka import Consumer, Producer
from pymongo import MongoClient

# Wait a moment for infrastructure to spin up
time.sleep(10)

# Connect to internal Docker services
mongo_client = MongoClient('mongodb://mongodb:27017/')
db = mongo_client['game_database']
history_collection = db['player_histories']

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'ai-worker-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['game-questions'])
producer = Producer({'bootstrap.servers': 'kafka:9092'})

def process_message():
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error(): continue

        data = json.loads(msg.value().decode('utf-8'))
        client_id, prompt = data['client_id'], data['prompt']
        
        print(f"Worker picked up question from {client_id}")

        # 1. Fetch or Initialize MongoDB State
        record = history_collection.find_one({"client_id": client_id})
        chat_history = record["messages"] if record else [{"role": "system", "content": "You are taylor swift. And we are also best friends and we speak regualrly. Answer the question as taylor swift would answer."}]
        chat_history.append({"role": "user", "content": prompt})
        
        # 2. Ask Ollama
        try:
            payload = {"model": "llama3.2", "messages": chat_history, "stream": False}
            res = requests.post("http://ollama:11434/api/chat", json=payload).json()
            ai_answer = res["message"]["content"]
        except Exception as e:
            ai_answer = "*The NPC stares blankly.* (Error connecting to Brain)"

        # 3. Save updated state back to Mongo
        chat_history.append({"role": "assistant", "content": ai_answer})
        history_collection.update_one(
            {"client_id": client_id}, 
            {"$set": {"messages": chat_history}}, 
            upsert=True # Creates the document if it's a brand new player
        )
        
        # 4. Push answer back to Kafka
        answer_payload = {"client_id": client_id, "answer": ai_answer}
        producer.produce('game-answers', value=json.dumps(answer_payload).encode('utf-8'))
        producer.flush()

if __name__ == "__main__":
    print("AI Worker Booted and Listening...")
    process_message()