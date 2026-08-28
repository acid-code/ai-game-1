import uuid
import json
import threading
from confluent_kafka import Producer, Consumer

class GameClient:
    def __init__(self, player_name):
        self.player_name = player_name
        self.client_id = str(uuid.uuid4())
        
        # Connecting to the exposed external port
        self.producer = Producer({'bootstrap.servers': 'localhost:9094'})
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9094',
            'group.id': f'client-{self.client_id}', # Unique group so every client sees all messages
            'auto.offset.reset': 'latest'
        })
        self.consumer.subscribe(['game-answers'])
        
        # Start a background thread to listen for answers
        threading.Thread(target=self._listen_for_answers, daemon=True).start()

    def _listen_for_answers(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error(): continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            # Only print the answer if it belongs to this specific UUID!
            if data['client_id'] == self.client_id:
                print(f"\n[NPC]: {data['answer']}\n")
                action = input("\n> ")
                if action.lower() == 'quit':
                    break
                player.send_action(action)

    def send_action(self, action_text):
        payload = {"client_id": self.client_id, "prompt": action_text}
        self.producer.produce('game-questions', value=json.dumps(payload).encode('utf-8'))
        self.producer.flush()

if __name__ == "__main__":
    player = GameClient(player_name="asaf2")
    print(f"Logged in as {player.player_name} (UUID: {player.client_id})")
    print("Connecting to the game world... Type 'quit' to exit.")
    
    action = input("\n> ")
    player.send_action(action)
    while True:
        continue