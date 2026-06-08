import json
import time
import random
import datetime
import os


dossier_flux = "streaming_input"
os.makedirs(dossier_flux, exist_ok=True)


cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux"]
categories = ["Vehicules", "Immobilier", "Multimedia", "Maison", "Loisirs"]
actions = ["AIME", "VOUT", "ACHAT"]

print("Démarrage du générateur de flux réseau... (Appuyez sur Ctrl+C pour stopper)")

file_counter = 0


while True:
    event = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "user_id": f"usr_{random.randint(1000, 9999)}",
        "user_city": random.choice(cities),
        "product_id": f"prod_{random.randint(1000, 9999)}",
        "product_cat": random.choice(categories),
        "seller_id": f"sel_{random.randint(100, 999)}",
        "action_type": random.choice(actions),
        "price": round(random.uniform(10.0, 5000.0), 2)
    }
    
    filename = f"{dossier_flux}/event_{file_counter}.json"
    with open(filename, 'w') as f:
        json.dump(event, f)
        
    print(f"[LOG] {filename} créé : Action {event['action_type']} - Catégorie {event['product_cat']} - {event['price']}€")
    
    file_counter += 1
    time.sleep(2) 
