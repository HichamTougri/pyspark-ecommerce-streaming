import json
import time
import random
import datetime
import os

# Préparation du dossier
# On choisit le nom du dossier et on le crée s'il n'existe pas encore
dossier_flux = "streaming_input"
os.makedirs(dossier_flux, exist_ok=True)

# Données de base
cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux"]
categories = ["Vehicules", "Immobilier", "Multimedia", "Maison", "Loisirs"]
actions = ["AIME", "VOUT", "ACHAT"]

print("Démarrage du générateur de flux réseau... (Appuyez sur Ctrl+C pour stopper)")

# Un numéro qui va augmenter pour nommer les fichiers (event_0, event_1, etc.)
file_counter = 0

# Boucle infini
while True:
    
    # On crée une fausse action avec des informations tirées au hasard
    event = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z", # Heure exacte
        "user_id": f"usr_{random.randint(1000, 9999)}",            # Numéro d'utilisateur
        "user_city": random.choice(cities),                        # Ville choisie au hasard
        "product_id": f"prod_{random.randint(1000, 9999)}",        # Numéro de produit
        "product_cat": random.choice(categories),                  # Catégorie choisie au hasard
        "seller_id": f"sel_{random.randint(100, 999)}",            # Numéro de vendeur
        "action_type": random.choice(actions),                     # Action choisie au hasard
        "price": round(random.uniform(10.0, 5000.0), 2)            # Prix aléatoire entre 10 et 5000
    }
    
    # On choisit le nom du fichier et on l'enregistre dans notre dossier
    filename = f"{dossier_flux}/event_{file_counter}.json"
    with open(filename, 'w') as f:
        json.dump(event, f)
        
    # On affiche un petit message sur l'écran pour confirmer que ça a marché
    print(f"[LOG] {filename} créé : Action {event['action_type']} - Catégorie {event['product_cat']} - {event['price']}€")
    
    # On ajoute +1 au compteur pour que le prochain fichier ait un numéro différent
    file_counter += 1
    
    # On met le programme en pause pendant 2 secondes avant de recommencer
    time.sleep(2)
    
