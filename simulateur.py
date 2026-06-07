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

