# LeBonCoin - Dashboard Graphe de Connexions Temps Réel

Ce projet simule des flux d'interactions utilisateurs sur une plateforme de type "LeBonCoin" (utilisateurs, produits, vendeurs) et les représente sous la forme d'un graphe dynamique.

## Architecture du Projet

Le système repose sur trois composants principaux qui fonctionnent en parallèle :

1.  **Générateur de flux (`simulateur.py`) :** Un script Python qui génère en continu des événements aléatoires (achats, likes, vues) au format JSON et les dépose dans un répertoire source.
2.  **Moteur de traitement (`pipeline_graph.py`) :** Une application PySpark (Structured Streaming) qui écoute le répertoire source, traite les données par micro-lots (micro-batches), extrait les relations (nœuds et arêtes) et exporte l'état du graphe dans un fichier JSON.
3.  **Visualisation (`index.html`) :** Un tableau de bord web basé sur la bibliothèque Vis.js qui lit le fichier JSON généré et met à jour dynamiquement la topologie du réseau toutes les 3 secondes.

## Prérequis

Pour exécuter ce projet localement, votre environnement doit disposer des éléments suivants :

* **Python 3.8+** installé sur votre machine.
* **Java 8 ou 11** (Requis par Apache Spark).
* **Apache Spark** fonctionnel en local.
* Le module Python `pyspark` installé.

Vous pouvez installer les dépendances Python via la commande :
`pip install pyspark`

*Note technique : Le script PySpark utilise le package `graphframes`. Il est configuré pour le télécharger automatiquement au lancement via la configuration Spark (`spark.jars.packages`). Une connexion internet est donc requise au premier lancement.*

## 🚀 Guide de Démarrage

Pour simuler le flux en temps réel, vous devez ouvrir **trois terminaux séparés** à la racine du projet.

### Étape 1 : Lancer le Simulateur
Dans le premier terminal, exécutez le simulateur. Ce script va créer le dossier `streaming_input` et y déposer de nouveaux fichiers JSON toutes les 2 secondes.

`python simulateur.py`
*(Laissez ce terminal ouvert et actif).*

### Étape 2 : Lancer le Pipeline de Traitement (PySpark)
Dans le deuxième terminal, démarrez le pipeline de données. Spark va lire les fichiers générés par le simulateur et produire en continu le fichier `graph_export.json` à la racine.

`python pipeline_graph.py`
*(Laissez le pipeline tourner. Il affichera des logs à chaque fois qu'un nouveau micro-batch est traité).*

### Étape 3 : Lancer le Dashboard
Pour que la page HTML puisse lire le fichier `graph_export.json` sans être bloquée par les politiques de sécurité des navigateurs (CORS), il est fortement recommandé d'utiliser un petit serveur web local.

Dans le troisième terminal, lancez :
`python -m http.server 8000`

Ensuite, ouvrez votre navigateur et accédez à l'URL suivante :
**http://localhost:8000**

Vous verrez le graphe se construire et se mettre à jour en temps réel au fur et à mesure que le simulateur génère des actions !

## Arrêter le Projet

Pour stopper l'application, retournez sur vos terminaux et utilisez le raccourci `Ctrl + C` dans chacun d'eux pour arrêter les scripts Python, le pipeline Spark et le serveur web. Vous pouvez ensuite supprimer le dossier `streaming_input` et le fichier `graph_export.json` pour remettre l'environnement à zéro.
