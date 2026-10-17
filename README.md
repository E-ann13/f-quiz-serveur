# Serveur F-QUIZ (multijoueur en ligne)

Petit serveur Node.js qui relie les joueurs d’un salon (2 pour les duels, jusqu’à 8 pour le Grand Prix) identifié par un code de 5 caractères.
Il relaie les messages de jeu sans jamais connaître les avions secrets : chaque appareil répond lui-même
aux questions posées sur son propre avion.

## Lancer en local
    npm install
    npm start          # écoute sur le port 8787 (ou la variable PORT)
Dans le jeu : Multijoueur → Contact radar → En ligne, adresse `ws://localhost:8787`.

## Mettre en ligne
N'importe quel hébergeur qui fait tourner Node.js et accepte les WebSockets convient.
- Commande de démarrage : `npm start` ; le port est lu dans la variable `PORT`.
- Vérification : ouvrir `https://ADRESSE/health` doit répondre `{"ok":true,...}`.
- Dans le jeu, l'adresse à saisir commence par `wss://` (et non `https://`).
- Une fois l'adresse connue, la mettre dans `FQ_NET_DEFAULT_URL` dans f-quiz.html pour que les joueurs n'aient rien à saisir.

## Fonctionnement
- `create` → crée un salon et renvoie le code ; `join` → rejoint avec le code (8 joueurs max).
- `msg` → message relayé à l'adversaire, numéroté et gardé (500 derniers) pour les reprises.
- Coupure réseau : le jeu se reconnecte seul et récupère les messages manqués (place gardée 3 min).
- Un salon inactif disparaît au bout de 2 h. Limite anti-abus : 120 messages / 10 s, 64 Ko par message.
