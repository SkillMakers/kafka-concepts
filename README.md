# Apache Kafka : les concepts, mesurés sur un vrai cluster

Code de la vidéo « Apache Kafka : c'est quoi ? Le cours complet en français » (ItSkillsMaker).
Un cluster **Kafka 4.4.0** de trois nœuds en mode **KRaft** (sans ZooKeeper), dans Docker, et un scénario par concept.
Chaque sortie montrée dans la vidéo est dans `preuves/`.

## Lancer le cluster

```bash
docker compose up -d          # 3 conteneurs : kafka-1, kafka-2, kafka-3 (~460 Mo d'image, 512 Mo de mémoire chacun)
docker compose down           # tout arrêter
```

## Les scénarios

Les scripts tournent **dans** le conteneur `kafka-1`, sauf deux, qui se lancent depuis la machine :
`04-replication.sh` (il arrête et redémarre un broker) et `06-defauts.sh` (il a besoin de `python` sur la machine).
Sous Git Bash (Windows), préfixer chaque commande `docker` par `MSYS_NO_PATHCONV=1`.

```bash
docker cp commun.sh kafka-1:/tmp/                                               # une fois
docker cp 01-journal.sh kafka-1:/tmp/ && docker exec kafka-1 bash /tmp/01-journal.sh   # idem pour 02, 03, 07
bash 04-replication.sh                                                          # depuis la machine
bash 06-defauts.sh                                                              # depuis la machine (python requis)
```

Chaque script vérifie ce qu'il affiche : topic mal créé, groupe qui ne se stabilise pas, commit passé trop tôt…
Il s'arrête alors avec « ECHEC » au lieu de montrer un résultat faux. `03-doublons.sh` dépend du timing : si le commit
automatique passe avant le `kill -9`, il le dit et demande de le relancer.

| Script | Ce qu'il montre |
|---|---|
| `01-journal.sh` | Un topic est un journal : deux groupes lisent les mêmes 6 messages, relire ne supprime rien, on rejoue avec `--reset-offsets`. L'ordre n'est garanti que dans une partition |
| `02-groupe.sh` + `Chrono.java` | 4 consommateurs pour 3 partitions : le 4e ne reçoit rien. Consommateur tué (`kill -9`) : sa partition attend ~46 s ; arrêt propre : ~3 s (mesure toutes les 100 ms) |
| `03-doublons.sh` | « Au moins une fois » : 10 messages traités, consommateur tué avant le commit automatique, les 10 sont retraités au redémarrage |
| `04-replication.sh` | Leader, ISR, panne du leader ; `acks=all` + `min.insync.replicas` : `NotEnoughReplicasException` |
| `05-share.sh` + `Facteur.java` | Share groups (Kafka 4.2+) : 3 consommateurs sur 1 partition. Groupe classique 60/0/0, share group 20/20/20 avec `share.acquire.mode=record_limit` |
| `06-defauts.sh` | Les valeurs par défaut citées dans la vidéo, lues sur le cluster et dans `kafka-clients` |
| `07-ajout-partitions.sh` | Passer de 3 à 4 partitions déplace des clés (3 clients sur 6) |

`Facteur.java` (scénario 05) et `Chrono.java` (scénario 02) se compilent avec le JAR client, que l'image contient déjà
(elle n'a pas de compilateur Java) :

```bash
docker cp kafka-1:/opt/kafka/libs/kafka-clients-4.4.0.jar .
javac --release 17 -cp kafka-clients-4.4.0.jar Facteur.java Chrono.java
docker cp Facteur.class kafka-1:/tmp/ && docker cp Chrono.class kafka-1:/tmp/
docker cp 05-share.sh kafka-1:/tmp/ && docker exec kafka-1 bash /tmp/05-share.sh
```
