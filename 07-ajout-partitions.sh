#!/bin/bash
# Scenario 7 : la cle choisit la partition (hachage de la cle, modulo le nombre de partitions).
# Ajouter une partition change la partition de certaines cles : leur ordre n'est plus garanti pendant la transition.
. /tmp/commun.sh
nouveau_topic clients 3
for c in A B C D E F; do echo "client-$c:avant"; done | envoyer clients
echo '$ kafka-topics.sh --alter --topic clients --partitions 4'
$K/kafka-topics.sh --bootstrap-server $B --alter --topic clients --partitions 4 2>&1 | propre
for c in A B C D E F; do echo "client-$c:apres"; done | envoyer clients
ATTENTE=8000 lire clients verif > /tmp/clients.log
printf "%-10s %-26s %s\n" "cle" "avec 3 partitions" "avec 4 partitions"
for c in A B C D E F; do
  a=$(grep "client-$c.avant" /tmp/clients.log | sed 's/Partition:\([0-9]*\).*/\1/')
  b=$(grep "client-$c.apres" /tmp/clients.log | sed 's/Partition:\([0-9]*\).*/\1/')
  printf "%-10s %-26s %s%s\n" "client-$c" "partition $a" "partition $b" "$([ "$a" != "$b" ] && echo '   <- a change')"
done
