#!/bin/bash
# Scenario 5 : 3 consommateurs qui mettent 200 ms par message, sur un topic d'UNE seule partition, 60 messages.
# Groupe classique : un seul travaille. Share group (Kafka 4.2+, « Queues for Kafka ») : les trois se partagent le travail.
. /tmp/commun.sh
pkill -f Facteur 2>/dev/null; sleep 1
[ -f /tmp/Facteur.class ] || echec "Facteur.class absent de /tmp (voir le README)"
nouveau_topic emails 1
# Le share group demarre par defaut a la fin du topic (share.auto.offset.reset=latest) : on le fait partir du debut.
for g in notifications-partage notifications-defaut; do
  $K/kafka-configs.sh --bootstrap-server $B --alter --entity-type groups --entity-name $g --add-config share.auto.offset.reset=earliest >/dev/null 2>&1     || echec "share.auto.offset.reset=earliest refuse pour le groupe $g (il partirait de la fin du topic et ne lirait rien)"
done
seq 1 60 | sed 's/.*/email-&:email &/' | envoyer emails; echo "60 messages envoyes"
lancer() { # mode groupe
  for i in 1 2 3; do
    nohup java -cp "/tmp:/opt/kafka/libs/*" Facteur $1 $2 consommateur-$i 40 > /tmp/$1-$i.log 2>/dev/null &
  done
  T0=$(date +%s); wait; echo "termine en $(( $(date +%s) - T0 )) s (chaque consommateur tourne 40 s)"
  for i in 1 2 3; do printf "  consommateur-%s : %2s messages traites\n" $i $(grep -c traite /tmp/$1-$i.log); done
  cat /tmp/$1-[123].log | grep traite | sort -n | awk 'NR==1{d=$1} {f=$1} END {printf "  du 1er au dernier message traite : %.1f s", (f-d)/1000}'; echo
  echo "  les 6 premiers messages :"; cat /tmp/$1-[123].log | grep traite | sort -n | cut -d' ' -f2- | sed -n '1,6p' | sed 's/^/    /'
}
echo; echo '--- groupe CLASSIQUE « notifications » (KafkaConsumer)'; lancer classique notifications
echo; echo '--- SHARE GROUP « notifications-defaut » (KafkaShareConsumer, reglages par defaut : share.acquire.mode=batch_optimized)'; lancer partage-defaut notifications-defaut
echo; echo '--- SHARE GROUP « notifications-partage » (KafkaShareConsumer, share.acquire.mode=record_limit, max.poll.records=5)'; lancer partage notifications-partage
