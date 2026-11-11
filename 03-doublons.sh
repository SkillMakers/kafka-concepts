#!/bin/bash
# Scenario 3 : « au moins une fois ». Le consommateur traite les messages, puis plante avant d'avoir enregistre son
# offset (commit automatique toutes les 5 s par defaut) : au redemarrage, il retraite les memes messages.
# Le resultat depend du timing (le commit peut passer avant le kill) : le script verifie l'offset avant de conclure.
. /tmp/commun.sh
pkill -f ConsoleConsumer 2>/dev/null; sleep 1
nouveau_topic expeditions 1
seq 1 10 | sed 's/.*/commande-&:expedier la commande &/' | envoyer expeditions
echo "10 messages envoyes"
echo; echo '--- le consommateur du groupe colis traite les 10 messages, puis on le tue (kill -9) aussitot'
$K/kafka-console-consumer.sh --bootstrap-server $B --topic expeditions --group colis --from-beginning \
  --command-property group.protocol=consumer --formatter-property print.offset=true > /tmp/avant.log 2>/dev/null &
PID=$!
dix_traites() { [ "$(grep -c Offset /tmp/avant.log)" -ge 10 ]; }
attendre 60 dix_traites || { kill -9 $PID 2>/dev/null; echec "le consommateur n'a pas recu les 10 messages en 60 s"; }
kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null
propre < /tmp/avant.log; grep -c Offset /tmp/avant.log | sed 's/^/messages traites avant le crash : /'
echo; echo '$ kafka-consumer-groups.sh --describe --group colis --offsets'
OFF=$($K/kafka-consumer-groups.sh --bootstrap-server $B --describe --group colis --offsets 2>&1 | propre)
echo "$OFF" | awk 'NF {print $2, $3, $4, $5, $6}'
COMMITE=$(echo "$OFF" | awk '$2=="expeditions" {print $4}')
if [ "$COMMITE" != "-" ]; then
  echo; echo "Le commit automatique est passe avant le kill (offset $COMMITE) : pas de doublon cette fois. Relancer le scenario."
  exit 2
fi
echo; echo '--- redemarrage du meme groupe colis (aucun offset enregistre : il repart du debut)'
T0=$(date +%s.%N)
ATTENTE=90000 lire expeditions colis --max-messages 10 > /tmp/apres.log; T1=$(date +%s.%N); cat /tmp/apres.log
N=$(grep -c Offset /tmp/apres.log)
[ "$N" -gt 0 ] || echec "aucun message relu en 90 s"
printf "(les messages sont arrives %.0f s apres le redemarrage : le consommateur tue occupait encore la partition)\n" $(echo "$T1 - $T0" | bc)
echo; echo "messages traites deux fois : $N"
