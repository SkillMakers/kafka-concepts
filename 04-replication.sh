#!/bin/bash
# Scenario 4 (lance depuis la machine hote : il arrete et redemarre un broker) :
# replication, leader, ISR, acks=all et min.insync.replicas.
# On arrete le broker leader de la partition : un autre prend le relais, sans perte.
export MSYS_NO_PATHCONV=1
k() { docker exec kafka-1 bash -c ". /tmp/commun.sh; $1"; }
decrire() { echo '$ kafka-topics.sh --describe --topic factures'; k '$K/kafka-topics.sh --bootstrap-server $B --describe --topic factures' | grep 'Partition:' | sed 's/Topic: factures\t//; s/\tElr:.*//; s/\t/  /g'; }
produire() { # acks message : le message passe par une variable d'environnement (pas de probleme d'apostrophe)
  local sortie
  sortie=$(docker exec -e MSG="$2" -e ACKS="$1" kafka-1 bash -c '. /tmp/commun.sh; echo "$MSG" | $K/kafka-console-producer.sh --bootstrap-server $B --topic factures \
     --command-property acks=$ACKS --command-property request.timeout.ms=5000 --command-property delivery.timeout.ms=10000 2>&1')
  if echo "$sortie" | grep -q -E 'Exception|ERROR'; then
    echo "$sortie" | grep -o -E 'NotEnoughReplicas[A-Za-z]*|Messages are rejected since there are fewer in-sync replicas than required\.|[A-Za-z]+Exception' | sort -u
    echo "=> REFUSE : « $2 »"
  else
    echo "=> ACCEPTE : « $2 »"
  fi
}
attendre_isr() { # liste attendue, 60 s max
  for _ in $(seq 60); do decrire | grep -q "Isr: $1\$" && return 0; sleep 1; done; echo "ECHEC : ISR $1 non atteint"; exit 1; }
k '. /tmp/commun.sh; existe factures && $K/kafka-topics.sh --bootstrap-server $B --delete --topic factures >/dev/null 2>&1; attendre 60 eval "! existe factures"' || { echo "ECHEC : suppression"; exit 1; }
echo '$ kafka-topics.sh --create --topic factures --replica-assignment 3:1:2 --config min.insync.replicas=2'
k '$K/kafka-topics.sh --bootstrap-server $B --create --topic factures --replica-assignment 3:1:2 --config min.insync.replicas=2' || exit 1
attendre_isr 3,1,2 >/dev/null; decrire
echo; echo '$ docker stop kafka-3      (le leader de la partition)'; docker stop kafka-3 >/dev/null
attendre_isr 1,2 >/dev/null; decrire
echo; echo '--- envoi avec acks=all, min.insync.replicas=2 (2 replicas a jour sur 3)'; produire all 'facture 1'
echo; echo '$ kafka-configs.sh --alter --entity-type topics --entity-name factures --add-config min.insync.replicas=3'
k '$K/kafka-configs.sh --bootstrap-server $B --alter --entity-type topics --entity-name factures --add-config min.insync.replicas=3' | grep -v '^$'
echo; echo '--- envoi avec acks=all, min.insync.replicas=3 (il n en reste que 2)'; produire all 'facture 2'
echo; echo '--- envoi avec acks=1 (seul le leader confirme)'; produire 1 'facture 3'
echo; echo '$ docker start kafka-3'; docker start kafka-3 >/dev/null
attendre_isr 1,2,3 >/dev/null; decrire
echo; echo '--- contenu du topic'
k 'ATTENTE=8000 lire factures verif'
