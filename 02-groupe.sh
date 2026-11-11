#!/bin/bash
# Scenario 2 : un groupe de consommateurs. 4 consommateurs pour 3 partitions : le 4e ne recoit rien.
# Puis on tue (kill -9) le consommateur de la partition 0 : Kafka ne la redonne qu'apres le delai de session.
# Enfin on en arrete un proprement (TERM) : il quitte le groupe, la reprise est bien plus rapide.
# Les durees sont mesurees par Chrono.java : il tue lui-meme le consommateur, puis interroge le groupe toutes les 100 ms.
. /tmp/commun.sh
pkill -f ConsoleConsumer 2>/dev/null; sleep 1
[ -f /tmp/Chrono.class ] || echec "Chrono.class absent de /tmp (voir le README)"
nouveau_topic paiements 3
for i in 1 2 3 4; do
  nohup $K/kafka-console-consumer.sh --bootstrap-server $B --topic paiements --group encaissement \
    --command-property group.protocol=consumer --command-property client.id=consommateur-$i > /tmp/conso-$i.log 2>&1 &
done
echo "4 consommateurs lances dans le groupe encaissement (protocole consumer, KIP-848)"
brut() { $K/kafka-consumer-groups.sh --bootstrap-server $B --describe --group encaissement --members --verbose 2>&1; }
# stable <n> : attend (90 s au plus) n membres, sans reequilibrage en cours, chacun avec son affectation cible.
est_stable() { S=$(brut); [ "$(echo "$S" | awk '$1=="encaissement"' | wc -l)" -eq "$1" ] \
  && ! echo "$S" | grep -q rebalancing && ! echo "$S" | awk '$1=="encaissement" && $7!=$9' | grep -q .; }
stable() { attendre 90 est_stable $1 || echec "le groupe ne s'est pas stabilise avec $1 membres"; }
montrer() { echo "$S" | awk '$1=="encaissement" {printf "  %-16s partitions : %s\n", $4, $7}' | sort; }
# La mesure elle-meme est faite par Chrono.java (une seule JVM, releve toutes les 100 ms : precision ~0,1 s).
mesurer() { # KILL|TERM
  java -cp "/tmp:/opt/kafka/libs/*" Chrono encaissement paiements 0 $1 2>/dev/null || echec "mesure impossible"
}

echo; echo '$ kafka-consumer-groups.sh --describe --group encaissement --members --verbose'
stable 4; montrer
echo; echo "On tue le consommateur qui lisait la partition 0 (kill -9)"
mesurer KILL
echo; echo '$ kafka-consumer-groups.sh --describe --group encaissement --members --verbose'
stable 3; montrer
echo; echo "Cette fois on arrete proprement celui qui lit la partition 0 (kill, signal TERM : il quitte le groupe)"
mesurer TERM
echo; echo '$ kafka-consumer-groups.sh --describe --group encaissement --members --verbose'
stable 2; montrer
pkill -f ConsoleConsumer
