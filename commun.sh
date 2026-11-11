# Fonctions communes aux scenarios (executees DANS le conteneur kafka-1).
B=kafka-1:9092; K=/opt/kafka/bin
propre() { grep -v -E 'deprecated|KIP-848\) is production-ready|ERROR Error processing message|TimeoutException|^\s+at |^org\.apache|^Caused by|WARN' ; }
echec() { echo "ECHEC : $*" >&2; pkill -f 'Console(Share)?Consumer|Facteur' 2>/dev/null; exit 1; }
# attendre <secondes max> <commande...> : relance la commande jusqu'a ce qu'elle reussisse, ou echoue apres le delai.
attendre() { local fin=$(( $(date +%s) + $1 )); shift; until "$@"; do [ "$(date +%s)" -ge $fin ] && return 1; sleep 0.5; done; }
existe() { $K/kafka-topics.sh --bootstrap-server $B --list 2>/dev/null | grep -qx "$1"; }
nouveau_topic() { # nom partitions [options...]
  local n=$1 p=$2; shift 2
  if existe $n; then
    $K/kafka-topics.sh --bootstrap-server $B --delete --topic $n >/dev/null 2>&1
    attendre 60 eval "! existe $n" || echec "le topic $n n'a pas ete supprime"
  fi
  echo "\$ kafka-topics.sh --create --topic $n --partitions $p --replication-factor 3 $*"
  $K/kafka-topics.sh --bootstrap-server $B --create --topic $n --partitions $p --replication-factor 3 "$@" \
    || echec "creation du topic $n"
  # Sans cette verification, un topic mal cree serait recree en silence par le producteur
  # avec les valeurs du broker (1 partition, 1 replique) et le scenario montrerait autre chose.
  local vu=$($K/kafka-topics.sh --bootstrap-server $B --describe --topic $n 2>/dev/null | grep -c 'Partition:')
  [ "$vu" -eq "$p" ] || echec "le topic $n a $vu partition(s) au lieu de $p"
}
envoyer() { # topic, lignes cle:valeur sur l'entree standard
  $K/kafka-console-producer.sh --bootstrap-server $B --topic $1 --reader-property parse.key=true --reader-property key.separator=: 2>&1 | propre
}
lire() { # topic groupe [options...]
  local t=$1 g=$2; shift 2
  $K/kafka-console-consumer.sh --bootstrap-server $B --topic $t --group $g --from-beginning \
    --command-property group.protocol=consumer \
    --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true \
    --timeout-ms ${ATTENTE:-8000} "$@" 2>&1 | propre
}
