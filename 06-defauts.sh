#!/bin/bash
# Valeurs par defaut citees dans la video, lues sur le cluster 4.4.0 lui-meme (broker) et dans kafka-clients 4.4.0 (clients).
export MSYS_NO_PATHCONV=1
echo "Broker (kafka-configs.sh --entity-type brokers --entity-name 1 --describe --all) :"
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type brokers --entity-name 1 --describe --all 2>/dev/null \
 | grep -E "^\s*(group\.consumer\.session\.timeout\.ms|group\.consumer\.heartbeat\.interval\.ms|log\.retention\.hours|min\.insync\.replicas|group\.share\.record\.lock\.duration\.ms|group\.share\.delivery\.count\.limit|num\.partitions|default\.replication\.factor)=" | sed 's/ sensitive=.*//'
echo; echo "Clients (documentation embarquee dans kafka-clients-4.4.0.jar) :"
for c in org.apache.kafka.clients.producer.ProducerConfig org.apache.kafka.clients.consumer.ConsumerConfig; do
  docker exec kafka-1 java -cp "/opt/kafka/libs/*" $c 2>/dev/null | python -c "
import sys,re,html
t=sys.stdin.read()
for name in ['acks','enable.idempotence','enable.auto.commit','auto.commit.interval.ms','max.poll.records','auto.offset.reset','share.acquire.mode','group.protocol','session.timeout.ms']:
    m=re.search(r'id=\"[^\"]*_'+re.escape(name)+r'\".*?Default:</th><td>(.*?)</td>',t,re.S)
    if m: print('  '+sys.argv[1], name,'=',html.unescape(re.sub('<[^>]+>','',m.group(1))).strip())
" ${c##*.}
done
