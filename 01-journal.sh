#!/bin/bash
# Scenario 1 : Kafka est un journal, pas une file.
# Deux groupes lisent les memes messages ; lire ne supprime rien ; on peut rejouer.
. /tmp/commun.sh
nouveau_topic commandes 3
printf 'client-A:commande 1 creee\nclient-B:commande 2 creee\nclient-A:commande 1 payee\nclient-C:commande 3 creee\nclient-B:commande 2 payee\nclient-A:commande 1 expediee\n' | envoyer commandes
echo "6 messages envoyes, dans cet ordre :"
echo "  1 client-A commande 1 creee | 2 client-B commande 2 creee | 3 client-A commande 1 payee"
echo "  4 client-C commande 3 creee | 5 client-B commande 2 payee | 6 client-A commande 1 expediee"
echo; echo '--- groupe facturation, 1re lecture'; lire commandes facturation
echo; echo '--- groupe stock, 1re lecture'; lire commandes stock
echo; echo '--- groupe facturation, 2e lecture'; lire commandes facturation
echo; echo '$ kafka-consumer-groups.sh --group facturation --topic commandes --reset-offsets --to-earliest --execute'
$K/kafka-consumer-groups.sh --bootstrap-server $B --group facturation --topic commandes --reset-offsets --to-earliest --execute 2>&1 | propre
echo; echo '--- groupe facturation, apres remise a zero'; lire commandes facturation
