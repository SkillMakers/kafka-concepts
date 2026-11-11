import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Consommateur de demonstration qui met 200 ms a traiter chaque message (un envoi d'email, par exemple).
 *
 * <pre>javac --release 17 -cp kafka-clients-4.4.0.jar Facteur.java, puis : java -cp ".:/opt/kafka/libs/*" Facteur classique|partage|partage-defaut &lt;groupe&gt; &lt;nom&gt; &lt;secondes&gt;</pre>
 *
 * classique : KafkaConsumer dans un groupe de consommateurs (une partition = un seul consommateur).
 * partage   : KafkaShareConsumer dans un share group (Kafka 4.2+) : plusieurs consommateurs sur la meme partition.
 * partage-defaut : idem, avec le mode d'acquisition par defaut (batch_optimized).
 */
public class Facteur {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 4 || !List.of("classique", "partage", "partage-defaut").contains(args[0])) {
            System.err.println("Usage : Facteur classique|partage|partage-defaut <groupe> <nom> <secondes>");
            System.exit(2);
        }
        String mode = args[0], groupe = args[1], nom = args[2];
        Instant fin = Instant.now().plusSeconds(Long.parseLong(args[3]));

        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092");
        p.put("group.id", groupe);
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        // Petits lots : sinon le premier consommateur arrive a prendre tout le retard d'un coup.
        p.put("max.poll.records", "5");

        if (mode.startsWith("partage")) {
            // Par defaut (batch_optimized), un consommateur peut acquerir tout un lot bien au-dela de max.poll.records.
            // record_limit (KIP-1206, Kafka 4.2) : il n'acquiert pas plus de max.poll.records messages a la fois.
            if (mode.equals("partage")) {
                p.put("share.acquire.mode", "record_limit");
            }
            try (KafkaShareConsumer<String, String> c = new KafkaShareConsumer<>(p)) {
                c.subscribe(List.of("emails"));
                while (Instant.now().isBefore(fin)) {
                    traiter(nom, c.poll(Duration.ofMillis(500)));
                }
            }
        } else {
            p.put("group.protocol", "consumer");
            p.put("auto.offset.reset", "earliest");
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                c.subscribe(List.of("emails"));
                while (Instant.now().isBefore(fin)) {
                    traiter(nom, c.poll(Duration.ofMillis(500)));
                }
            }
        }
    }

    private static void traiter(String nom, ConsumerRecords<String, String> lot) throws InterruptedException {
        for (var r : lot) {
            Thread.sleep(200); // le « travail » : 200 ms par message
            // L'heure en millisecondes en tete de ligne sert au script a mesurer la duree totale.
            System.out.printf("%d %s traite %s (partition %d, offset %d)%n",
                    System.currentTimeMillis(), nom, r.value(), r.partition(), r.offset());
        }
    }
}
