package fr.esgi.kafka.tempo.common;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

/**
 * Fait porter les fenetres sur l'heure de l'evenement (champ "timestamp" du
 * message) et non sur son heure d'arrivee dans Kafka.
 *
 * Le flux contient des evenements en retard de 30 a 180 min. Par defaut Kafka
 * Streams les rangerait dans la fenetre de leur heure d'arrivee : une ecoute
 * de 14h00 arrivant a 15h30 serait comptee a 15h30, ce qui fausse le skip rate
 * (TMP-2) et le top par pays (TMP-3).
 *
 * Repli sur l'horodatage du broker quand le champ est absent ou illisible :
 * ces messages partent en DLQ de toute facon, et un extracteur ne doit jamais
 * renvoyer de valeur negative sous peine de faire lever une exception a Kafka
 * Streams.
 */
public class EventTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value != null) {
            ListeningEvent event = JsonSerdes.parseOrNull(value.toString(), ListeningEvent.class);
            if (event != null && event.timestamp() != null) {
                try {
                    return Instant.parse(event.timestamp()).toEpochMilli();
                } catch (Exception ignored) {
                    // timestamp illisible ("hier a 15h") : on retombe sur le repli
                }
            }
        }
        if (record.timestamp() >= 0) {
            return record.timestamp();
        }
        return partitionTime >= 0 ? partitionTime : 0L;
    }
}
