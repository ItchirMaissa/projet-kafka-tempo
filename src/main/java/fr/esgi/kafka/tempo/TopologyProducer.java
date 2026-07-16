package fr.esgi.kafka.tempo;

import fr.esgi.kafka.tempo.common.EventTimestampExtractor;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.common.Validation;
import fr.esgi.kafka.tempo.model.DlqMessage;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import fr.esgi.kafka.tempo.topology.FraudTopology;
import fr.esgi.kafka.tempo.topology.RoyaltiesTopology;
import fr.esgi.kafka.tempo.topology.SkipRateTopology;
import fr.esgi.kafka.tempo.topology.TopByCountryTopology;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

/**
 * Topologie du projet TEMPO.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 *
 * Cette classe ne fait que l'ingestion (TMP-1) puis distribue le flux valide
 * aux quatre topologies metier, une par ticket. Ajoutez votre code dans VOTRE
 * classe du package topology, pas ici : c'est ce qui evite les conflits de
 * merge entre nous trois.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Une ecoute complete (>= 30 s) rapporte 0.003 EUR a l'artiste. */
    public static final double ROYALTY_PER_STREAM = 0.003;
    public static final int MIN_MS_FOR_ROYALTY = 30000;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Les fenetres doivent porter sur l'heure de l'evenement, pas sur son
        // heure d'arrivee : le flux contient des retardataires (30 a 180 min).
        KStream<String, String> rawEvents = builder.stream(
                Topics.LISTENING_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new EventTimestampExtractor()));

        // TMP-1 - Ingestion fiable : valider chaque message, router les
        // invalides vers la DLQ avec le message original et la raison.
        KStream<String, Validation.Result> validated =
                rawEvents.mapValues((key, value) -> Validation.validate(value));

        Map<String, KStream<String, Validation.Result>> branches = validated
                .split(Named.as("branch-"))
                .branch((key, value) -> value.isValid(), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        branches.get("branch-invalid")
                .mapValues(v -> JsonSerdes.toJson(new DlqMessage(v.raw(), v.reason())))
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, ListeningEvent> validEvents = branches.get("branch-valid")
                .mapValues(Validation.Result::event);

        SkipRateTopology.build(builder, validEvents);        // TMP-2
        TopByCountryTopology.build(builder, validEvents);    // TMP-3
        RoyaltiesTopology.build(builder, validEvents);       // TMP-4
        FraudTopology.build(builder, validEvents);           // TMP-5

        return builder.build();
    }
}
