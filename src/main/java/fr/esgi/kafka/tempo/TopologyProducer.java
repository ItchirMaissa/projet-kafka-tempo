package fr.esgi.kafka.tempo;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Map;
import java.util.Set;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Construisez ici la topologie du projet TEMPO.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Une ecoute complete (>= 30 s) rapporte 0.003 EUR a l'artiste. */
    public static final double ROYALTY_PER_STREAM = 0.003;
    public static final int MIN_MS_FOR_ROYALTY = 30000;
    private static final Set<String> VALID_EVENT_TYPES = Set.of("START", "SKIP", "COMPLETE");
    private static final Set<String> VALID_COUNTRIES   = Set.of("FR", "US", "DE", "BR", "JP", "GB");
    // regrouper le messae brute + évènement parsé + raison d'échec (null si valide)
    private record ValidationResult(String raw, ListeningEvent event, String reason) {
    boolean isValid() { return event != null; }
}

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawEvents = builder.stream(
                Topics.LISTENING_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        //rawEvents.peek((key, value) -> Log.infof("[tempo] %s -> %s", key, value));
        //valider chaque messaeg
        KStream<String, ValidationResult> validated =
            rawEvents.mapValues((k, v) -> validate(v));
        // Router : invalides -> DLQ, valides -> suite
        Map<String, KStream<String, ValidationResult>> branches = validated
            .split(Named.as("branch-"))
            .branch((k, v) -> v.isValid(), Branched.as("valid"))
            .defaultBranch(Branched.as("invalid"));

        // DLQ : message original + raison
        branches.get("branch-invalid")
            .mapValues(v -> JsonSerdes.toJson(Map.of("original", v.raw(), "reason", v.reason())))
            .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));
        // Flux valide (utilisé par TMP-2, 3, 4, 5)
        KStream<String, ListeningEvent> validEvents = branches.get("branch-valid")
            .mapValues(v -> v.event());
        // -----------------------------------------------------------------
        // TMP-1 - Ingestion fiable
        //   Parser (model.ListeningEvent), valider (champs requis, types,
        //   event_type/country dans les enums, ms_played >= 0, timestamp
        //   ISO-8601), router les invalides vers Topics.DLQ avec message
        //   original + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // TMP-2 - Skip rate par titre (tumbling 10 min)   -> Topics.SKIP_RATE
        // TMP-3 - Top titres par pays (hopping 15/5 min,
        //         jointure GlobalKTable tempo.tracks)     -> Topics.TOP_BY_COUNTRY
        // TMP-4 - Compteur de royalties par artiste       -> Topics.ROYALTIES
        // TMP-5 - Fraude "stream farm"                    -> Topics.ALERTS_FRAUD
        // TMP-6 (bonus) - Tests unitaires TopologyTestDriver (cf. README)

        return builder.build();
    }
    // vérifier les règles de validation d'un ListeningEvent, renvoyer ValidationResult
    private static ValidationResult validate(String raw) {
    ListeningEvent e = JsonSerdes.parseOrNull(raw, ListeningEvent.class);
    if (e == null)
        return new ValidationResult(raw, null, "JSON invalide ou tronque");
    if (e.eventId() == null || e.eventId().isBlank())
        return new ValidationResult(raw, null, "event_id manquant");
    if (e.userId() == null || e.userId().isBlank())
        return new ValidationResult(raw, null, "user_id manquant");
    if (e.trackId() == null || e.trackId().isBlank())
        return new ValidationResult(raw, null, "track_id manquant");
    if (e.artist() == null || e.artist().isBlank())
        return new ValidationResult(raw, null, "artist manquant");
    if (e.eventType() == null || !VALID_EVENT_TYPES.contains(e.eventType()))
        return new ValidationResult(raw, null, "event_type invalide: " + e.eventType());
    if (e.msPlayed() == null || e.msPlayed() < 0)
        return new ValidationResult(raw, null, "ms_played invalide: " + e.msPlayed());
    if (e.country() == null || !VALID_COUNTRIES.contains(e.country()))
        return new ValidationResult(raw, null, "country invalide: " + e.country());
    if (e.timestamp() == null)
        return new ValidationResult(raw, null, "timestamp manquant");
    try {
        java.time.Instant.parse(e.timestamp());
    } catch (Exception ex) {
        return new ValidationResult(raw, null, "timestamp non ISO-8601: " + e.timestamp());
    }
    return new ValidationResult(raw, e, null);
    }
}
