package fr.esgi.kafka.tempo.topology;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.esgi.kafka.tempo.Topics;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * TMP-2 - Skip rate par titre -> Topics.SKIP_RATE
 *
 * Fenetres tumbling de 10 min, par track_id : skips / (skips + completes).
 * Flop si skip rate > 70 % ET au moins 50 ecoutes (en dessous le ratio n'est
 * pas significatif : 3 ecoutes dont 3 skips n'est PAS un flop).
 */
public final class SkipRateTopology {

    /** Taille de fenetre imposee par le README. */
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(10);

    /**
     * Le flux contient des retardataires jusqu'a 180 min. On garde la fenetre
     * ouverte aussi longtemps : TMP-1 exige qu'aucun message valide ne soit
     * perdu, et un retardataire est un message valide.
     */
    private static final Duration GRACE = Duration.ofHours(3);

    private static final double FLOP_THRESHOLD = 0.70;
    private static final int MIN_LISTENS_FOR_FLOP = 50;

    private SkipRateTopology() {
    }

    /** Deux compteurs dans un seul aggregate, plutot que deux flux a joindre. */
    public record Counters(
            @JsonProperty("skips") int skips,
            @JsonProperty("completes") int completes) {

        static Counters empty() {
            return new Counters(0, 0);
        }

        Counters add(ListeningEvent e) {
            return "SKIP".equals(e.eventType())
                    ? new Counters(skips + 1, completes)
                    : new Counters(skips, completes + 1);
        }

        int total() {
            return skips + completes;
        }
    }

    /** Valeur produite dans <grp>.tempo.skiprate. */
    public record SkipRate(
            @JsonProperty("track_id") String trackId,
            @JsonProperty("window_start") String windowStart,
            @JsonProperty("window_end") String windowEnd,
            @JsonProperty("skips") int skips,
            @JsonProperty("completes") int completes,
            @JsonProperty("total") int total,
            @JsonProperty("skip_rate") double skipRate,
            @JsonProperty("flop") boolean flop) {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {
        validEvents
                // Un skip rate ne compare que les skips aux completes : les START
                // ne sont ni l'un ni l'autre, ils fausseraient le denominateur.
                .filter((key, e) -> "SKIP".equals(e.eventType()) || "COMPLETE".equals(e.eventType()))
                // La cle entrante est user_id ; on agrege par titre, donc on
                // rekey sur track_id. Kafka Streams repartitionne tout seul
                // derriere, pour que toutes les ecoutes d'un meme titre
                // atterrissent sur la meme instance.
                .selectKey((key, e) -> e.trackId())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE))
                .aggregate(
                        Counters::empty,
                        (trackId, e, counters) -> counters.add(e),
                        Materialized.with(Serdes.String(), JsonSerdes.of(Counters.class)))
                .toStream()
                .map((windowedKey, counters) -> KeyValue.pair(
                        windowedKey.key(),
                        toSkipRate(windowedKey.key(), windowedKey.window().startTime().toString(),
                                windowedKey.window().endTime().toString(), counters)))
                .mapValues(JsonSerdes::toJson)
                .to(Topics.SKIP_RATE, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static SkipRate toSkipRate(String trackId, String start, String end, Counters c) {
        int total = c.total();
        double rate = total == 0 ? 0.0 : (double) c.skips() / total;
        boolean flop = rate > FLOP_THRESHOLD && total >= MIN_LISTENS_FOR_FLOP;
        return new SkipRate(trackId, start, end, c.skips(), c.completes(), total, rate, flop);
    }
}
