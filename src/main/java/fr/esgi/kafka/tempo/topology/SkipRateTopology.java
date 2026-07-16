package fr.esgi.kafka.tempo.topology;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * TMP-2 - Skip rate par titre -> Topics.SKIP_RATE
 *
 * Fenetres tumbling de 10 min, par track_id : skips / (skips + completes).
 * Marquer flop si skip rate > 70 % ET au moins 50 ecoutes (en dessous le
 * ratio n'est pas significatif : 3 ecoutes dont 3 skips n'est PAS un flop).
 *
 * Piste README : un seul aggregate avec deux compteurs (skips, completes),
 * plutot que deux flux a joindre.
 *
 * A definir et a savoir justifier a l'oral : la cle d'agregation, la taille
 * de fenetre, et le grace period (les retardataires vont jusqu'a 180 min).
 */
public final class SkipRateTopology {

    private SkipRateTopology() {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {
        // TODO TMP-2
    }
}
