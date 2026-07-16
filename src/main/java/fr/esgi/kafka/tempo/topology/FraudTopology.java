package fr.esgi.kafka.tempo.topology;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * TMP-5 - Fraude "stream farm" -> Topics.ALERTS_FRAUD
 *
 * Alerte quand un couple (user_id, track_id) depasse 20 COMPLETE par heure
 * (tumbling 1 h). La taille de fenetre doit etre parametrable : la demo se
 * fait a 5 min, le generateur lance une ferme toutes les ~5 min
 * (~36 COMPLETE a 31 000 ms en 3 min).
 *
 * Silence attendu en regime normal.
 *
 * A savoir justifier a l'oral : la cle composite user+track et le choix
 * tumbling.
 */
public final class FraudTopology {

    private FraudTopology() {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {
        // TODO TMP-5
    }
}
