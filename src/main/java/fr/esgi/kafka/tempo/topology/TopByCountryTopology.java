package fr.esgi.kafka.tempo.topology;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * TMP-3 - Top titres par pays -> Topics.TOP_BY_COUNTRY
 *
 * Compter les ecoutes par (country, track_id) sur fenetres hopping
 * 15 min / avance 5 min. START exclu du comptage.
 * Enrichir avec title et artist via une GlobalKTable sur Topics.TRACKS
 * (d'ou le StreamsBuilder en parametre).
 *
 * Un track_id absent du catalogue ne doit pas faire crasher la jointure.
 *
 * A savoir justifier a l'oral : GlobalKTable vs KTable, et pourquoi une
 * fenetre hopping plutot que tumbling ici.
 */
public final class TopByCountryTopology {

    private TopByCountryTopology() {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {
        // TODO TMP-3
    }
}
