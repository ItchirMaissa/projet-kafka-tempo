package fr.esgi.kafka.tempo.topology;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * TMP-4 - Compteur de royalties par artiste -> Topics.ROYALTIES
 *
 * Cumuler TopologyProducer.ROYALTY_PER_STREAM par ecoute eligible, par artist
 * (KTable, aggregate). Eligible = event_type COMPLETE ET ms_played >= 30000
 * (TopologyProducer.MIN_MS_FOR_ROYALTY). Un SKIP a 45 000 ms ne paie pas.
 *
 * Dedoublonnage : un meme event_id ne doit pas payer deux fois. Piste README :
 * un state store des event_id vus, avec une politique d'expiration, place
 * AVANT l'agregation.
 *
 * A savoir justifier a l'oral : la cle (artist), la strategie de
 * dedoublonnage et l'expiration du store.
 */
public final class RoyaltiesTopology {

    private RoyaltiesTopology() {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {
        // TODO TMP-4
    }
}
