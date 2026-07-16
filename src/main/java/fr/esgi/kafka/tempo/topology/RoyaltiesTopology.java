package fr.esgi.kafka.tempo.topology;

import fr.esgi.kafka.tempo.Topics;
import fr.esgi.kafka.tempo.TopologyProducer;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashSet;
import java.util.Set;

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

    /** Le "post-it" garde par artiste : le total gagne + la memoire des event_id deja comptes. */
    private record RoyaltyTotal(double total, Set<String> seenEventIds) {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {

        // SECTION A : ne garder que les ecoutes qui rapportent de l'argent.
        // validEvents est deja fourni "ouvert" (ListeningEvent) par TMP-1,
        // pas besoin de re-parser du texte ici.
        // Regle : il faut COMPLETE ET au moins 30 000 ms ecoutees.
        KStream<String, ListeningEvent> eligibleEvents = validEvents.filter(
                (key, event) -> "COMPLETE".equals(event.eventType())
                        && event.msPlayed() >= TopologyProducer.MIN_MS_FOR_ROYALTY);

        // SECTION B : re-etiqueter chaque ecoute par l'artiste (au lieu de
        // user_id), puis rassembler les ecoutes qui ont la meme etiquette.
        KGroupedStream<String, ListeningEvent> groupedByArtist = eligibleEvents
                .selectKey((key, event) -> event.artist())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)));

        // SECTION C/D : le tableau au mur qui se met a jour, avec protection
        // anti-doublon (un event_id deja compte ne paie pas une 2e fois).
        KTable<String, RoyaltyTotal> royaltiesByArtist = groupedByArtist.aggregate(
                () -> new RoyaltyTotal(0.0, new HashSet<>()),
                (artist, event, current) -> {
                    if (current.seenEventIds().contains(event.eventId())) {
                        return current;
                    }
                    current.seenEventIds().add(event.eventId());
                    double nouveauTotal = current.total() + TopologyProducer.ROYALTY_PER_STREAM;
                    return new RoyaltyTotal(nouveauTotal, current.seenEventIds());
                },
                Materialized.with(Serdes.String(), JsonSerdes.of(RoyaltyTotal.class)));

        // SECTION E : publier le tableau en continu vers le topic de sortie.
        royaltiesByArtist.toStream()
                .to(Topics.ROYALTIES, Produced.with(Serdes.String(), JsonSerdes.of(RoyaltyTotal.class)));
    }
}
