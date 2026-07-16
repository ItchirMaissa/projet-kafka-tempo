package fr.esgi.kafka.tempo.topology;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.esgi.kafka.tempo.Topics;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import fr.esgi.kafka.tempo.model.Track;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * TMP-3 - Top titres par pays -> Topics.TOP_BY_COUNTRY
 *
 * Compte les ecoutes par (country, track_id) sur fenetres hopping
 * 15 min / avance 5 min, START exclu, puis enrichit avec title et artist
 * depuis le catalogue tempo.tracks.
 */
public final class TopByCountryTopology {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(15);
    private static final Duration ADVANCE_BY = Duration.ofMinutes(5);

    /** Meme raisonnement que TMP-2 : retards annonces jusqu'a 180 min. */
    private static final Duration GRACE = Duration.ofHours(3);

    /** Separateur de la cle composite pays|titre. */
    private static final String KEY_SEPARATOR = "|";

    private TopByCountryTopology() {
    }

    /** Comptage brut, avant enrichissement. */
    private record CountedTrack(String country, String trackId, String windowStart,
                                String windowEnd, long listens) {
    }

    /** Valeur produite dans <grp>.tempo.top.by-country. */
    public record TopEntry(
            @JsonProperty("country") String country,
            @JsonProperty("track_id") String trackId,
            @JsonProperty("title") String title,
            @JsonProperty("artist") String artist,
            @JsonProperty("genre") String genre,
            @JsonProperty("window_start") String windowStart,
            @JsonProperty("window_end") String windowEnd,
            @JsonProperty("listens") long listens,
            @JsonProperty("enriched") boolean enriched) {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {

        // GlobalKTable plutot que KTable, pour deux raisons a defendre a l'oral :
        //  1. co-partitionnement : une jointure KStream-KTable exige que les deux
        //     cotes aient le meme nombre de partitions et la meme cle.
        //     tempo.listening.events a 6 partitions, tempo.tracks en a 3 :
        //     la jointure KTable echouerait sans repartitionner le catalogue.
        //  2. une GlobalKTable est repliquee entierement sur chaque instance,
        //     donc la jointure est un lookup local, et elle autorise une cle de
        //     jointure quelconque (ici track_id, extrait de la cle composite)
        //     au lieu d'imposer la cle du flux.
        // Le catalogue est petit (~2000 titres) et compacte : le cout memoire
        // de la replication est negligeable.
        GlobalKTable<String, Track> tracks = builder.globalTable(
                Topics.TRACKS,
                Consumed.with(Serdes.String(), JsonSerdes.of(Track.class)));

        KStream<String, CountedTrack> counts = validEvents
                // START exclu : on compte des ecoutes, pas des lancements.
                .filter((key, e) -> !"START".equals(e.eventType()))
                // Cle composite : on veut un compteur par couple pays + titre.
                .selectKey((key, e) -> e.country() + KEY_SEPARATOR + e.trackId())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)))
                // Hopping : fenetre de 15 min qui redemarre toutes les 5 min, donc
                // un top rafraichi toutes les 5 min tout en gardant une vue lissee
                // sur 15 min. Une ecoute compte dans 3 fenetres a la fois.
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE).advanceBy(ADVANCE_BY))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((windowedKey, listens) -> {
                    String[] parts = windowedKey.key().split("\\" + KEY_SEPARATOR, 2);
                    return KeyValue.pair(windowedKey.key(), new CountedTrack(
                            parts[0],
                            parts[1],
                            windowedKey.window().startTime().toString(),
                            windowedKey.window().endTime().toString(),
                            listens));
                });

        // Jointure placee APRES l'agregation : un lookup par resultat de fenetre
        // emis, et non un par evenement. Le titre et l'artiste ne servent qu'a
        // l'affichage, ils n'entrent pas dans le comptage - les porter dans
        // l'agregat ne ferait que grossir le state store.
        //
        // leftJoin et non join : un track_id absent du catalogue doit ressortir
        // avec son compteur, non enrichi. Un join (inner) le ferait disparaitre
        // silencieusement du top, ce qui serait une perte de donnee.
        counts.leftJoin(
                        tracks,
                        (key, counted) -> counted.trackId(),
                        TopByCountryTopology::enrich)
                .mapValues(JsonSerdes::toJson)
                .to(Topics.TOP_BY_COUNTRY, Produced.with(Serdes.String(), Serdes.String()));
    }

    /** track peut etre null : titre hors catalogue, ou tombstone du topic compacte. */
    private static TopEntry enrich(CountedTrack c, Track track) {
        return new TopEntry(
                c.country(),
                c.trackId(),
                track == null ? null : track.title(),
                track == null ? null : track.artist(),
                track == null ? null : track.genre(),
                c.windowStart(),
                c.windowEnd(),
                c.listens(),
                track != null);
    }
}
