package fr.esgi.kafka.tempo.topology;

import fr.esgi.kafka.tempo.Topics;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.eclipse.microprofile.config.ConfigProvider;

import java.time.Duration;
import java.time.Instant;

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

    // Taille de la fenetre, modifiable SANS recompiler (variable d'env
    // FRAUD_WINDOW_MINUTES). Le sujet demande 1h en "production", mais pour
    // la demo/le test on la met a 5 min par defaut : le generateur lance une
    // ferme toutes les ~5 min, on veut la voir passer vite.
    private static final int WINDOW_MINUTES = ConfigProvider.getConfig()
            .getOptionalValue("FRAUD_WINDOW_MINUTES", Integer.class)
            .orElse(5);

    // Au-dela de ce nombre de COMPLETE, pour le meme (user, track), dans la
    // meme fenetre, on considere que c'est une ferme a streams.
    private static final long FRAUD_THRESHOLD = 20;

    /** Le message d'alerte publie dans le topic de sortie. */
    private record FraudAlert(String userId, String trackId, long count,
                               String windowStart, String windowEnd) {
    }

    public static void build(StreamsBuilder builder, KStream<String, ListeningEvent> validEvents) {

        // SECTION A : ne garder que les ecoutes COMPLETE.
        // Une ferme rejoue le meme titre en boucle JUSQU'AU BOUT pour gonfler
        // les royalties : les START et les SKIP ne comptent pas ici.
        KStream<String, ListeningEvent> completedEvents = validEvents.filter(
                (key, event) -> "COMPLETE".equals(event.eventType()));

        // SECTION B : cle composite (user_id + track_id).
        // On regroupe par COUPLE utilisateur+titre, pas juste par
        // utilisateur : sinon on confondrait "il ecoute plein de titres
        // differents" (normal) avec "il boucle sur le meme titre" (fraude).
        KGroupedStream<String, ListeningEvent> groupedByUserAndTrack = completedEvents
                .selectKey((key, event) -> event.userId() + "|" + event.trackId())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)));

        // SECTION C : fenetre tumbling (des tranches de temps qui ne se
        // chevauchent jamais, ex: 14h00-14h05 puis 14h05-14h10...), et on
        // compte les COMPLETE de chaque couple (user, track) dans chaque
        // tranche. Une grace period courte tolere les petits retardataires
        // sans retarder l'alerte trop longtemps (a justifier a l'oral : le
        // flux a des retardataires jusqu'a 180 min, ici on privilegie une
        // alerte rapide a l'exhaustivite totale).
        KTable<Windowed<String>, Long> countsByWindow = groupedByUserAndTrack
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofMinutes(WINDOW_MINUTES), Duration.ofMinutes(1)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // SECTION D : ne garder que les couples qui DEPASSENT le seuil.
        // C'est ce qui garantit le silence en regime normal demande par le
        // sujet : un auditeur ordinaire ne rejoue jamais 20 fois le meme
        // titre en 5 minutes.
        KTable<Windowed<String>, Long> suspiciousWindows = countsByWindow
                .filter((windowedKey, count) -> count > FRAUD_THRESHOLD);

        // SECTION E : reconstruire une alerte lisible (utilisateur, titre,
        // nombre d'ecoutes, debut/fin de fenetre) et la publier.
        suspiciousWindows.toStream()
                .map((windowedKey, count) -> {
                    String[] parts = windowedKey.key().split("\\|", 2);
                    String userId = parts[0];
                    String trackId = parts.length > 1 ? parts[1] : "?";
                    FraudAlert alert = new FraudAlert(
                            userId, trackId, count,
                            Instant.ofEpochMilli(windowedKey.window().start()).toString(),
                            Instant.ofEpochMilli(windowedKey.window().end()).toString());
                    return KeyValue.pair(userId + "|" + trackId, JsonSerdes.toJson(alert));
                })
                .to(Topics.ALERTS_FRAUD, Produced.with(Serdes.String(), Serdes.String()));
    }
}
