package fr.esgi.kafka.tempo.common;

import fr.esgi.kafka.tempo.model.ListeningEvent;

import java.time.Instant;
import java.util.Set;

/**
 * TMP-1 - Validation des evenements d'ecoute.
 * Regroupe le message brut, l'evenement parse et la raison d'echec
 * (null si valide).
 */
public final class Validation {

    private static final Set<String> VALID_EVENT_TYPES = Set.of("START", "SKIP", "COMPLETE");
    private static final Set<String> VALID_COUNTRIES = Set.of("FR", "US", "DE", "BR", "JP", "GB");

    private Validation() {
    }

    public record Result(String raw, ListeningEvent event, String reason) {
        public boolean isValid() {
            return event != null;
        }
    }

    /** Verifie les regles de validation d'un ListeningEvent. */
    public static Result validate(String raw) {
        ListeningEvent e = JsonSerdes.parseOrNull(raw, ListeningEvent.class);
        if (e == null) {
            return new Result(raw, null, "JSON invalide ou tronque");
        }
        if (e.eventId() == null || e.eventId().isBlank()) {
            return new Result(raw, null, "event_id manquant");
        }
        if (e.userId() == null || e.userId().isBlank()) {
            return new Result(raw, null, "user_id manquant");
        }
        if (e.trackId() == null || e.trackId().isBlank()) {
            return new Result(raw, null, "track_id manquant");
        }
        if (e.artist() == null || e.artist().isBlank()) {
            return new Result(raw, null, "artist manquant");
        }
        if (e.eventType() == null || !VALID_EVENT_TYPES.contains(e.eventType())) {
            return new Result(raw, null, "event_type invalide: " + e.eventType());
        }
        if (e.msPlayed() == null || e.msPlayed() < 0) {
            return new Result(raw, null, "ms_played invalide: " + e.msPlayed());
        }
        if (e.country() == null || !VALID_COUNTRIES.contains(e.country())) {
            return new Result(raw, null, "country invalide: " + e.country());
        }
        if (e.timestamp() == null) {
            return new Result(raw, null, "timestamp manquant");
        }
        try {
            Instant.parse(e.timestamp());
        } catch (Exception ex) {
            return new Result(raw, null, "timestamp non ISO-8601: " + e.timestamp());
        }
        return new Result(raw, e, null);
    }
}
