package fr.esgi.kafka.tempo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Valeur produite dans <grp>.tempo.dlq : le message original tel qu'il est
 * arrive, plus la raison du rejet.
 *
 * Un record plutot qu'un Map.of : Map.of leve une NullPointerException des
 * qu'une valeur est nulle, ce qui ferait mourir l'application en boucle sur un
 * message a valeur nulle - exactement le poison pill que TMP-1 doit empecher.
 */
public record DlqMessage(
        @JsonProperty("original") String original,
        @JsonProperty("reason") String reason) {
}
