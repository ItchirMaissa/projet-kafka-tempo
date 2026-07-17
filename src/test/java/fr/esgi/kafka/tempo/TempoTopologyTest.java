package fr.esgi.kafka.tempo;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TempoTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> dlq;
    private TestOutputTopic<String, String> royalties;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(new TopologyProducer().buildTopology(), props);

        input = driver.createInputTopic(
                Topics.LISTENING_EVENTS, new StringSerializer(), new StringSerializer());
        dlq = driver.createOutputTopic(
                Topics.DLQ, new StringDeserializer(), new StringDeserializer());
        royalties = driver.createOutputTopic(
                Topics.ROYALTIES, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void messageInvalideDoitPartirEnDlq() {
        // event_type inconnu → invalide
        input.pipeInput("u-0001",
                "{\"event_id\":\"ev-001\",\"user_id\":\"u-0001\",\"track_id\":\"trk-001\"," +
                "\"artist\":\"Test\",\"event_type\":\"UNKNOWN\",\"ms_played\":5000," +
                "\"country\":\"FR\",\"timestamp\":\"2026-07-16T10:00:00.000Z\"}",
                Instant.parse("2026-07-16T10:00:00.000Z"));

        assertFalse(dlq.isEmpty(), "Le message invalide doit aller en DLQ");
        assertTrue(dlq.readValue().contains("event_type invalide"),
                "La DLQ doit contenir la raison du rejet");
    }

    @Test
    void ecouteCompleteCrediteUneRoyaltie() {
        // COMPLETE à 31 000 ms → éligible → 0.003 € crédité
        input.pipeInput("u-0001",
                "{\"event_id\":\"ev-002\",\"user_id\":\"u-0001\",\"track_id\":\"trk-001\"," +
                "\"artist\":\"Nova Kane\",\"event_type\":\"COMPLETE\",\"ms_played\":31000," +
                "\"country\":\"FR\",\"timestamp\":\"2026-07-16T10:00:00.000Z\"}",
                Instant.parse("2026-07-16T10:00:00.000Z"));

        assertFalse(royalties.isEmpty(), "Une royaltie doit être créditée");
        var record = royalties.readRecord();
        assertEquals("Nova Kane", record.key(), "La clé doit être l'artiste");
        assertTrue(record.value().contains("0.003"), "Le montant 0.003 doit apparaître");    
        }

    @Test
    void skipNeCrediteRien() {
        // SKIP à 12 000 ms → non éligible → aucune royaltie
        input.pipeInput("u-0001",
                "{\"event_id\":\"ev-003\",\"user_id\":\"u-0001\",\"track_id\":\"trk-001\"," +
                "\"artist\":\"Nova Kane\",\"event_type\":\"SKIP\",\"ms_played\":12000," +
                "\"country\":\"FR\",\"timestamp\":\"2026-07-16T10:00:00.000Z\"}",
                Instant.parse("2026-07-16T10:00:00.000Z"));

        assertTrue(royalties.isEmpty(), "Un SKIP ne doit créditer aucune royaltie");
    }
}