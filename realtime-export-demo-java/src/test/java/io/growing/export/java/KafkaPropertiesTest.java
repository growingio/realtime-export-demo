package io.growing.export.java;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by dengziming on 2021/10/18.
 */
public class KafkaPropertiesTest {

    @Test
    public void testSaslProperties() {
        Properties properties = KafkaProperties.saslProperties("test", "username", "password");
        assertEquals("SASL_PLAINTEXT", properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("SCRAM-SHA-256", properties.getProperty(SaslConfigs.SASL_MECHANISM));
    }
}
