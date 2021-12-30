package com.rfoe.sample.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import com.rfoe.sample.kafka.cluster.config.ClusterSwitch;
import com.rfoe.sample.kafka.cluster.factory.CustomKafkaListenerContainerFactory;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

    @Value("${kafka.primary.hall.prefix}")
	private String prefixPrimary;

	@Value("${kafka.replica.hall.prefix}")
	private String prefixReplica;

    @Value("${kafka.primary.broker}")
	private String primaryBroker;

	@Value("${kafka.replica.broker}")
	private String replicaBroker;

    @PostConstruct
    public void init(){
        ClusterSwitch.configureSwitch(primaryBroker, replicaBroker, prefixPrimary, prefixReplica);
    }
   


    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<Object, Object>(configs);
        pf.setBootstrapServersSupplier(ClusterSwitch.getInstance().getSwitch());
        return pf;
    }

    @Bean
    public DefaultKafkaConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put("enable.auto.commit","true");
        configs.put("auto.commit.interval.ms","1000");
        configs.put("reconnect.backoff.ms","10000");

        DefaultKafkaConsumerFactory<Object, Object> cf = new DefaultKafkaConsumerFactory<Object, Object>(configs);
		cf.setBootstrapServersSupplier(ClusterSwitch.getInstance().getSwitch());

		return cf;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public CustomKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> consumerFactory) {
        CustomKafkaListenerContainerFactory<Object, Object> factory = new CustomKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setIdleEventInterval(10000L);
        return factory;

    }

    
    @KafkaListener(id = "staffGrp", topics = "staff")
    public void listenerStaff(String text) {
        log.info("incoming from staff listener" + text);
	}

    @KafkaListener(id = "taskGrp", topics = "task")
    public void listenerTask(String text) {
        log.info("incoming from TaskListener" + text);
	}

}
