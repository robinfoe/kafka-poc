package com.rfoe.sample.kafka.cluster.factory;

import java.util.Collection;

import com.rfoe.sample.kafka.cluster.config.ClusterSwitch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionOffset;


/**
 * CustomKafkaListenerContainerFactory
 */
// @Component
public class CustomKafkaListenerContainerFactory<K, V> extends ConcurrentKafkaListenerContainerFactory<K, V>{

    Logger log = LoggerFactory.getLogger(CustomKafkaListenerContainerFactory.class);

    @Override
	protected ConcurrentMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {

		@SuppressWarnings("all")
		MethodKafkaListenerEndpoint<K,V> ep  = (MethodKafkaListenerEndpoint) endpoint;
		ClusterSwitch.getInstance().addEndpoint(ep);

		TopicPartitionOffset[] topicPartitions = endpoint.getTopicPartitionsToAssign();
		if (topicPartitions != null && topicPartitions.length > 0) {
			ContainerProperties properties = new ContainerProperties(topicPartitions);
			return new ConcurrentMessageListenerContainer<>(getConsumerFactory(), properties);
		}
		else {
			Collection<String> topics = endpoint.getTopics();
			if (!topics.isEmpty()) { // NOSONAR
				ContainerProperties properties = new ContainerProperties(topics.toArray(new String[0]));
				return new ConcurrentMessageListenerContainer<>(getConsumerFactory(), properties);
			}
			else {
				ContainerProperties properties = new ContainerProperties(endpoint.getTopicPattern()); // NOSONAR
				return new ConcurrentMessageListenerContainer<>(getConsumerFactory(), properties);
			}
		}
	}

    
}