package com.rfoe.sample.kafka.cluster.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.rfoe.sample.kafka.cluster.context.ContextProvider;
import com.rfoe.sample.kafka.cluster.factory.CustomKafkaListenerContainerFactory;
import com.rfoe.sample.kafka.cluster.policy.BrokerHealthcheck;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

public class ClusterSwitch {

    Logger log = LoggerFactory.getLogger(ClusterSwitch.class);

    private static ClusterSwitch INSTANCE;

    private ClusterConfig primaryConfig;
    private ClusterConfig replicaConfig;
    private List<KafkaListenerEndpoint> endpoints = new ArrayList<KafkaListenerEndpoint>();
    public void addEndpoint(KafkaListenerEndpoint endpoint){
        this.endpoints.add(endpoint);
    }

    private ABSwitchCluster switcher;


    private ClusterSwitch(){/** ENSURE SINGLETON**/}

    public ABSwitchCluster getSwitch(){return switcher;}
    public boolean isPrimary(){return this.getSwitch().isPrimary();}
    public String generateSiteReplicationTopicName(String name ){
        return ( this.getSwitch().isPrimary() ) ? this.replicaConfig.generateTopicName(name)  : this.primaryConfig.generateTopicName(name);
    }


    public void switchToPrimary(){
        if (!this.isPrimary()){ this.failoverCluster(); }
    }

    public void switchToReplica(){
        if (this.isPrimary()) { this.failoverCluster(); }
    }

    
    public void failoverCluster(){

        @SuppressWarnings("all")
        DefaultKafkaProducerFactory  producer = ContextProvider.getApplicationContext().getBean(DefaultKafkaProducerFactory.class);
        KafkaListenerEndpointRegistry registry = ContextProvider.getApplicationContext().getBean(KafkaListenerEndpointRegistry.class);
        BrokerHealthcheck brokerCheck = ContextProvider.getApplicationContext().getBean(BrokerHealthcheck.BROKER_CHECK_BEAN, BrokerHealthcheck.class);

        registry.stop();
        for(MessageListenerContainer listener : registry.getListenerContainers() ){
           listener.stop();
        }

        if(this.isPrimary()){
            this.getSwitch().secondary();
        }else{
            this.getSwitch().primary();
        }

        registry.start();
        producer.reset();


        for(MessageListenerContainer listener : registry.getListenerContainers() ){
            this.consumeBacklog(listener);
            listener.start();
        }

        
        brokerCheck.configureProperties();

    }

    public void consumeBacklog(MessageListenerContainer listener){



        CustomKafkaListenerContainerFactory<?,?> factory = ContextProvider.getApplicationContext().getBean(CustomKafkaListenerContainerFactory.class);
        Map<String,Object> configProps = factory.getConsumerFactory().getConfigurationProperties();

        List<String> backlogTopics = new ArrayList<String>();
        for(String item : listener.getContainerProperties().getTopics()){
            backlogTopics.add(this.generateSiteReplicationTopicName(item));
        }

        // listener.getGroupId()
        // retreive endpoint based on group id 
        KafkaListenerEndpoint kep = null;
        for(KafkaListenerEndpoint item : this.endpoints){
            if(listener.getGroupId().equals(item.getGroupId())){
                kep = item;
                break;
            }
        }

        if(kep != null){
            @SuppressWarnings("all")
            MethodKafkaListenerEndpoint ep = (MethodKafkaListenerEndpoint)kep;

            Properties props = new Properties();

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, ep.getGroupId());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, configProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, configProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

            @SuppressWarnings("all")
            KafkaConsumer consumer = null;

            try{
                consumer = new KafkaConsumer<>(props);

                List<String> listenTopics = new ArrayList<String>();
                for(Object item : ep.getTopics()){
                    listenTopics.add(this.generateSiteReplicationTopicName( (String) item ));
                }

                consumer.subscribe(listenTopics);
                Duration pollDuration = Duration.ofMillis(1000);
                for(int i = 0; i < 3; i ++){
                    ConsumerRecords<?,?> records = consumer.poll(pollDuration);
                    log.info("pulling.....");
                    for (ConsumerRecord<?,?> record : records){
                        ep.getMethod().invoke(ep.getBean(), record.value());
                    }
                }
            }
            catch(Exception e){
                log.error(e.getMessage());
            }finally{
                if(consumer != null){
                    consumer.close();
                    consumer = null;
                }
            }
            
        }
    }

    public String  getCurentActiveBrokerUrl(){
        return (this.isPrimary()) ? this.primaryConfig.getBootstrapURL() : this.replicaConfig.getBootstrapURL();
    }

    public static void configureSwitch(String primaryBroker, String replicaBroker, String primaryPrefix, String replicaPrefix){

        if(ClusterSwitch.INSTANCE == null){
            ClusterSwitch.INSTANCE  = new ClusterSwitch();
        }
    
        ClusterSwitch.INSTANCE.primaryConfig = new ClusterConfig(primaryBroker, primaryPrefix);
        ClusterSwitch.INSTANCE.replicaConfig = new ClusterConfig(replicaBroker, replicaPrefix);
        ClusterSwitch.INSTANCE.switcher = new ABSwitchCluster( ClusterSwitch.INSTANCE.primaryConfig.getBootstrapURL(),  ClusterSwitch.INSTANCE.replicaConfig.getBootstrapURL() );
    }

    public static ClusterSwitch getInstance(){
        if(ClusterSwitch.INSTANCE == null){
            throw new IllegalArgumentException("ClusterSwitch - is null, run ClusterSwitch.configureSwitch before proceed");
        }

        return ClusterSwitch.INSTANCE;
    }

    
}
