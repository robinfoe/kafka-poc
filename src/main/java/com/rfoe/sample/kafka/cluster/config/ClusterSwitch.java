package com.rfoe.sample.kafka.cluster.config;

import com.rfoe.sample.kafka.cluster.context.ContextProvider;
import com.rfoe.sample.kafka.cluster.policy.BrokerHealthcheck;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

public class ClusterSwitch {


    private static ClusterSwitch INSTANCE;

    private ClusterConfig primaryConfig;
    private ClusterConfig replicaConfig;

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
        for(MessageListenerContainer listener : registry.getListenerContainers() ){
            //need to add code to drain the messages from the replica topic first in the new cluster it switch too. Before listening to message of the topic itself.

           listener.start();
        }
        producer.reset();

        brokerCheck.configureProperties();

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
