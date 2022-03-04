package com.rfoe.sample.kafka.cluster.policy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.rfoe.sample.kafka.cluster.config.ClusterSwitch;
import com.rfoe.sample.kafka.cluster.context.ContextProvider;
import com.rfoe.sample.kafka.cluster.factory.CustomKafkaListenerContainerFactory;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Component;

@Component("OCBrokerCheck")
public class BrokerHealthcheck {

	private static final String BOOLEAN_YES = "Y";
	private static final String BOOLEAN_NO = "N";

	public static final String BROKER_CHECK_BEAN = "OCBrokerCheck";
	private static final Logger log = LoggerFactory.getLogger(BrokerHealthcheck.class);

	@Value("${broker.check.interest.group}")
	private String listenTo;
	public String getListenTo(){return this.getDefaultIfNull(listenTo, "") ;}
	public void setListenTo(String listenTo){this.listenTo = listenTo;}

	@Value("${broker.check.autofailover}")
	private String autoFailover;
	public void setAutoFailover(String autoFailover){this.autoFailover = autoFailover;}
	public void setAutoFailover(boolean autoFailover){this.autoFailover = (autoFailover) ? BrokerHealthcheck.BOOLEAN_YES : BrokerHealthcheck.BOOLEAN_NO;}
	public boolean isAutoFailover(){return BrokerHealthcheck.BOOLEAN_YES.equalsIgnoreCase(autoFailover);}


	@Value("${broker.check.min.node}")
	private int minNodeAvailable;
	public void setMinNodeAvailable(int minNode){ this.minNodeAvailable = minNode;}
	public int getMinNodeAvailable(){return this.minNodeAvailable;}



	private Map<String,Object> brokerProperties;

	BrokerHealthcheck(){
		this.configureProperties();
	}

	public void configureProperties(){
        KafkaAdmin admin = ContextProvider.getApplicationContext().getBean(KafkaAdmin.class);
		CustomKafkaListenerContainerFactory<?,?> factory = ContextProvider.getApplicationContext().getBean(CustomKafkaListenerContainerFactory.class);

		this.brokerProperties = new HashMap<>(admin.getConfigurationProperties());
		this.brokerProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.parseInt(factory.getContainerProperties().getIdleEventInterval().toString()));
		this.brokerProperties.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 10000);
		this.brokerProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterSwitch.getInstance().getCurentActiveBrokerUrl());

		
	}

	//** UTILITIES *********************** */
	private String getDefaultIfNull(String text, String defText){
		if(text == null){
			return defText;
		}
		return text;
	}





	@EventListener
	public void idler(ListenerContainerIdleEvent event) {
		log.info("@@@@@@@@@@@@ " + event.toString());
		log.info("-- " + event.getListenerId());

		// no autofailover
		if(!this.isAutoFailover()){
			return;
		}

		// does not match with interest groups
		if(!event.getListenerId().startsWith(this.getListenTo())){
			return;
		}

		if( event.getListenerId().startsWith(this.getListenTo()) ){
			log.info("@@ - start to perform failover check");

			try (AdminClient client = AdminClient.create(this.brokerProperties)) {

				DescribeClusterResult describeCluster = client.describeCluster();
				String cluster = describeCluster.clusterId().get(10,  TimeUnit.SECONDS);

				log.info("Broker [REACHABLE]: " + cluster);
				log.info("Broker [CHECK FOR]: " + cluster);

				Collection<Node> nodes = describeCluster.nodes().get(10, TimeUnit.SECONDS);
				if(nodes.size() < this.minNodeAvailable){
					log.warn("Broker [CHECK NODE] : Cluster Does not meet min node size of %s current active node size %s ", this.minNodeAvailable , nodes.size());
					log.info(" Performing auto failover.....");
					ClusterSwitch.getInstance().failoverCluster();
				}

			}catch(Exception e){
				log.info("Broker [DOWN] : Performing auto failover ");
				ClusterSwitch.getInstance().failoverCluster();
			}

		}
		
	}
    
}
