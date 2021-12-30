package com.rfoe.sample.kafka.controller;

import com.rfoe.sample.kafka.cluster.config.ClusterSwitch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SendController {

    @Autowired
	private KafkaTemplate<String, String> template;

	@GetMapping(path = "/send/{topic}/{message}")
	public void sendTask(@PathVariable String topic,@PathVariable String message) {
		this.template.send(topic, message);
	}

	@GetMapping(path = "/failover")
	public void failoverCluster() {
		ClusterSwitch.getInstance().failoverCluster();
	}


	@GetMapping(path = "/switch/primary")
	public void switchToPrimaryCluster() {
		ClusterSwitch.getInstance().switchToPrimary();;
	}

	@GetMapping(path = "/switch/replica")
	public void switchToReplica() {
		ClusterSwitch.getInstance().switchToReplica();;
	}
    
}


