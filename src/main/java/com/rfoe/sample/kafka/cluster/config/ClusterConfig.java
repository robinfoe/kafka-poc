package com.rfoe.sample.kafka.cluster.config;

public class ClusterConfig {
    
    private String bootstrapURL;
    public String getBootstrapURL() {return bootstrapURL;}
    public void setBootstrapURL(String bootstrapURL) {this.bootstrapURL = bootstrapURL;}
    
    private String prefix;
    public String getPrefix() {return prefix;}
    public void setPrefix(String prefix) {this.prefix = prefix;}

    public ClusterConfig(){}
    public ClusterConfig(String bootstrapURL , String prefix){
        this.bootstrapURL = bootstrapURL;
        this.prefix = prefix;
    }
    
    public String generateTopicName(String name){
        if(name != null && (name.trim()) .length() > 0){
            return this.prefix + "." + name;
        }
        return name;
    }
}
