/***
 *   Copyleft 2016 - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yilmaz@guleryuz.net 
 *  @see https://github.com/WareNinja | http://www.BeerStorm.net 
 *  
 *  disclaimer: I code for fun, dunno what I'm coding about!
 *  
 */

package com.wareninja.opensource.ip2location.config;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.shield.ShieldPlugin;

import java.net.InetAddress;

public class ElasticConfiguration {

	private final Logger logger = Logger.getLogger(ElasticConfiguration.class);
    private final YamlConfiguration config;
    private Client elasticClient;

    public ElasticConfiguration(final YamlConfiguration config) {
        this.config = config;
        this.prepareClient();
    }

    private void prepareClient() {
        try {
        	
        	Settings.Builder elasticSettings = Settings.settingsBuilder();
        	
        	elasticSettings.put("client.transport.ping_timeout", "15s");
        	elasticSettings.put("client.transport.nodes_sampler_interval", "5s");
        	// to ensure reliable connection & resolve NoNodeAvailableException
        	elasticSettings.put("client.transport.sniff", true);
        	elasticSettings.put("network.bind_host", 0);
            
        	// for supporting ES Auth with ES Shield
        	if (config.getElastic().auth!=null) { 
        		elasticSettings.put("shield.user",  
                		config.getElastic().auth.username+":"+config.getElastic().auth.password
                		);
        	}
        	
        	if (config.getElastic().clusterName!=null) {
        		elasticSettings.put("cluster.name", config.getElastic().clusterName);
        	}
        	else {
        		elasticSettings.put("client.transport.ignore_cluster_name", true);
        	}
        	  
            InetSocketTransportAddress ista = new InetSocketTransportAddress(InetAddress.getByName(
            		config.getElastic().host), config.getElastic().port
            		);
            elasticClient = TransportClient.builder()
            		.addPlugin(ShieldPlugin.class)
            		.settings(elasticSettings.build())
            		.build()
            		.addTransportAddress(ista);
            
        } catch (Exception ex) {
        	logger.error(ex.getMessage(), ex.fillInStackTrace());
        }
    }

    public void closeNode() {
    	elasticClient.close();
    }

    public Client getClient() {
        return elasticClient;
    }

}
