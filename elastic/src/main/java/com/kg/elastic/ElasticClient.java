package com.kg.elastic;

import com.kg.config.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by kevingracie on 22/09/2016.
 */
public class ElasticClient {

    private static TransportClient CLIENT;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticClient.class);

    private ElasticClient() {

    }

    public static Client getClient(final Configuration config) {
        if(CLIENT == null) {
            try {
                final Settings settings = Settings.builder().put("cluster.name", config.getClusterName()).build();
                CLIENT = TransportClient.builder().settings(settings).build();
                for(String node : config.getElasticNodes()) {
                    final String[] hostPortSplit = node.split(":");
                    CLIENT.addTransportAddress(
                            new InetSocketTransportAddress(InetAddress.getByName(hostPortSplit[0]),
                                    Integer.parseInt(hostPortSplit[1])));
                }
            }
            catch(UnknownHostException e) {
                LOGGER.error("Unable to construct Elastic client", e);
                throw new IllegalStateException("Unable to construct Elastic client", e);
            }
        }
        return CLIENT;
    }
}
