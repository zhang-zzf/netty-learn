package org.example.mqtt.broker.cluster.infra.es.config;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

public class ElasticsearchClientConfig {

    public ElasticsearchClient elasticsearchClient(String inetHost, int inetPort) {
        final UsernamePasswordCredentials credentials =
                new UsernamePasswordCredentials("elastic", "8E78NY1mnfGvQJ6e7aHy");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
        // Create the low-level client
        RestClient restClient = RestClient
                .builder(new HttpHost(inetHost, inetPort))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider))
                .build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);
        return client;
    }

    public ElasticsearchAsyncClient elasticsearchAsyncClient(String inetHost, int inetPort) {
        final UsernamePasswordCredentials credentials =
                new UsernamePasswordCredentials("elastic", "8E78NY1mnfGvQJ6e7aHy");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
        // Create the low-level client
        RestClient restClient = RestClient
                .builder(new HttpHost(inetHost, inetPort))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        return new ElasticsearchAsyncClient(transport);
    }

}
