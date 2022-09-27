package org.example.mqtt.broker.cluster.infra.es.config;

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
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchClientConfig {

    @Bean
    public ElasticsearchClient elasticsearchClient(
            @Value("${mqtt.server.cluster.db.impl.es.url:http://nudocker01:9120}") String url,
            @Value("${mqtt.server.cluster.db.impl.es.username:elastic}") String username,
            @Value("${mqtt.server.cluster.db.impl.es.password:8E78NY1mnfGvQJ6e7aHy}") String password) {
        // Create the low-level client
        RestClientBuilder builder = RestClient.builder(HttpHost.create(url));
        if (username != null) {
            final UsernamePasswordCredentials credentials =
                    new UsernamePasswordCredentials(username, password);
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, credentials);
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider));
        }
        RestClient restClient = builder.build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);
        return client;
    }

}
