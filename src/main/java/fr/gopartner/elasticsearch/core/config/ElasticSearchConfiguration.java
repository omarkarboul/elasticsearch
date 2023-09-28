package fr.gopartner.elasticsearch.core.config;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;


@Configuration
public class ElasticSearchConfiguration {

    @Value("${elasticsearch.username}")
    String username;
    @Value("${elasticsearch.password}")
    String password;
    @Value("${elasticsearch.host}")
    String host;
    @Value("${elasticsearch.port}")
    int port;
    @Value("${elasticsearch.caCertificatePath}")
    private String caCertificatePath;
    @Value("${elasticsearch.connectionType}")
    String connectionType;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient() throws IOException {
        File certFile = new File(caCertificatePath);
        SSLContext sslContext = TransportUtils.sslContextFromHttpCaCrt(certFile);
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        return new RestHighLevelClientBuilder(RestClient.builder(new HttpHost(host, port, connectionType))
                .setHttpClientConfigCallback(hc -> hc
                        .setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(basicCredentialsProvider
                        )
                ).build())
                .setApiCompatibilityMode(true)
                .build();
    }

    @Bean
    public ElasticsearchTransport getElasticsearchTransport() throws IOException {
        return new RestClientTransport(
                restHighLevelClient().getLowLevelClient(), new JacksonJsonpMapper());
    }


    @Bean
    public ElasticsearchClient getElasticsearchClient() throws IOException {
        return new ElasticsearchClient(getElasticsearchTransport());
    }

}
