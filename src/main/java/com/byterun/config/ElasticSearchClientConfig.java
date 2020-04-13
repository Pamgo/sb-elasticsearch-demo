package com.byterun.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchClientConfig {

    @Bean
    @Qualifier("highLevelClient")
    public RestHighLevelClient restHighLevelClient() {
        RestHighLevelClient highLevelClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("127.0.0.1", 9200, "http")
        ));
        return highLevelClient;
    }
}
