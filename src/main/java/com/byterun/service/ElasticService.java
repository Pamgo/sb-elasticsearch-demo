package com.byterun.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byterun.model.UserModel;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ElasticService {


    private final RestHighLevelClient restHighLevelClient;
    @Autowired
    public ElasticService(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    // 创建索引（测试）
    public String createIndex(String index) throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest(index);
        CreateIndexResponse indexResponse = restHighLevelClient.indices()
                .create(indexRequest, RequestOptions.DEFAULT);
        log.info("返回值=>{},确认状态=>{}", indexResponse.index(),
                indexResponse.isAcknowledged());
        return "ok";
    }

    // 删除索引(测试)
    public String deleteIndex(String index) throws IOException {
        DeleteIndexRequest indexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = restHighLevelClient.indices()
                .delete(indexRequest, RequestOptions.DEFAULT);
        log.info("返回值=>{},确认状态=>{}", delete.isFragment(),
                delete.isAcknowledged());
        return "ok";
    }

    // 添加文档(测试)
    public String addDoc(UserModel user, String index) throws IOException {

        IndexRequest request = new IndexRequest(index);

        request.id("1");   // 设置id
        request.type("my_tbl1");  // 设置type
        request.timeout(TimeValue.timeValueSeconds(1));

        // 数据存储
        request.source(JSONObject.toJSONString(user), XContentType.JSON);

        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        log.info("响应=>{}", response.toString());
        log.info("状态=>{}", response.status());
        return "ok";
    }

    // 批量添加数据（测试）
    public String bulkRequest(List<UserModel> users, String index, String type) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueSeconds(10L));

        for (int i=0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest(index)
                            .id(""+(i+1)).type(type)  // 7以后，type可以省略
                            .source(JSONObject.toJSONString(users.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info("响应结果=>{}", bulkResponse.hasFailures());

        return "ok";
    }

    // 查询数据（测试）
    public List<UserModel> search(@PathVariable("index") String index,
                                  @PathVariable("desc") String desc) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        // 构造搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询条件
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("desc", desc);
        sourceBuilder.query(matchQuery)
                .timeout(TimeValue.timeValueSeconds(60));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        log.info(JSON.toJSONString(searchResponse.getHits()));
        SearchHit[] hits = searchResponse.getHits().getHits();
        // 构造返回值
        List<UserModel> userModels = Arrays.stream(hits).map((hit) -> {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            UserModel user = new UserModel();
            user.setName((String) sourceAsMap.get("name"));
            user.setAge((Integer) sourceAsMap.get("age"));
            user.setDesc((String) sourceAsMap.get("desc"));
            user.setEntry_time((String) sourceAsMap.get("experience"));
            user.setLevel((String) sourceAsMap.get("level"));
            user.setEntry_time((String) sourceAsMap.get("entry_time"));
            return user;
        }).collect(Collectors.toList());

        return userModels;
    }

}
