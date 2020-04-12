package com.byterun.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byterun.model.UserModel;
import com.byterun.service.ContentService;
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
import org.omg.CORBA.OBJECT_NOT_EXIST;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/elastic")
public class ElasticController {

    private final ContentService contentService;
    private final RestHighLevelClient highLevelClient;

    @Autowired
    public ElasticController(ContentService contentService, RestHighLevelClient highLevelClient) {
        this.contentService = contentService;
        this.highLevelClient = highLevelClient;
    }

    // 创建索引
    @PutMapping("/createIndex/{index}")
    public String createIndex(@PathVariable("index") String index) throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest(index);
        CreateIndexResponse indexResponse = highLevelClient.indices()
                .create(indexRequest, RequestOptions.DEFAULT);
        log.info("返回值=>{},确认状态=>{}", indexResponse.index(),
                indexResponse.isAcknowledged());
        return "ok";
    }

    // 删除索引
    @DeleteMapping("/deleteIndex/{index}")
    public String deleteIndex(@PathVariable("index") String index) throws IOException {
        DeleteIndexRequest indexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = highLevelClient.indices()
                .delete(indexRequest, RequestOptions.DEFAULT);
        log.info("返回值=>{},确认状态=>{}", delete.isFragment(),
                delete.isAcknowledged());
        return "ok";
    }

    // 添加文档数据
    @PostMapping("/addDoc/{index}")
    public String addDoc(@RequestBody UserModel user, @PathVariable("index") String index) throws IOException {

        IndexRequest request = new IndexRequest(index);

        request.id("1");   // 设置id
        request.type("my_tbl1");  // 设置type
        request.timeout(TimeValue.timeValueSeconds(1));

        // 数据存储
        request.source(JSONObject.toJSONString(user), XContentType.JSON);

        IndexResponse response = highLevelClient.index(request, RequestOptions.DEFAULT);
        log.info("响应=>{}", response.toString());
        log.info("状态=>{}", response.status());
        return "ok";
    }

    // 批量添加文档数据
    @PostMapping("/bulkRequest/{index}/{type}")
    public String bulkRequest(@RequestBody List<UserModel> users,
                              @PathVariable("index") String index,
                              @PathVariable("type") String type) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueSeconds(10L));

        for (int i=0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest(index)
                    .id(""+(i+1)).type(type)
                    .source(JSONObject.toJSONString(users.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info("响应结果=>{}", bulkResponse.hasFailures());

        return "ok";
    }

    @GetMapping("/search/{index}/{desc}")
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

        SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
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


    // 京东查询、爬虫,数据入库
    @GetMapping("/parseJdSearch/{keyword}")
    public boolean parseJdSearch(@PathVariable("keyword") String keyword) throws IOException {
        return contentService.parseContent(keyword);
    }
    // 查询数据
    @GetMapping("/searchJdPage/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String, Object>> searchJdPage(@PathVariable("keyword") String keyword,
                                                  @PathVariable("pageNo") int pageNo,
                                                  @PathVariable("pageSize") int pageSize) throws IOException {
        return contentService.serachPage(keyword, pageNo, pageSize);
    }


}
