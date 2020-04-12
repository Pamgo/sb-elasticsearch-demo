package com.byterun.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byterun.constant.EsConst;
import com.byterun.model.ContentModel;
import com.byterun.utils.HtmlParseUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.swing.text.Highlighter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ContentService {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 1、解析数据放入es中
    public boolean parseContent(String keywords) throws IOException {
        List<ContentModel> parseHtml = HtmlParseUtil.parseHtml(keywords);
        // 把查询的数据放入 es 中
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueSeconds(2));

        parseHtml.forEach((c)-> {
            bulkRequest.add(new IndexRequest(EsConst.indices_jd)
            .source(JSON.toJSONString(c), XContentType.JSON));
        });

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulkResponse.hasFailures();
    }

    // 分页查询
    public List<Map<String, Object>> serachPage(String keyword, int pageNo,
                                                int pageSize) throws IOException {
        if (pageNo <= 1) {
            pageNo = 1;
        }

        // 条件查询
        SearchRequest searchRequest = new SearchRequest(EsConst.indices_jd);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 分页
        sourceBuilder.from(pageNo);
        sourceBuilder.size(pageSize);

        //精准匹配
        TermQueryBuilder termQuery = QueryBuilders.termQuery("title", keyword);
        sourceBuilder.query(termQuery);
        sourceBuilder.timeout(TimeValue.timeValueSeconds(50));
        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.requireFieldMatch(false); // 是否需要多个高亮
        highlightBuilder.preTags("<span type='color:red");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);
        // 执行搜索，分页
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 解析结果，
        List<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()){
            // 解析高亮字段
            Map<String, HighlightField> highlightFields =
                    hit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (title != null) { // 解析高亮,替换高亮字段
                Text[] fragments = title.fragments();
                String n_title = "";
                for (Text t : fragments) {
                    n_title += t;
                }
                sourceAsMap.put("title", n_title); // 替换原理的高亮字段
            }
            list.add(sourceAsMap);
        }

        return list;
    }

}
