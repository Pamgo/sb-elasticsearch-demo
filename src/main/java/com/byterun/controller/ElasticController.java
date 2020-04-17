package com.byterun.controller;

import com.byterun.model.UserModel;
import com.byterun.service.ContentService;
import com.byterun.service.ElasticService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/elastic")
public class ElasticController {

    private final ContentService contentService;
    private final ElasticService elasticService;

    @Autowired
    public ElasticController(ContentService contentService, ElasticService elasticService) {
        this.contentService = contentService;
        this.elasticService = elasticService;
    }

    // 创建索引
    @PutMapping("/createIndex/{index}")
    public String createIndex(@PathVariable("index") String index) throws IOException {
        elasticService.createIndex(index);
        return "ok";
    }

    // 删除索引
    @DeleteMapping("/deleteIndex/{index}")
    public String deleteIndex(@PathVariable("index") String index) throws IOException {
        elasticService.deleteIndex(index);
        return "ok";
    }

    // 添加文档数据
    @PostMapping("/addDoc/{index}")
    public String addDoc(@RequestBody UserModel user, @PathVariable("index") String index) throws IOException {
        elasticService.addDoc(user, index);
        return "ok";
    }

    // 批量添加文档数据
    @PostMapping("/bulkRequest/{index}/{type}")
    public String bulkRequest(@RequestBody List<UserModel> users,
                              @PathVariable("index") String index,
                              @PathVariable("type") String type) throws IOException {
        elasticService.bulkRequest(users, index, type);
        return "ok";
    }

    @GetMapping("/search/{index}/{desc}")
    public List<UserModel> search(@PathVariable("index") String index,
                         @PathVariable("desc") String desc) throws IOException {
        List<UserModel> userModels = elasticService.search(index, desc);
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
        log.info("查询es数据，参数:{},{},{}", keyword, pageNo, pageSize);
        return contentService.serachPage(keyword, pageNo, pageSize);
    }


}
