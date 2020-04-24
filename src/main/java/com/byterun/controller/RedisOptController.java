package com.byterun.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/redisOpt")
@Slf4j
public class RedisOptController {
    @Autowired
    private RedisTemplate redisTemplate;

    @GetMapping("/ok/{key}/{value}")
    @Cacheable(value = "ok", key = "#key")
    public String ok(@PathVariable("key") String key,
                     @PathVariable("value") String value) {
        log.info("入参信息key=>{},value=>{}", key, value);
        return "ok";
    }

    @GetMapping("/ok2/{key}")
    public String ok2(@PathVariable("key") String key) {
        redisTemplate.opsForValue().set(key, "测试");
        log.info("入参信息key=>{}", key);
        return "ok2";
    }
}
