package com.byterun.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Controller
@Slf4j
public class SearchJdController {

    @RequestMapping({"/","/index"})
    public String index() {
        return "index";
    }
}
