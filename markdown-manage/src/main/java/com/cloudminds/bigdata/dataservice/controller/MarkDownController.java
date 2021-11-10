package com.cloudminds.bigdata.dataservice.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/markdown")
public class MarkDownController {

    @RequestMapping("/edit")
    public String edit() {
        return "edit";
    }

}
