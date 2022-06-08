package com.cloudminds.bigdata.dataservice.label.manage.controller;

import com.cloudminds.bigdata.dataservice.label.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.label.manage.service.LabelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/label")
public class LabelControl {
    @Autowired
    private LabelService labelService;

    // 查询事件
    @RequestMapping(value = "queryByPid", method = RequestMethod.GET)
    public CommonResponse findLabelItemByPid(String pid,int value) {
        return labelService.findLabelItemByPid(pid,value);
    }
}
