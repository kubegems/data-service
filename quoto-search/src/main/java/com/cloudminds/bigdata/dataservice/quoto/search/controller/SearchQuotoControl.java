package com.cloudminds.bigdata.dataservice.quoto.search.controller;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.service.ESQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/search/label")
public class SearchQuotoControl {
    @Autowired
    private ESQueryService eSQueryService;
    // 查询事件
    @RequestMapping(value = "queryByKey", method = RequestMethod.GET)
    public CommonResponse findLabelItemByPid(String key) {
        return eSQueryService.matchQuery(key);
    }

    // 查询事件
    @RequestMapping(value = "boolQueryByKey", method = RequestMethod.GET)
    public CommonResponse boolQueryByKey(String key) {
        return eSQueryService.boolQuery(key);
    }
}
