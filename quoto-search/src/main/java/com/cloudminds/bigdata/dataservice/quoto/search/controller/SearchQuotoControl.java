package com.cloudminds.bigdata.dataservice.quoto.search.controller;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.service.ESQueryService;
import com.cloudminds.bigdata.dataservice.quoto.search.service.HbaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/search/label")
public class SearchQuotoControl {
    @Autowired
    private ESQueryService eSQueryService;

    @Autowired
    private HbaseService hbaseService;

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

    // 查询符合条件的数据
    @RequestMapping(value = "queryDataInfo", method = RequestMethod.POST)
    public CommonQueryResponse queryDataInfo(String request) {
        return eSQueryService.queryDataInfo(request);
    }

}
