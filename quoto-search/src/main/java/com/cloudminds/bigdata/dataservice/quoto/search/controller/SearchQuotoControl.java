package com.cloudminds.bigdata.dataservice.quoto.search.controller;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.QueryDataByRowKeyReq;
import com.cloudminds.bigdata.dataservice.quoto.search.service.ESQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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

    // 查询符合条件的数据
    @RequestMapping(value = "queryDataInfo", method = RequestMethod.POST)
    public CommonResponse queryDataInfo(@RequestBody String request) {
        return eSQueryService.queryDataInfo(request);
    }

    //根据标签对象查询文档
    @RequestMapping(value = "queryApiDoc", method = RequestMethod.GET)
    public CommonResponse queryApiDoc(String object_code){
        return eSQueryService.queryApiDoc(object_code);
    }

    @RequestMapping(value = "queryDataByRowKey", method = RequestMethod.POST)
    public CommonResponse queryDataByRowKey(@RequestBody QueryDataByRowKeyReq queryDataByRowKeyReq){
        return eSQueryService.queryDataByRowKey(queryDataByRowKeyReq);
    }
}
