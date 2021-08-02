package com.cloudminds.bigdata.dataservice.standard.manage.controller;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.CommonField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.ModelField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventQuery;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.LogFieldDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.service.LogFieldService;

@RestController
@RequestMapping("/standard/logField")
public class LogFieldControl {
    @Autowired
    private LogFieldService logFieldService;

    // 获取通用的日志字段
    @RequestMapping(value = "common", method = RequestMethod.GET)
    public CommonResponse getCommonLogField(int type, String version) {
        return logFieldService.getCommonLogField(type, version);
    }

    // 获取模块的日志字段
    @RequestMapping(value = "model", method = RequestMethod.GET)
    public CommonResponse getModelLogField(int type, String version, String model_name) {
        return logFieldService.getModelLogField(type, version, model_name);
    }

    // 获取模块信息
    @RequestMapping(value = "modelInfo", method = RequestMethod.GET)
    public CommonResponse getModel(int type) {
        return logFieldService.getModelInfo(type);
    }

    //删除通用的日志字段
    @RequestMapping(value = "deleteCommonLogField", method = RequestMethod.POST)
    public CommonResponse deleteCommonLogField(@RequestBody LogFieldDeleteReq deleteReq) {
        return logFieldService.deleteCommonLogField(deleteReq);
    }

    //更新通用的日志字段
    @RequestMapping(value = "updateCommonLogField", method = RequestMethod.POST)
    public CommonResponse updateCommonLogField(@RequestBody CommonField commonField) {
        return logFieldService.updateCommonLogField(commonField);
    }

    //查询通用日志
    @RequestMapping(value = "queryCommonLogField", method = RequestMethod.POST)
    public CommonQueryResponse queryCommonLogField(@RequestBody EventQuery eventQuery) {
        return logFieldService.queryCommonLogField(eventQuery);
    }

    //查询模块日志
    @RequestMapping(value = "queryModelLogField", method = RequestMethod.POST)
    public CommonQueryResponse queryModelLogField(@RequestBody EventQuery eventQuery) {
        return logFieldService.queryModelLogField(eventQuery);
    }

    //删除模块日志
    @RequestMapping(value = "deleteModelLogField", method = RequestMethod.POST)
    public CommonResponse deleteModelLogField(@RequestBody LogFieldDeleteReq deleteReq) {
        return logFieldService.deleteModelLogField(deleteReq);
    }

    //更新模块日志
    @RequestMapping(value = "updateModelLogField", method = RequestMethod.POST)
    public CommonResponse updateModelLogField(@RequestBody ModelField modelField) {
        return logFieldService.updateModelLogField(modelField);
    }

    //新增模块日志
    @RequestMapping(value = "insertModelLogField", method = RequestMethod.POST)
    public CommonResponse insertModelLogField(@RequestBody ModelField modelField) {
        return logFieldService.insertModelLogField(modelField);
    }

    // 检查模块名是否唯一
    @RequestMapping(value = "checkUnique", method = RequestMethod.POST)
    public CommonResponse checkUnique(@RequestBody CheckReq checkReq) {
        return logFieldService.checkUnique(checkReq);
    }
}
