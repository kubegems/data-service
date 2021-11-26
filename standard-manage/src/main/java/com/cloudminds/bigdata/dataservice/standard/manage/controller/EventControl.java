package com.cloudminds.bigdata.dataservice.standard.manage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.EventInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventBatchDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventQuery;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.ReviewReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.service.EventService;

@RestController
@RequestMapping("/standard/event")
public class EventControl {

    @Autowired
    private EventService eventService;

    // 创建事件
    @RequestMapping(value = "add", method = RequestMethod.POST)
    public CommonResponse insertEvent(@RequestBody EventInfo eventInfo) {
        return eventService.insertEvent(eventInfo);
    }

    // 检查是否唯一
    @RequestMapping(value = "checkUnique", method = RequestMethod.POST)
    public CommonResponse checkUnique(@RequestBody CheckReq checkReq) {
        return eventService.checkUnique(checkReq);
    }

    // 查询事件
    @RequestMapping(value = "query", method = RequestMethod.POST)
    public CommonQueryResponse queryEvent(@RequestBody EventQuery eventQuery) {
        return eventService.queryEvent(eventQuery);
    }

    // 查询审核页的事件
    @RequestMapping(value = "queryReview", method = RequestMethod.POST)
    public CommonQueryResponse queryReviewEvent(@RequestBody EventQuery eventQuery) {
        return eventService.queryReviewEvent(eventQuery);
    }

    // 根据事件id查询事件详情
    @RequestMapping(value = "queryById", method = RequestMethod.GET)
    public CommonResponse queryById(int id) {
        return eventService.queryById(id);
    }

    // 根据事件编码查询所有版本版本
    @RequestMapping(value = "queryALLVersion", method = RequestMethod.GET)
    public CommonResponse queryAllVersion(String event_code) {
        return eventService.queryAllVersion(event_code);
    }

    // 删除事件
    @RequestMapping(value = "delete", method = RequestMethod.POST)
    public CommonResponse deleteEvent(@RequestBody EventDeleteReq deleteReq) {
        return eventService.deleteEvent(deleteReq);
    }

    // 批量删除事件
    @RequestMapping(value = "batchDelete", method = RequestMethod.POST)
    public CommonResponse batchDeleteEvent(@RequestBody EventBatchDeleteReq batchDeleteReq) {
        return eventService.batchDeleteEvent(batchDeleteReq);
    }

    // 发布事件
    @RequestMapping(value = "online", method = RequestMethod.POST)
    public CommonResponse onlineEvent(@RequestBody DeleteReq onlineReq) {
        return eventService.onlineEvent(onlineReq);
    }

    // 下线事件
    @RequestMapping(value = "offline", method = RequestMethod.POST)
    public CommonResponse offlineEvent(@RequestBody DeleteReq offlineReq) {
        return eventService.offlineEvent(offlineReq);
    }

    // 申请审核事件
    @RequestMapping(value = "applyReview", method = RequestMethod.POST)
    public CommonResponse applyReview(@RequestBody DeleteReq applyReq) {
        return eventService.applyReview(applyReq);
    }

    // 获取事件数量信息
    @RequestMapping(value = "queryEventNumInfo", method = RequestMethod.GET)
    public CommonResponse queryEventNumInfo() {
        return eventService.queryEventNumInfo();
    }

    // 获取各模块事件数量信息
    @RequestMapping(value = "queryModelEventNumInfo", method = RequestMethod.GET)
    public CommonResponse queryModelEventNumInfo(int type) {
        return eventService.queryModelEventNumInfo(type);
    }

    // 审核事件
    @RequestMapping(value = "review", method = RequestMethod.POST)
    public CommonResponse review(@RequestBody ReviewReq reviewReq) {
        return eventService.review(reviewReq);
    }

    //编辑事件
    @RequestMapping(value = "update", method = RequestMethod.POST)
    public CommonResponse updateEvent(@RequestBody EventInfo eventInfo) {
        return eventService.updateEvent(eventInfo);
    }

    //获取用户的email地址
    // 根据事件id查询事件详情
    @RequestMapping(value = "queryUserEmail", method = RequestMethod.GET)
    public CommonResponse queryUserEmail(String userName) {
        return eventService.getEmailAddress(userName);
    }

}
