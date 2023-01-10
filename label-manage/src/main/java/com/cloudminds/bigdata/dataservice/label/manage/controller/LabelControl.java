package com.cloudminds.bigdata.dataservice.label.manage.controller;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemComplex;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemTask;
import com.cloudminds.bigdata.dataservice.label.manage.entity.request.*;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.label.manage.service.LabelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/label")
public class LabelControl {
    @Autowired
    private LabelService labelService;

    // 根据pid查询标签
    @RequestMapping(value = "queryByPid", method = RequestMethod.GET)
    public CommonResponse findLabelItemByPid(String pid,int value) {
        return labelService.findLabelItemByPid(pid,value);
    }

    //新增标签
    @RequestMapping(value = "addLabelItem", method = RequestMethod.POST)
    public CommonResponse insertLableItem(@RequestBody TagItem tagItem){
        return labelService.insertLableItem(tagItem);
    }

    //更新标签
    @RequestMapping(value = "updateLabelItem", method = RequestMethod.POST)
    public CommonResponse updateLabelItem(@RequestBody TagItem tagItem){
        return labelService.updateLabelItem(tagItem);
    }

    //上下线标签
    @RequestMapping(value = "updateLabelItemState", method = RequestMethod.POST)
    public CommonResponse updateLabelItemState(@RequestBody UpdateLabelItemState updateLabelItemState){
        return labelService.updateLabelItemState(updateLabelItemState);
    }

    //删除标签
    @RequestMapping(value = "deleteLabelItem", method = RequestMethod.POST)
    public CommonResponse deleteLabelItem(@RequestBody UpdateLabelItemState updateLabelItemState){
        return labelService.deleteLabelItem(updateLabelItemState);
    }

    //查询标签
    @RequestMapping(value = "queryLabelItem", method = RequestMethod.GET)
    public CommonResponse queryLabelItem(int tag_object_id,String tag_cate_id,int page,int size,String order_name,boolean desc,String tag_name) {
        return labelService.queryLabelItem(tag_object_id,tag_cate_id,page,size,order_name,desc,tag_name);
    }

    //查询标签
    @RequestMapping(value = "queryAllLabelItem", method = RequestMethod.GET)
    public CommonResponse queryAllLabelItem(int tag_object_id) {
        return labelService.queryAllLabelItem(tag_object_id);
    }

    //查询标签
    @RequestMapping(value = "queryAllLabelItemNoTask", method = RequestMethod.GET)
    public CommonResponse queryAllLabelItemNoTask(int tag_object_id) {
        return labelService.queryAllLabelItemNoTask(tag_object_id);
    }

    //查询标签
    @RequestMapping(value = "queryLabelItemByTagEnums", method = RequestMethod.POST)
    public CommonResponse queryLabelItemByTagEnums(@RequestBody LabelItemByTagEnumsQuery labelItemByTagEnumsQuery) {
        return labelService.queryLabelItemByTagEnums(labelItemByTagEnumsQuery);
    }

    //查询标签
    @RequestMapping(value = "dataPreview", method = RequestMethod.GET)
    public CommonResponse dataPreview(String tag_id) {
        return labelService.dataPreview(tag_id);
    }

    //创建标签组合
    @RequestMapping(value = "addLabelItemComplex", method = RequestMethod.POST)
    public CommonResponse addLabelItemComplex(@RequestBody TagItemComplex tagItemComplex){
        return labelService.addLabelItemComplex(tagItemComplex);
    }

    //更新标签组合
    @RequestMapping(value = "updateLabelItemComplex", method = RequestMethod.POST)
    public CommonResponse updateLabelItemComplex(@RequestBody TagItemComplex tagItemComplex){
        return labelService.updateLabelItemComplex(tagItemComplex);
    }

    //更新标签组合的状态
    @RequestMapping(value = "updateLabelItemComplexStatus", method = RequestMethod.POST)
    public CommonResponse updateLabelItemComplexStatus(@RequestBody UpdateLabelItemComplexStatusReq updateLabelItemComplexStatusReq){
        return labelService.updateLabelItemComplexStatus(updateLabelItemComplexStatusReq.getId(),updateLabelItemComplexStatusReq.getState());
    }

    //查询标签组合
    @RequestMapping(value = "queryLabelItemComplex", method = RequestMethod.POST)
    public CommonResponse queryLabelItemComplex(@RequestBody LabelItemComplexQuery labelItemComplexQuery) {
        return labelService.queryLabelItemComplex(labelItemComplexQuery);
    }

    //删除标签组合
    @RequestMapping(value = "deleteLabelItemComplex", method = RequestMethod.POST)
    public CommonResponse deleteLabelItemComplex(@RequestBody DeleteReq deleteReq){
        return labelService.deleteLabelItemComplex(deleteReq);
    }

    //创建标签任务
    @RequestMapping(value = "addLabelItemTask", method = RequestMethod.POST)
    public CommonResponse addLabelItemTask(@RequestBody TagItemTask tagItemTask){
        return labelService.addLabelItemTask(tagItemTask);
    }

    //判断标签任务是否可以创建
    @RequestMapping(value = "checkLabelItemTask", method = RequestMethod.POST)
    public CommonResponse checkLabelItemTask(@RequestBody TagItemTask tagItemTask){
        return labelService.checkLabelItemTask(tagItemTask);
    }

    //更新标签任务
    @RequestMapping(value = "updateLabelItemTask", method = RequestMethod.POST)
    public CommonResponse updateLabelItemTask(@RequestBody TagItemTask tagItemTask){
        return labelService.updateLabelItemTask(tagItemTask);
    }

    //更新标签任务的状态
    @RequestMapping(value = "updateLabelItemTaskState", method = RequestMethod.POST)
    public CommonResponse updateLabelItemTaskState(@RequestBody UpdateLabelItemTaskStateReq updateLabelItemTaskStateReq){
        return labelService.updateLabelItemTaskState(updateLabelItemTaskStateReq);
    }

    //更新标签任务的状态
    @RequestMapping(value = "labelItemTaskIsReady", method = RequestMethod.POST)
    public CommonResponse labelItemTaskIsReady(@RequestBody DeleteReq deleteReq){
        return labelService.labelItemTaskIsReady(deleteReq);
    }

    //查询标签任务
    @RequestMapping(value = "queryLabelItemTask", method = RequestMethod.POST)
    public CommonResponse queryLabelItemTask(@RequestBody LabelItemTaskQuery labelItemTaskQuery) {
        return labelService.queryLabelItemTask(labelItemTaskQuery);
    }


    //删除标签任务
    @RequestMapping(value = "deleteLabelItemTask", method = RequestMethod.POST)
    public CommonResponse deleteLabelItemTask(@RequestBody DeleteReq deleteReq){
        return labelService.deleteLabelItemTask(deleteReq);
    }

    //统计标签
    @RequestMapping(value = "queryLabelSummary", method = RequestMethod.POST)
    public CommonResponse queryLabelSummary(@RequestBody LabelSummaryQuery labelSummaryQuery) {
        return labelService.queryLabelSummary(labelSummaryQuery);
    }

}
