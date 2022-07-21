package com.cloudminds.bigdata.dataservice.label.manage.controller;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import com.cloudminds.bigdata.dataservice.label.manage.entity.request.UpdateLabelItemState;
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
    public CommonResponse queryLabelItem(int tag_object_id) {
        return labelService.queryLabelItem(tag_object_id);
    }

    //查询标签
    @RequestMapping(value = "dataPreview", method = RequestMethod.GET)
    public CommonResponse dataPreview(String tag_id) {
        return labelService.dataPreview(tag_id);
    }
}
