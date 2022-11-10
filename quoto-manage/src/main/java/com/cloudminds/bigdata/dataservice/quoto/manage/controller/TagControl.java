package com.cloudminds.bigdata.dataservice.quoto.manage.controller;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.QuotoTag;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Tag;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.service.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/quotoManage/tag")
public class TagControl {
    @Autowired
    private TagService tagService;

    // 创建tag
    @RequestMapping(value = "add", method = RequestMethod.POST)
    public CommonResponse insertTag(@RequestBody Tag tag) {
        return tagService.insertTag(tag);
    }

    //更新tag
    @RequestMapping(value = "update", method = RequestMethod.POST)
    public CommonResponse updateTag(@RequestBody Tag tag) {
        return tagService.updateTag(tag);
    }

    //删除tag
    @RequestMapping(value = "delete", method = RequestMethod.POST)
    public CommonResponse deleteTag(@RequestBody DeleteReq deleteReq) {
        return tagService.deleteTag(deleteReq.getId());
    }

    //查该用户可用的tag
    @RequestMapping(value = "query", method = RequestMethod.GET)
    public CommonResponse queryTag(String creator) {
        return tagService.queryTag(creator);
    }

    //给指标打tag
    @RequestMapping(value = "quotoAddTag", method = RequestMethod.POST)
    public CommonResponse quotoAddTag(@RequestBody QuotoTag quotoTag) {
        return tagService.quotoAddTag(quotoTag);
    }

    //删除指标的某个tag
    @RequestMapping(value = "quotoRemoveTag", method = RequestMethod.POST)
    public CommonResponse quotoRemoveTag(@RequestBody QuotoTag quotoTag) {
        return tagService.quotoRemoveTag(quotoTag.getTag_id(),quotoTag.getQuoto_id());
    }
}
