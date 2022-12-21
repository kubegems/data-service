package com.cloudminds.bigdata.dataservice.label.manage.controller;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagCate;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.label.manage.service.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/tag")
public class TagControl {

    @Autowired
    private TagService tagService;

    //查询所有的标签对象
    @RequestMapping(value = "queryAllTagObject", method = RequestMethod.GET)
    public CommonResponse findAllTagObject() {
        return tagService.queryAllTagObject();
    }

    //查询标签对象的属性
    @RequestMapping(value = "queryTagObjectAttribute", method = RequestMethod.GET)
    public CommonResponse findAllTagObject(int id) {
        return tagService.queryTagObjectAttribute(id);
    }

    //增加分类
    @RequestMapping(value = "addTagCate", method = RequestMethod.POST)
    public CommonResponse insertTagCate(@RequestBody TagCate tagCate){
        return tagService.insertTagCate(tagCate);
    }

    //更新分类名称
    @RequestMapping(value = "updateTagCate", method = RequestMethod.POST)
    public CommonResponse UpdateTagCate(@RequestBody TagCate tagCate){
        return tagService.updateTagCate(tagCate);
    }

    //删除分类
    @RequestMapping(value = "deleteTagCate", method = RequestMethod.POST)
    public CommonResponse deleteTagCate(@RequestBody TagCate tagCate){
        return tagService.deleteTagCate(tagCate);
    }

    //查询分类
    @RequestMapping(value = "queryTagCate", method = RequestMethod.GET)
    public CommonResponse queryTagCate(int tag_object_id,String pid){
        return tagService.queryTagCate(tag_object_id,pid);
    }

    //根据id查询分类
    @RequestMapping(value = "queryTagCateById", method = RequestMethod.GET)
    public CommonResponse queryTagCateById(String id){
        return tagService.queryTagCateById(id);
    }

}
