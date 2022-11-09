package com.cloudminds.bigdata.dataservice.standard.manage.controller;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Classify;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.EventInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.service.ClassifyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/standard/classify")
public class ClassifyControl {
    @Autowired
    private ClassifyService classifyService;

    //创建分类
    @RequestMapping(value = "add", method = RequestMethod.POST)
    public CommonResponse insertClassify(@RequestBody Classify classify) {
        return classifyService.insertClassify(classify);
    }

    //更新分类名称
    @RequestMapping(value = "update", method = RequestMethod.POST)
    public CommonResponse updateClassify(@RequestBody Classify classify){
        return classifyService.updateClassify(classify);
    }

    //删除分类
    @RequestMapping(value = "delete", method = RequestMethod.POST)
    public CommonResponse deleteClassify(@RequestBody DeleteReq deleteReq){
        return classifyService.deleteClassify(deleteReq.getId());
    }

    //查询分类
    @RequestMapping(value="query",method = RequestMethod.GET)
    public CommonResponse queryClassify(int pid,int type){
        return  classifyService.queryClassify(pid,type);
    }

}
