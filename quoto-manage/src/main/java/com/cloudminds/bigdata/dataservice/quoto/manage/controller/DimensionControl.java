package com.cloudminds.bigdata.dataservice.quoto.manage.controller;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Dimension;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.DimensionObject;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.service.DimensionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/quotoManage/dimension")
public class DimensionControl {

    @Autowired
    private DimensionService dimensionService;

    //新增维度对象
    @RequestMapping(value = "addDimensionObject", method = RequestMethod.POST)
    public CommonResponse addDimensionObject(@RequestBody DimensionObject dimensionObject) {
        return dimensionService.addDimensionObject(dimensionObject);
    }
    //更新维度对象
    @RequestMapping(value = "updateDimensionObject", method = RequestMethod.POST)
    public CommonResponse updateDimensionObject(@RequestBody DimensionObject dimensionObject) {
        return dimensionService.updateDimensionObject(dimensionObject);
    }

    //删除维度对象
    @RequestMapping(value = "deleteDimensionObject", method = RequestMethod.POST)
    public CommonResponse deleteDimensionObject(@RequestBody DeleteReq deleteReq) {
        return dimensionService.deleteDimensionObject(deleteReq);
    }

    //查询维度对象
    @RequestMapping(value = "queryDimensionObject", method = RequestMethod.GET)
    public CommonResponse queryDimensionObject(String name,int page,int size,String order_name,boolean desc) {
        return dimensionService.queryDimensionObject(name,page,size,order_name,desc);
    }

    //查询所有的维度对象
    @RequestMapping(value = "queryAllDimensionObject", method = RequestMethod.GET)
    public CommonResponse queryDimensionObject(String order_name,boolean desc,boolean haveTime) {
        return dimensionService.queryAllDimensionObject(order_name,desc,haveTime);
    }

    //新增维度属性
    @RequestMapping(value = "addDimension", method = RequestMethod.POST)
    public CommonResponse addDimension(@RequestBody Dimension dimension) {
        return dimensionService.addDimension(dimension);
    }

    //更新维度属性
    @RequestMapping(value = "updateDimension", method = RequestMethod.POST)
    public CommonResponse updateDimension(@RequestBody Dimension dimension) {
        return dimensionService.updateDimension(dimension);
    }

    //删除维度属性
    @RequestMapping(value = "deleteDimension", method = RequestMethod.POST)
    public CommonResponse deleteDimension(@RequestBody DeleteReq deleteReq) {
        return dimensionService.deleteDimension(deleteReq);
    }

    //查询维度属性
    @RequestMapping(value = "queryDimension", method = RequestMethod.GET)
    public CommonResponse queryDimension(int dimension_object_id,int page,int size,String order_name,boolean desc) {
        return dimensionService.queryDimension(dimension_object_id,page,size,order_name,desc);
    }

    // 获取所有的维度属性
    @RequestMapping(value = "querySupportDimension", method = RequestMethod.GET)
    public CommonResponse querySupportDimension(int tableId) {
        return dimensionService.querySupportDimension(tableId);
    }


}
