package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Dimension;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.DimensionObject;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DimensionExtend;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.DimensionMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DimensionService {
    @Autowired
    private DimensionMapper dimensionMapper;

    //新增维度对象
    public CommonResponse addDimensionObject(DimensionObject dimensionObject) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(dimensionObject.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度名不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(dimensionObject.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度编码不能为空");
            return commonResponse;
        }
        if (dimensionMapper.queryDimensionObjectByName(dimensionObject.getName()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度名已存在");
            return commonResponse;
        }
        if (dimensionMapper.queryDimensionObjectByCode(dimensionObject.getCode()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度编码已存在");
            return commonResponse;
        }
        if (dimensionMapper.addDimensionObject(dimensionObject) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度创建失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    //更新维度对象
    public CommonResponse updateDimensionObject(DimensionObject dimensionObject) {
        CommonResponse commonResponse = new CommonResponse();
        DimensionObject oldDimensionObject = dimensionMapper.queryDimensionObjectById(dimensionObject.getId());
        if (oldDimensionObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度对象不存在");
            return commonResponse;
        }
        if (StringUtils.isEmpty(dimensionObject.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度名不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(dimensionObject.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度编码不能为空");
            return commonResponse;
        }
        if (!dimensionObject.getName().equals(oldDimensionObject.getName())) {
            if (dimensionMapper.queryDimensionObjectByName(dimensionObject.getName()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("维度名已存在");
                return commonResponse;
            }
        }
        if (!dimensionObject.getCode().equals(oldDimensionObject.getCode())) {
            if (dimensionMapper.queryDimensionObjectByCode(dimensionObject.getCode()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("维度编码已存在");
                return commonResponse;
            }
        }
        if (dimensionMapper.updateDimensionObject(dimensionObject) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度更新失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    //删除维度对象
    public CommonResponse deleteDimensionObject(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq.getIds() == null || deleteReq.getIds().length < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("ids必须有值");
            return commonResponse;
        }
        //校验是否存在维度属性
        List<String> dimensionObjectNames = dimensionMapper.findDimensionObjectName(deleteReq.getIds());
        if (dimensionObjectNames != null && dimensionObjectNames.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(dimensionObjectNames.toString() + " 还存在属性,不能删除");
            return commonResponse;
        }
        try {
            dimensionMapper.deleteDimensionObject(deleteReq.getIds());
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请联系管理员");
            return commonResponse;
        }

        return commonResponse;
    }

    //查询维度对象
    public CommonResponse queryDimensionObject(String name, int page, int size, String order_name, boolean desc) {
        // TODO Auto-generated method stub
        CommonQueryResponse commonResponse = new CommonQueryResponse();
        String condition = "deleted=0";
        if (page < 1 || size < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("page和size必须大于0!");
            return commonResponse;
        }
        if (StringUtils.isEmpty(order_name)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("排序的名字不能为空");
            return commonResponse;
        }
        if (!StringUtils.isEmpty(name)) {
            condition = condition + " and name like '" + name + "%'";
        }

        condition = condition + " order by " + order_name;
        if (desc) {
            condition = condition + " desc";
        } else {
            condition = condition + " asc";
        }
        int startLine = (page - 1) * size;
        commonResponse.setCurrentPage(page);
        commonResponse.setData(dimensionMapper.queryDimensionObject(condition, startLine, size));
        commonResponse.setTotal(dimensionMapper.queryDimensionObjectTotal(condition));
        return commonResponse;
    }

    //新增维度属性
    public CommonResponse addDimension(Dimension dimension) {
        CommonResponse commonResponse = new CommonResponse();
        //判断维度对象是否存在
        if (dimensionMapper.queryDimensionObjectById(dimension.getDimension_object_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度对象不存在");
            return commonResponse;
        }
        //参数校验
        if (StringUtils.isEmpty(dimension.getName()) || StringUtils.isEmpty(dimension.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名字和编码不能为空");
            return commonResponse;
        }
        //判断维度属性名是否重复
        if (dimensionMapper.queryDimensionByName(dimension.getName()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度属性名已存在");
            return commonResponse;
        }

        //判断维度属性编码是否重复
        if (dimensionMapper.queryDimensionByCode(dimension.getDimension_object_id(), dimension.getCode()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度属性编码已存在");
            return commonResponse;
        }

        if (dimensionMapper.addDimension(dimension) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度属性添加失败");
            return commonResponse;
        }

        return commonResponse;
    }

    //更新维度属性
    public CommonResponse updateDimension(Dimension dimension) {
        CommonResponse commonResponse = new CommonResponse();
        Dimension oldDimension = dimensionMapper.queryDimensionById(dimension.getId());
        if (oldDimension == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度属性不存在");
            return commonResponse;
        }
        //参数校验
        if (StringUtils.isEmpty(dimension.getName()) || StringUtils.isEmpty(dimension.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名字和编码不能为空");
            return commonResponse;
        }
        //判断维度属性名是否重复
        if (!oldDimension.getName().equals(dimension.getName())) {
            if (dimensionMapper.queryDimensionByName(dimension.getName()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("维度属性名已存在");
                return commonResponse;
            }
        }

        //判断维度属性编码是否重复
        if (!oldDimension.getCode().equals(dimension.getCode())) {
            if (dimensionMapper.queryDimensionByCode(dimension.getDimension_object_id(), dimension.getCode()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("维度属性编码已存在");
                return commonResponse;
            }
        }

        if (dimensionMapper.updateDimension(dimension) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("维度属性更新失败");
            return commonResponse;
        }

        return commonResponse;
    }

    //删除维度属性
    public CommonResponse deleteDimension(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq.getIds() == null || deleteReq.getIds().length < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("ids必须有值");
            return commonResponse;
        }
        //校验是否有修饰词在使用
        List<String> adjectiveNames = dimensionMapper.queryAdjectiveNameyDimensionIds(deleteReq.getIds());
        if (adjectiveNames != null && adjectiveNames.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(adjectiveNames.toString() + " 修饰词在使用这些维度,不能删除");
            return commonResponse;
        }
        //校验是否有指标在使用
        List<String> quotoNames = dimensionMapper.queryQuotoNamesByDimensionIds(deleteReq.getIds());
        if (quotoNames != null && quotoNames.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(quotoNames.toString() + " 指标在使用这些维度,不能删除");
            return commonResponse;
        }
        try {
            dimensionMapper.deleteDimension(deleteReq.getIds());
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    //查询维度属性
    public CommonResponse queryDimension(int dimension_object_id, int page, int size, String order_name, boolean desc) {
        // TODO Auto-generated method stub
        CommonQueryResponse commonResponse = new CommonQueryResponse();
        String condition = "deleted=0 and dimension_object_id=" + dimension_object_id;
        if (page < 1 || size < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("page和size必须大于0!");
            return commonResponse;
        }
        if (StringUtils.isEmpty(order_name)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("排序的名字不能为空");
            return commonResponse;
        }

        condition = condition + " order by " + order_name;
        if (desc) {
            condition = condition + " desc";
        } else {
            condition = condition + " asc";
        }
        int startLine = (page - 1) * size;
        commonResponse.setCurrentPage(page);
        commonResponse.setData(dimensionMapper.queryDimension(condition, startLine, size));
        commonResponse.setTotal(dimensionMapper.queryDimensionTotal(condition));
        return commonResponse;
    }

    //查询所有的维度对象
    public CommonResponse queryAllDimensionObject(String order_name, boolean desc, boolean haveTime) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "deleted=0";
        if (StringUtils.isEmpty(order_name)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("排序的名字不能为空");
            return commonResponse;
        }
        if (!haveTime) {
            condition = condition + " and code!='time'";
        }
        condition = condition + " order by " + order_name;
        if (desc) {
            condition = condition + " desc";
        } else {
            condition = condition + " asc";
        }
        commonResponse.setData(dimensionMapper.queryAllDimensionObject(condition));
        return commonResponse;
    }

    //查询支持的维度
    public CommonResponse querySupportDimension(int tableId) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        //查询支持的非时间的维度属性
        List<DimensionExtend> supportDimension = new ArrayList<>();
        List<DimensionExtend> notTimeDimesion = dimensionMapper.querySupportDimension(tableId);
        if (notTimeDimesion != null && notTimeDimesion.size() > 0) {
            supportDimension.addAll(notTimeDimesion);
        }

        //查询支持的时间维度属性
        List<ColumnAlias> timeColumnAlias = dimensionMapper.queryTimeColumnByTableId(tableId);
        if(timeColumnAlias!=null && timeColumnAlias.size()>0){
            List<DimensionExtend> timeDimension = dimensionMapper.queryTimeDimension();
            if(timeDimension!=null && timeDimension.size()>0){
                supportDimension.addAll(timeDimension);
            }
        }
        commonResponse.setData(supportDimension);
        return commonResponse;
    }
}
