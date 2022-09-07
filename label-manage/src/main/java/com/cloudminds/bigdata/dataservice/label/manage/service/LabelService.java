package com.cloudminds.bigdata.dataservice.label.manage.service;

import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagCate;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagEnumValue;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagObject;
import com.cloudminds.bigdata.dataservice.label.manage.entity.request.UpdateLabelItemState;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.*;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.LabelItemMapper;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.TagCateMapper;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.TagObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class LabelService {
    @Autowired
    private LabelItemMapper labelItemMapper;
    @Autowired
    private TagCateMapper tagCateMapper;
    @Autowired
    private TagObjectMapper tagObjectMapper;
    @Value("${dataServiceUrl}")
    private String dataServiceUrl;
    @Autowired
    RestTemplate restTemplate;

    // 查询事件详情
    public CommonResponse findLabelItemByPid(String pid, int value) {
        CommonResponse commonResponse = new CommonResponse();
        if (pid == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("pid不能为空");
            return commonResponse;
        }
        //pid 为0 首次查询分类
        if (pid.equals("0")) {
            commonResponse.setData(labelItemMapper.findTagInfoFromCate(pid));
            return commonResponse;
        }
        //pid不为0 value不为0
        if (value > 0) {
            TagItem labelItem = labelItemMapper.findTagItemByTagId(pid);
            if (labelItem == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签不存在");
                return commonResponse;
            }
            //
            if (labelItem.getValue_type() != 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("已经是最后层级了");
                return commonResponse;
            }
            //查询枚举类型
            commonResponse.setData(labelItemMapper.findTagInfoFromTagEnumValue(pid));
            return commonResponse;
        }

        //pid不为0 value为0
        List<TagInfo> taginfos = labelItemMapper.findTagInfoFromCate(pid);
        if (taginfos == null || taginfos.isEmpty()) {
            commonResponse.setData(labelItemMapper.findTagInfoFromTagItem(pid));
        } else {
            commonResponse.setData(taginfos);
        }
        return commonResponse;
    }

    public CommonResponse insertLableItem(TagItem tagItem) {
        CommonResponse commonResponse = new CommonResponse();
        //验证参数是否合法
        if (StringUtils.isEmpty(tagItem.getTag_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签名不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(tagItem.getTag_cate_id())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签类别不能为空");
            return commonResponse;
        }

        TagCate tagCate = tagCateMapper.queryTagCateById(tagItem.getTag_cate_id());
        if (tagCate == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签类别不存在");
            return commonResponse;
        }
        if (tagCate.getPid().equals("0")) {
            List<TagCate> subTagCates = tagCateMapper.queryTagCateByPid(tagItem.getTag_cate_id(), tagCate.getTag_object_id());
            if (subTagCates != null && subTagCates.size() > 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("请选择二级分类");
                return commonResponse;
            }
        }
        if (StringUtils.isEmpty(tagItem.getSource())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签来源不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(tagItem.getTag_type()) || tagItem.getTag_type().length() > 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签类型不能为空,请只能为单字符");
            return commonResponse;
        }

        if (StringUtils.isEmpty(tagItem.getCreator())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("创建者不能为空");
            return commonResponse;
        }

        //判断标签名是否存在
        if (labelItemMapper.findTagItemByTagCateIdAndName(tagItem.getTag_cate_id(), tagItem.getTag_name()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签已存在,请不要重复定义");
            return commonResponse;
        }
        //生成标签id
        String maxTagId = labelItemMapper.findMaxTagIdCode(tagItem.getTag_cate_id());
        if (StringUtils.isEmpty(maxTagId)) {
            maxTagId = "001";
        } else {
            maxTagId = (Integer.parseInt(maxTagId) + 1) + "";
            while (maxTagId.length() < 3) {
                maxTagId = "0" + maxTagId;
            }
        }
        String tagId = tagObjectMapper.queryTagObject(tagCate.getTag_object_id()).getCode() + tagItem.getTag_type();
        if (tagItem.isExclusive()) {
            tagId = tagId + "1";
        } else {
            tagId = tagId + "0";
        }
        if (tagCate.getPid().equals("0")) {
            tagId = tagId + tagCate.getTag_cate_id().substring(tagCate.getTag_cate_id().length() - 3) + "000" + maxTagId;
        } else {
            tagId = tagId + tagCate.getTag_cate_id().substring(tagCate.getTag_cate_id().length() - 6) + maxTagId;
        }
        tagItem.setTag_id(tagId);
        //枚举类型验证参数是否合法,并生成枚举值的id
        if (tagItem.getValue_type() == 1) {
            if (tagItem.getTagEnumValueList() == null || tagItem.getTagEnumValueList().size() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签类型为枚举时,取值范围必须有值");
                return commonResponse;
            }
            Set<String> name = new HashSet<String>();
            for (int i = 0; i < tagItem.getTagEnumValueList().size(); i++) {
                if (StringUtils.isEmpty(tagItem.getTagEnumValueList().get(i).getTag_value())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("枚举值名不能为空");
                    return commonResponse;
                }
                name.add(tagItem.getTagEnumValueList().get(i).getTag_value());
                String tagEnumId = i + 1 + "";
                while (tagEnumId.length() < 3) {
                    tagEnumId = "0" + tagEnumId;
                }
                tagItem.getTagEnumValueList().get(i).setTag_enum_id(tagId + "_" + tagEnumId);
            }
            if (name.size() < tagItem.getTagEnumValueList().size()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("枚举值名不能重复");
                return commonResponse;
            }
        }
        //入库
        if (labelItemMapper.insertTagItem(tagItem) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请联系管理员");
            return commonResponse;
        }
        //枚举类型要入库
        if (tagItem.getValue_type() == 1) {
            if (labelItemMapper.batchSaveTagEnumValue(tagItem.getTagEnumValueList(), tagItem.getTag_id(), tagItem.getCreator()) < 0) {
                commonResponse.setMessage("取值范围插入失败,请联系管理员或者更新标签");
            }
        }
        return commonResponse;
    }

    public CommonResponse updateLabelItem(TagItem tagItem) {
        CommonResponse commonResponse = new CommonResponse();
        boolean changeId = false;
        String newTageItemId = tagItem.getTag_id();
        if (StringUtils.isEmpty(tagItem.getTag_id())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("编码不能为空");
            return commonResponse;
        }
        TagItem oldTagItem = labelItemMapper.findTagItemByTagId(tagItem.getTag_id());
        if (oldTagItem == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始编码不存在");
            return commonResponse;
        }
        if (StringUtils.isEmpty(tagItem.getTag_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签名不能为空");
            return commonResponse;
        }
        List<TagEnumValue> tagEnumValuesInsert = new ArrayList<>();
        List<TagEnumValue> tagEnumValuesUpdate = new ArrayList<>();
        if (tagItem.getValue_type() == 1) {
            int maxCode = 1;
            if (oldTagItem.getValue_type() == 1) {
                maxCode = Integer.parseInt(labelItemMapper.findMaxTagEnumIdCode(tagItem.getTag_id())) + 1;
            }
            if (tagItem.getTagEnumValueList() == null || tagItem.getTagEnumValueList().size() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签类型为枚举时,取值范围必须有值");
                return commonResponse;
            }
            Set<String> name = new HashSet<String>();
            for (int i = 0; i < tagItem.getTagEnumValueList().size(); i++) {
                tagItem.getTagEnumValueList().get(i).setUpdater(tagItem.getUpdater());
                if (StringUtils.isEmpty(tagItem.getTagEnumValueList().get(i).getTag_value())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("枚举值名不能为空");
                    return commonResponse;
                }
                name.add(tagItem.getTagEnumValueList().get(i).getTag_value());
                if (StringUtils.isEmpty(tagItem.getTagEnumValueList().get(i).getTag_enum_id())) {
                    String tagEnumId = maxCode + "";
                    maxCode = maxCode + 1;
                    while (tagEnumId.length() < 3) {
                        tagEnumId = "0" + tagEnumId;
                    }
                    tagItem.getTagEnumValueList().get(i).setTag_enum_id(tagItem.getTag_id() + "_" + tagEnumId);
                    tagEnumValuesInsert.add(tagItem.getTagEnumValueList().get(i));
                } else {
                    TagEnumValue oldTagEnumValue = labelItemMapper.findTagEnumValueByTagEnumId(tagItem.getTagEnumValueList().get(i).getTag_enum_id());
                    if (!oldTagEnumValue.getTag_value().equals(tagItem.getTagEnumValueList().get(i).getTag_value())) {
                        tagEnumValuesUpdate.add(tagItem.getTagEnumValueList().get(i));
                    } else {
                        if (oldTagEnumValue.getDescr() == null) {
                            if (tagItem.getTagEnumValueList().get(i).getDescr() != null) {
                                tagEnumValuesUpdate.add(tagItem.getTagEnumValueList().get(i));
                            } else {
                                if (tagItem.getTagEnumValueList().get(i).getDescr() == null) {
                                    tagEnumValuesUpdate.add(tagItem.getTagEnumValueList().get(i));
                                } else if (!tagItem.getTagEnumValueList().get(i).getDescr().equals(oldTagEnumValue.getDescr())) {
                                    tagEnumValuesUpdate.add(tagItem.getTagEnumValueList().get(i));
                                }
                            }
                        }
                    }
                }

            }
            if (name.size() < tagItem.getTagEnumValueList().size()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("枚举值名不能重复");
                return commonResponse;
            }

        }
        //更改标签名需判断标签名是否存在
        if (!tagItem.getTag_name().equals(oldTagItem.getTag_name())) {
            if (labelItemMapper.findTagItemByTagCateIdAndName(tagItem.getTag_cate_id(), tagItem.getTag_name()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签已存在,请不要重复定义");
                return commonResponse;
            }
        }
        if (StringUtils.isEmpty(tagItem.getSource())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签来源不能为空");
            return commonResponse;
        }

        if (StringUtils.isEmpty(tagItem.getUpdater())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新者不能为空");
            return commonResponse;
        }
        if (oldTagItem.getState() == 11) {
            if (!tagItem.getTag_type().equals(oldTagItem.getTag_type())) {
                changeId = true;
                newTageItemId = newTageItemId.substring(0,newTageItemId.length() - 11)+tagItem.getTag_type()+newTageItemId.substring(newTageItemId.length() - 10);
            }
            if(tagItem.isExclusive()!=oldTagItem.isExclusive()){
                changeId = true;
                String exclusive="0";
                if (tagItem.isExclusive()) {
                    exclusive = "1";
                }
                newTageItemId = newTageItemId.substring(0,newTageItemId.length() - 10)+exclusive+newTageItemId.substring(newTageItemId.length() - 9);
            }
            if(!tagItem.getTag_cate_id().equals(oldTagItem.getTag_cate_id())){
                changeId = true;
                TagCate tagCate = tagCateMapper.queryTagCateById(tagItem.getTag_cate_id());
                if (tagCate == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("标签类别不存在");
                    return commonResponse;
                }
                if (tagCate.getPid().equals("0")) {
                    List<TagCate> subTagCates = tagCateMapper.queryTagCateByPid(tagItem.getTag_cate_id(), tagCate.getTag_object_id());
                    if (subTagCates != null && subTagCates.size() > 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("请选择二级分类");
                        return commonResponse;
                    }
                }
                //生成标签id
                String maxTagId = labelItemMapper.findMaxTagIdCode(tagItem.getTag_cate_id());
                if (StringUtils.isEmpty(maxTagId)) {
                    maxTagId = "001";
                } else {
                    maxTagId = (Integer.parseInt(maxTagId) + 1) + "";
                    while (maxTagId.length() < 3) {
                        maxTagId = "0" + maxTagId;
                    }
                }
                if (tagCate.getPid().equals("0")) {
                    maxTagId = tagCate.getTag_cate_id().substring(tagCate.getTag_cate_id().length() - 3) + "000" + maxTagId;
                } else {
                    maxTagId = tagCate.getTag_cate_id().substring(tagCate.getTag_cate_id().length() - 6) + maxTagId;
                }
                newTageItemId = newTageItemId.substring(0,newTageItemId.length()-9)+maxTagId;
            }
        }
        //更新标签
        if (labelItemMapper.updateTagItem(tagItem,newTageItemId) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败,请联系管理员或稍后再试");
            return commonResponse;
        }
        //更新枚举值
        try {
            if (oldTagItem.getValue_type() == 1) {
                if (tagItem.getValue_type() == 1) {
                    //新增枚举值
                    boolean insertFail = false;
                    if (tagEnumValuesInsert.size() > 0) {
                        if (labelItemMapper.batchSaveTagEnumValue(tagEnumValuesInsert, newTageItemId, tagItem.getUpdater()) < 0) {
                            insertFail = true;
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("枚举值插入失败,请联系管理员或者更新标签");
                        }
                    }
                    //更新枚举值
                    if (tagEnumValuesUpdate.size() > 0) {
                        for (TagEnumValue tagEnumValue : tagEnumValuesUpdate) {
                            if (labelItemMapper.updateTagEnumValue(tagEnumValue) < 0) {
                                commonResponse.setSuccess(false);
                                commonResponse.setMessage("枚举值更新失败,请联系管理员或者更新标签");
                                if (insertFail) {
                                    commonResponse.setMessage("枚举值更新和插入都失败,请联系管理员");
                                }
                                break;
                            }
                        }

                    }
                    //删除枚举值
                    labelItemMapper.deleteTagEnumValue(tagItem.getTagEnumValueList(), newTageItemId, tagItem.getUpdater());
                } else {
                    //删除所有的枚举值
                    if (labelItemMapper.deleteTagEnumValueByTagId(newTageItemId) < 1) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("枚举值删除失败,请联系管理员或者更新标签");
                    }

                }
            } else {
                if (tagItem.getValue_type() == 1) {
                    //新增所有的枚举值
                    if (labelItemMapper.batchSaveTagEnumValue(tagItem.getTagEnumValueList(), newTageItemId, tagItem.getCreator()) < 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("枚举值插入失败,请联系管理员或者更新标签");
                    }
                }
            }
            if(changeId){
                labelItemMapper.updateTagEnumId(newTageItemId);
            }
        } catch (Exception e) {
            System.out.println(e);
            commonResponse.setSuccess(false);
            commonResponse.setMessage("枚举值更新失败,请联系管理员");
        }
        return commonResponse;
    }

    public CommonResponse updateLabelItemState(UpdateLabelItemState updateLabelItemState) {
        CommonResponse commonResponse = new CommonResponse();
        if (updateLabelItemState.getTag_ids() == null || updateLabelItemState.getTag_ids().length == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag_ids不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(updateLabelItemState.getUpdater())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新者不能为空");
            return commonResponse;
        }
        labelItemMapper.updateTagItemState(updateLabelItemState.getTag_ids(), updateLabelItemState.getState(), updateLabelItemState.getUpdater());
        return commonResponse;
    }

    public CommonResponse deleteLabelItem(UpdateLabelItemState updateLabelItemState) {
        CommonResponse commonResponse = new CommonResponse();
        //参数校验
        if (updateLabelItemState.getTag_ids() == null || updateLabelItemState.getTag_ids().length == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag_ids不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(updateLabelItemState.getUpdater())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新者不能为空");
            return commonResponse;
        }
        //判断是否有标签状态为上线中
        String onlineTagName = labelItemMapper.findOnlineTagItemName(updateLabelItemState.getTag_ids());
        if (!StringUtils.isEmpty(onlineTagName)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(onlineTagName + "--为上线状态,请先下线");
            return commonResponse;
        }
        //批量删除标签
        if (labelItemMapper.batchDeleteTagItem(updateLabelItemState.getTag_ids(), updateLabelItemState.getUpdater()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请联系管理员或稍后再试!");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryLabelItem(int tag_object_id, String tag_cate_id, int page, int size, String order_name, boolean desc, String tag_name) {
        CommonQueryResponse commonResponse = new CommonQueryResponse();
        String condition = "c.tag_object_id=" + tag_object_id + " and i.deleted=0";
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
        if (!StringUtils.isEmpty(tag_cate_id)) {
            condition = condition + " and i.tag_cate_id like '" + tag_cate_id + "%'";
        }

        if (!StringUtils.isEmpty(tag_name)) {
            condition = condition + " and i.tag_name like '" + tag_name + "%'";
        }
        condition = condition + " order by i." + order_name;
        if (desc) {
            condition = condition + " desc";
        } else {
            condition = condition + " asc";
        }
        int startLine = (page - 1) * size;
        commonResponse.setCurrentPage(page);
        commonResponse.setData(labelItemMapper.findTagItem(condition, startLine, size));
        commonResponse.setTotal(labelItemMapper.findTagItemCount(condition));
        return commonResponse;
    }

    public CommonResponse dataPreview(String tag_id) {
        DataCommonResponse commonResponse = new DataCommonResponse();
        //查询标签是否存在
        TagItem tagItem = labelItemMapper.findTagItemByTagId(tag_id);
        if (tagItem == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签不存在!");
            return commonResponse;
        }
        //查询标签对象是否存在
        TagObject tagObject = tagObjectMapper.queryTagObjectByTagCateId(tagItem.getTag_cate_id());
        if (tagObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签对象不存在!");
            return commonResponse;
        }

        String url = dataServiceUrl;
        String tag_value_type_name = "string";
        if (tagItem.getValue_type() != 1) {
            tag_value_type_name = "int";
        }
        //限制limit条数
        String querySql = "select tag_value,bitmapCardinality(oids) as cnt from tag.dis_" + tagObject.getCode() + "_tag_" + tag_value_type_name + " where tag_id like '" + tagItem.getTag_id() + "%' limit 10000";
        String bodyRequestTotal = "{\"[]\":{\"" + tagObject.getTable() + "\":{\"@sql\":\"select count(*) from tag.dis_" + tagObject.getTable() + "\"},\"query\":1},\"total@\":\"/[]/total\"}";
        String bodyRequest = "{\"[]\":{\"" + tagObject.getTable() + "\":{\"@sql\":\"" + querySql + "\"},\"page\":0,\"count\":10000}}";
        // 请求数据服务
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("token", "L0V91TZWH4K8YZPBBG3M");
        // 将请求头部和参数合成一个请求查询数据分布情况
        HttpEntity<String> requestEntity = new HttpEntity<>(bodyRequest, headers);
        try {
            ResponseEntity<String> responseEntity = restTemplate.postForEntity(url, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("对应的服务不可用,请联系管理员排查");
                return commonResponse;
            } else {
                JSONObject result = JSONObject.parseObject(responseEntity.getBody().toString());
                DataServiceResponse dataServiceResponse = JSONObject.toJavaObject(
                        JSONObject.parseObject(responseEntity.getBody().toString()), DataServiceResponse.class);
                commonResponse.setSuccess(dataServiceResponse.isOk());
                commonResponse.setMessage(dataServiceResponse.getMsg());
                if (dataServiceResponse.isOk()) {
                    if (result.get("[]") != null) {
                        List<JSONObject> list = JSONObject.parseArray(result.get("[]").toString(), JSONObject.class);
                        if (list != null) {
                            List<Object> data = new ArrayList<Object>();
                            for (int i = 0; i < list.size(); i++) {
                                data.add(list.get(i).get(tagObject.getTable()));
                                commonResponse.setData(data);
                            }
                        }
                    }
                } else {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(dataServiceResponse.getMsg());
                    return commonResponse;
                }
            }

            // 将请求头部和参数合成一个请求查询数据总数情况
            HttpEntity<String> requestEntityTotal = new HttpEntity<>(bodyRequestTotal, headers);
            ResponseEntity<String> responseEntityTotal = restTemplate.postForEntity(url, requestEntityTotal, String.class);
            if (!responseEntityTotal.getStatusCode().is2xxSuccessful()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("数据服务不可用,请联系管理员排查");
                return commonResponse;
            } else {
                JSONObject result = JSONObject.parseObject(responseEntityTotal.getBody().toString());
                DataServiceResponse dataServiceResponse = JSONObject.toJavaObject(
                        JSONObject.parseObject(responseEntityTotal.getBody().toString()), DataServiceResponse.class);
                commonResponse.setSuccess(dataServiceResponse.isOk());
                commonResponse.setMessage(dataServiceResponse.getMsg());
                if (dataServiceResponse.isOk()) {
                    if (result.get("total") != null) {
                        commonResponse.setTotal(dataServiceResponse.getTotal());
                    }
                } else {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(dataServiceResponse.getMsg());
                    return commonResponse;
                }
            }
        } catch (Exception e) {
            System.out.println(e);
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }
}
