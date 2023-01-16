package com.cloudminds.bigdata.dataservice.label.manage.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.label.manage.entity.*;
import com.cloudminds.bigdata.dataservice.label.manage.entity.request.*;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.*;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class LabelService {
    @Autowired
    private LabelItemMapper labelItemMapper;
    @Autowired
    private TagCateMapper tagCateMapper;
    @Autowired
    private TagObjectMapper tagObjectMapper;
    @Autowired
    private TagItemComplexMapper tagItemComplexMapper;
    @Autowired
    private TagItemTaskMapper tagItemTaskMapper;
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
                newTageItemId = newTageItemId.substring(0, newTageItemId.length() - 11) + tagItem.getTag_type() + newTageItemId.substring(newTageItemId.length() - 10);
            }
            if (tagItem.isExclusive() != oldTagItem.isExclusive()) {
                changeId = true;
                String exclusive = "0";
                if (tagItem.isExclusive()) {
                    exclusive = "1";
                }
                newTageItemId = newTageItemId.substring(0, newTageItemId.length() - 10) + exclusive + newTageItemId.substring(newTageItemId.length() - 9);
            }
            if (!tagItem.getTag_cate_id().equals(oldTagItem.getTag_cate_id())) {
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
                newTageItemId = newTageItemId.substring(0, newTageItemId.length() - 9) + maxTagId;
            }
        }
        //更新标签
        if (labelItemMapper.updateTagItem(tagItem, newTageItemId) < 1) {
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
            if (changeId) {
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
        //下线标签 校验是否有组合标签和数据集在用
        if (updateLabelItemState.getState() == 0) {
            String tag_ids = "(";
            for (String tag_id : updateLabelItemState.getTag_ids()) {
                tag_ids = tag_ids + "'" + tag_id + "',";
            }
            tag_ids = tag_ids.substring(0, tag_ids.length() - 1) + ")";
            List<String> onlineTagItemComplex = tagItemComplexMapper.queryOnlineTagItemComplex(tag_ids);
            if (onlineTagItemComplex != null && onlineTagItemComplex.size() > 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("处于上线状态的组合标签使用了这些基础标签,请先下线组合标签：" + StringUtils.join(onlineTagItemComplex, ","));
                return commonResponse;
            }
            //校验有没有数据集在使用,如果数据集在使用不能下线
            List<BaseEntity> dataSet = labelItemMapper.queryDatasetByTagItem(tag_ids);
            if (dataSet != null && dataSet.size() > 0) {
                String message = "以下数据集在使用标签,请联系使用者删除数据集：\n";
                for (int i = 0; i < dataSet.size(); i++) {
                    message = message + dataSet.get(i).getCreator() + ": " + dataSet.get(i).getDescr();
                    if (i != dataSet.size() - 1) {
                        message = message + "\n";
                    }
                }
                commonResponse.setSuccess(false);
                commonResponse.setMessage(message);
                return commonResponse;
            }
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
        //判断是否有组合标签在使用
        String tag_ids = "(";
        for (String tag_id : updateLabelItemState.getTag_ids()) {
            tag_ids = tag_ids + "'" + tag_id + "',";
        }
        tag_ids = tag_ids.substring(0, tag_ids.length() - 1) + ")";
        List<String> useTagItemComplex = tagItemComplexMapper.queryUseTagItemComplex(tag_ids);
        if (useTagItemComplex != null && useTagItemComplex.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("这些组合标签使用了这些基础标签,请先删除组合标签：" + StringUtils.join(useTagItemComplex, ","));
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

    public CommonResponse queryLabelItem(int tag_object_id, String tag_cate_id, int page, int size, String order_name, boolean desc, String tag_name, Integer state) {
        CommonQueryResponse commonResponse = new CommonQueryResponse();
        String condition = "c.tag_object_id=" + tag_object_id + " and i.deleted=0";
        if (state != null && state != -1) {
            condition = condition + " and i.state=" + state;
        }
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

    public CommonResponse queryAllLabelItem(int tag_object_id, Integer state) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "c.tag_object_id=" + tag_object_id + " and i.deleted=0";
        if (state != null && state != -1) {
            condition = condition + " and i.state=" + state;
        }
        commonResponse.setData(labelItemMapper.findAllTagItem(condition));
        return commonResponse;
    }

    public CommonResponse queryLabelItemByTagEnums(LabelItemByTagEnumsQuery labelItemByTagEnumsQuery) {
        CommonResponse commonResponse = new CommonResponse();
        if (labelItemByTagEnumsQuery == null || labelItemByTagEnumsQuery.getTag_enum_ids().length == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag_enum_ids不能为空");
            return commonResponse;
        }
        String tagItems = "(";
        String tagItemEnums = "(";
        for (int i = 0; i < labelItemByTagEnumsQuery.getTag_enum_ids().length; i++) {
            String tagItemEnum = labelItemByTagEnumsQuery.getTag_enum_ids()[i];
            String tagItem = tagItemEnum.substring(0, tagItemEnum.indexOf("_"));
            tagItemEnums = tagItemEnums + "'" + tagItemEnum + "'";
            tagItems = tagItems + "'" + tagItem + "'";
            if (i != labelItemByTagEnumsQuery.getTag_enum_ids().length - 1) {
                tagItemEnums = tagItemEnums + ",";
                tagItems = tagItems + ",";
            }
        }
        tagItems = tagItems + ")";
        tagItemEnums = tagItemEnums + ")";
        List<TagItemExtend> tagItemExtends = labelItemMapper.findTagItemByTagEnums(tagItems);
        if (tagItemExtends != null && tagItemExtends.size() > 0) {
            for (int i = 0; i < tagItemExtends.size(); i++) {
                tagItemExtends.get(i).setTagEnumValueList(labelItemMapper.findTagEnumValueByTagEnums(tagItemExtends.get(i).getTag_id(), tagItemEnums));
            }
        }
        commonResponse.setData(tagItemExtends);
        return commonResponse;
    }

    public CommonResponse queryAllLabelItemNoTask(int tag_object_id) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "c.tag_object_id=" + tag_object_id + " and i.deleted=0";
        commonResponse.setData(labelItemMapper.findAllTagItemNoTask(condition));
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

    public CommonResponse addLabelItemComplex(TagItemComplex tagItemComplex) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (tagItemComplex == null || StringUtils.isEmpty(tagItemComplex.getName()) || StringUtils.isEmpty(tagItemComplex.getFilter())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签组合名称和标签组不能为空");
            return commonResponse;
        }
        //校验名字是否重复
        if (tagItemComplexMapper.findTagItemComplexByName(tagItemComplex.getName(), tagItemComplex.getTag_object_id()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签组合名字已存在");
            return commonResponse;
        }
        //校验tag_object_id是否存在
        if (tagObjectMapper.queryTagObject(tagItemComplex.getTag_object_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签对象不存在");
            return commonResponse;
        }
        //解析出使用的基础标签枚举值
        CommonResponse tmpCommonResponse = analyseFilter(tagItemComplex.getFilter());
        if (!tmpCommonResponse.isSuccess()) {
            return tmpCommonResponse;
        }
        tagItemComplex.setTag_enum_values((String[]) tmpCommonResponse.getData());
        //存入数据库
        if (tagItemComplexMapper.insertTagItemComplex(tagItemComplex) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("新建组合标签失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse analyseFilter(String filter) {
        CommonResponse commonResponse = new CommonResponse();
        JSONArray jsonArray = JSONObject.parseArray(filter);
        if (jsonArray == null || jsonArray.isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("filter必须有值");
            return commonResponse;
        }
        Set<String> data = new HashSet<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject finalJsonObject = jsonArray.getJSONObject(i);
            Boolean finalInOp = true;
            if (finalJsonObject.get("op") != null && finalJsonObject.get("op").toString().toLowerCase().equals("not in")) {
                finalInOp = false;
            }
            if (!finalJsonObject.containsKey("tag_values")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("filter里tag_values必须有值");
                return commonResponse;
            }
            List<String> tagValues = JSONArray.parseArray(finalJsonObject.getString("tag_values"), String.class);
            if (tagValues == null || tagValues.isEmpty()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("filter里tag_values必须有值");
                return commonResponse;
            }
            data.addAll(tagValues);
        }
        commonResponse.setData(data.toArray(new String[data.size()]));
        return commonResponse;
    }

    public CommonResponse updateLabelItemComplex(TagItemComplex tagItemComplex) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (tagItemComplex == null || StringUtils.isEmpty(tagItemComplex.getName()) || StringUtils.isEmpty(tagItemComplex.getFilter())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签组合名称和标签组不能为空");
            return commonResponse;
        }

        TagItemComplex oldTagItemComplex = tagItemComplexMapper.findTagItemComplexById(tagItemComplex.getId());
        if (oldTagItemComplex == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("组合标签不存在");
            return commonResponse;
        }

        //校验名字是否重复
        if (!oldTagItemComplex.getName().equals(tagItemComplex.getName())) {
            if (tagItemComplexMapper.findTagItemComplexByName(tagItemComplex.getName(), oldTagItemComplex.getTag_object_id()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签组合名字已存在");
                return commonResponse;
            }
        }

        //解析出使用的基础标签枚举值
        if (!oldTagItemComplex.getFilter().equals(tagItemComplex.getFilter())) {
            CommonResponse tmpCommonResponse = analyseFilter(tagItemComplex.getFilter());
            if (!tmpCommonResponse.isSuccess()) {
                return tmpCommonResponse;
            }
            tagItemComplex.setTag_enum_values((String[]) tmpCommonResponse.getData());
        } else {
            tagItemComplex.setTag_enum_values(oldTagItemComplex.getTag_enum_values());
        }

        //更新数据库
        if (tagItemComplexMapper.updateTagItemComplex(tagItemComplex) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签组合更新失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteLabelItemComplex(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数
        if (deleteReq == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("请求参数不能为空");
            return commonResponse;
        }
        //校验数据是否存在
        TagItemComplex tagItemComplex = tagItemComplexMapper.findTagItemComplexById(deleteReq.getId());
        if (tagItemComplex == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("组合标签不存在");
            return commonResponse;
        }
        //校验状态
        if (tagItemComplex.getState() == 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("处于上线状态的组合标签不能删除");
            return commonResponse;
        }
        //删除
        if (tagItemComplexMapper.deleteTagItemComplex(deleteReq.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("组合标签删除失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonQueryResponse queryLabelItemComplex(LabelItemComplexQuery labelItemComplexQuery) {
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "deleted=0";
        if (labelItemComplexQuery.getTag_object_id() > 0) {
            condition = condition + " and tag_object_id=" + labelItemComplexQuery.getTag_object_id();
        }
        if (labelItemComplexQuery.getState() != -1) {
            condition = condition + " and state=" + labelItemComplexQuery.getState();
        }
        int page = labelItemComplexQuery.getPage();
        int size = labelItemComplexQuery.getSize();
        int startLine = (page - 1) * size;
        List<TagItemComplexExtend> tagItemComplexExtends = tagItemComplexMapper.queryLabelItemComplex(condition, startLine, size);
        if (tagItemComplexExtends != null && tagItemComplexExtends.size() > 0) {
            for (int i = 0; i < tagItemComplexExtends.size(); i++) {
                tagItemComplexExtends.get(i).setTag_enum_values_list(findTagEnumValueByTagEnumIds(tagItemComplexExtends.get(i).getTag_enum_values()));
            }
        }
        commonQueryResponse.setData(tagItemComplexExtends);
        commonQueryResponse.setCurrentPage(labelItemComplexQuery.getPage());
        commonQueryResponse.setTotal(tagItemComplexMapper.queryLabelItemComplexCount(condition));
        return commonQueryResponse;
    }

    public List<TagEnumValueExtend> findTagEnumValueByTagEnumIds(String[] tag_enum_values) {
        String tagEnumValues = "";
        for (int i = 0; i < tag_enum_values.length; i++) {
            tagEnumValues = tagEnumValues + "'" + tag_enum_values[i] + "'";
            if (i != tag_enum_values.length - 1) {
                tagEnumValues = tagEnumValues + ",";
            }
        }
        return labelItemMapper.findTagEnumValueByTagEnumIds(tagEnumValues);
    }

    public CommonResponse updateLabelItemComplexStatus(int id, int state) {
        CommonResponse commonResponse = new CommonResponse();
        if (state != 0 && state != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持的状态");
            return commonResponse;
        }
        //校验数据是否存在
        TagItemComplex tagItemComplex = tagItemComplexMapper.findTagItemComplexById(id);
        if (tagItemComplex == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("组合标签不存在");
            return commonResponse;
        }
        if (state != tagItemComplex.getState()) {
            //上线的话校验基础标签是否处于上线状态
            if (state == 1) {
                String tag_enum_values = "(";
                for (String tag_enum_value : tagItemComplex.getTag_enum_values()) {
                    TagEnumValueExtend tagEnumValueExtend = labelItemMapper.findTagEnumValueByTagEnumId(tag_enum_value);
                    if (tagEnumValueExtend == null) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage(tag_enum_value + "不存在了,请重新编辑组合标签");
                        return commonResponse;
                    }
                    tag_enum_values = tag_enum_values + "'" + tag_enum_value + "',";
                }
                tag_enum_values = tag_enum_values.substring(0, tag_enum_values.length() - 1) + ")";
                List<String> unOnlineLableItem = tagItemComplexMapper.queryUnOnlineLableItem(tag_enum_values);
                if (unOnlineLableItem != null && unOnlineLableItem.size() > 0) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("使用到的" + String.join(",", unOnlineLableItem) + "这些基础标签需是上线状态");
                    return commonResponse;
                }
            } else {
                //校验有没有数据集在使用,如果数据集在使用不能下线
                List<BaseEntity> dataSet = tagItemComplexMapper.queryDatasetByTagItemComplex(tagItemComplex.getName(), tagItemComplex.getTag_object_id());
                if (dataSet != null && dataSet.size() > 0) {
                    String message = "以下数据集在使用这个组合标签,请联系使用者删除数据集：\n";
                    for (int i = 0; i < dataSet.size(); i++) {
                        message = message + dataSet.get(i).getCreator() + ": " + dataSet.get(i).getDescr();
                        if (i != dataSet.size() - 1) {
                            message = message + "\n";
                        }
                    }
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(message);
                    return commonResponse;
                }

            }
        }
        //更新状态
        if (tagItemComplexMapper.updateTagItemComplexState(id, state) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败,请联系管理员");
            return commonResponse;
        }

        return commonResponse;
    }

    public CommonResponse addLabelItemTask(TagItemTask tagItemTask) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (tagItemTask == null || StringUtils.isEmpty(tagItemTask.getTag_id()) || StringUtils.isEmpty(tagItemTask.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签和任务名不能为空");
            return commonResponse;
        }
        //校验标签是否存在
        if (labelItemMapper.findTagItemByTagId(tagItemTask.getTag_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签不存在");
            return commonResponse;
        }
        //验证标签对象是否存在
        if (tagObjectMapper.queryTagObject(tagItemTask.getTag_object_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签对象不存在");
            return commonResponse;
        }
        //校验任务是不是已经存在
        TagItemTask oldTagItemTask = tagItemTaskMapper.findTagItemTaskByTagId(tagItemTask.getTag_id());
        if (oldTagItemTask != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该标签下已经存在任务：" + oldTagItemTask.getName() + " 不能再创建了");
            return commonResponse;
        }

        //规则类别参数校验
        if (tagItemTask.getTag_rule_type() == 1) {
            if (StringUtils.isEmpty(tagItemTask.getTag_rule())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则类别为sql时,标签规则不能为空");
                return commonResponse;
            }
        } else {
            if (StringUtils.isEmpty(tagItemTask.getMain_class()) || StringUtils.isEmpty(tagItemTask.getJar_package())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则类别为model时,程序入口和jar包路径不能为空");
                return commonResponse;
            }

        }
        //插入数据
        if (tagItemTaskMapper.insertTagItemTask(tagItemTask) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("新建任务失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse checkLabelItemTask(TagItemTask tagItemTask) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (tagItemTask == null || StringUtils.isEmpty(tagItemTask.getTag_id()) || StringUtils.isEmpty(tagItemTask.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签和任务名不能为空");
            return commonResponse;
        }
        //校验标签是否存在
        if (labelItemMapper.findTagItemByTagId(tagItemTask.getTag_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签不存在");
            return commonResponse;
        }
        //验证标签对象是否存在
        if (tagObjectMapper.queryTagObject(tagItemTask.getTag_object_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签对象不存在");
            return commonResponse;
        }
        //校验任务是不是已经存在
        TagItemTask oldTagItemTask = tagItemTaskMapper.findTagItemTaskByTagId(tagItemTask.getTag_id());
        if (oldTagItemTask != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该标签下已经存在任务：" + oldTagItemTask.getName() + " 不能再创建了");
            return commonResponse;
        }

        //规则类别参数校验
        if (tagItemTask.getTag_rule_type() == 1) {
            if (StringUtils.isEmpty(tagItemTask.getTag_rule())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则类别为sql时,标签规则不能为空");
                return commonResponse;
            }
        } else {
            if (StringUtils.isEmpty(tagItemTask.getMain_class()) || StringUtils.isEmpty(tagItemTask.getJar_package())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则类别为model时,程序入口和jar包路径不能为空");
                return commonResponse;
            }

        }
        return commonResponse;
    }

    public CommonResponse updateLabelItemTask(TagItemTask tagItemTask) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (tagItemTask == null || StringUtils.isEmpty(tagItemTask.getTag_id()) || StringUtils.isEmpty(tagItemTask.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签和任务名不能为空");
            return commonResponse;
        }
        TagItemTask oldTagItemTask = tagItemTaskMapper.findTagItemTaskById(tagItemTask.getId());
        if (oldTagItemTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签任务不存在");
            return commonResponse;
        }
        //校验标签是否存在
        if (!oldTagItemTask.getTag_id().equals(tagItemTask.getTag_id())) {
            if (labelItemMapper.findTagItemByTagId(tagItemTask.getTag_id()) == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签不存在");
                return commonResponse;
            }
            //校验任务是不是已经存在
            TagItemTask tagItemTask1 = tagItemTaskMapper.findTagItemTaskByTagId(tagItemTask.getTag_id());
            if (tagItemTask1 != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("该标签下已经存在任务：" + tagItemTask1.getName() + " 不能再有新任务了");
                return commonResponse;
            }
        }

        //规则类别参数校验
        if (tagItemTask.getTag_rule_type() == 1) {
            if (StringUtils.isEmpty(tagItemTask.getTag_rule())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则类别为sql时,标签规则不能为空");
                return commonResponse;
            }
        } else {
            if (StringUtils.isEmpty(tagItemTask.getMain_class()) || StringUtils.isEmpty(tagItemTask.getJar_package())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则类别为model时,程序入口和jar包路径不能为空");
                return commonResponse;
            }

        }
        //更新任务
        if (tagItemTaskMapper.updateTagItemTask(tagItemTask) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务更新失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteLabelItemTask(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数
        if (deleteReq == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("请求参数不能为空");
            return commonResponse;
        }
        //校验数据是否存在
        TagItemTask tagItemTask = tagItemTaskMapper.findTagItemTaskById(deleteReq.getId());
        if (tagItemTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务不存在");
            return commonResponse;
        }
        //校验状态
        if (tagItemTask.getState() == 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("处于运行状态的任务不能删除");
            return commonResponse;
        }
        //删除
        if (tagItemTaskMapper.deleteTagItemTask(deleteReq.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务删除失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryLabelItemTask(LabelItemTaskQuery labelItemTaskQuery) {
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "t.deleted=0";
        if (labelItemTaskQuery.getState() != -1) {
            condition = condition + " and t.state=" + labelItemTaskQuery.getState();
        }
        if (labelItemTaskQuery.getTag_object_id() > 0) {
            condition = condition + " and t.tag_object_id=" + labelItemTaskQuery.getTag_object_id();
        }
        if (!StringUtils.isEmpty(labelItemTaskQuery.getQueryValue())) {
            condition = condition + " and (t.name like '" + labelItemTaskQuery.getQueryValue() + "%' or t.tag_id like '" + labelItemTaskQuery.getQueryValue() + "%' or l.tag_name like '" + labelItemTaskQuery.getQueryValue() + "%')";
        }
        condition = condition + " order by t.update_time desc";
        int page = labelItemTaskQuery.getPage();
        int size = labelItemTaskQuery.getSize();
        int startLine = (page - 1) * size;
        commonQueryResponse.setData(tagItemTaskMapper.queryTagItemTask(condition, startLine, size));
        commonQueryResponse.setCurrentPage(labelItemTaskQuery.getPage());
        commonQueryResponse.setTotal(tagItemTaskMapper.queryTagItemTaskCount(condition));
        return commonQueryResponse;
    }

    public CommonResponse updateLabelItemTaskState(UpdateLabelItemTaskStateReq updateLabelItemTaskStateReq) {
        CommonResponse commonResponse = new CommonResponse();
        TagItemTask tagItemTask = tagItemTaskMapper.findTagItemTaskById(updateLabelItemTaskStateReq.getId());
        if (tagItemTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务不存在");
            return commonResponse;
        }
        if (updateLabelItemTaskStateReq.getState() == 1) {
            if (labelItemMapper.findTagItemByTagId(tagItemTask.getTag_id()).getState() != 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("请先上线基础标签");
                return commonResponse;
            }
        }
        //更新
        if (tagItemTaskMapper.updateTagItemTaskState(updateLabelItemTaskStateReq.getId(), updateLabelItemTaskStateReq.getState(), updateLabelItemTaskStateReq.getRun_info()) < 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务更新失败,请稍后再试");
            return commonResponse;
        }

        return commonResponse;
    }

    public CommonResponse labelItemTaskIsReady(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        TagItemTask tagItemTask = tagItemTaskMapper.findTagItemTaskById(deleteReq.getId());
        if (tagItemTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务不存在");
            return commonResponse;
        }
        TagItem tagItem = labelItemMapper.findTagItemByTagId(tagItemTask.getTag_id());
        if (tagItem == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务异常：基础标签已经被删了");
            return commonResponse;
        }
        if (tagItem.getState() != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("请先上线基础标签");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryLabelSummary(LabelSummaryQuery labelSummaryQuery) {
        CommonResponse commonResponse = new CommonResponse();
        if (labelSummaryQuery.getQuery_type() == 1) {
            commonResponse.setData(labelItemMapper.findTagItemSumaryByObject());
            return commonResponse;
        } else if (labelSummaryQuery.getQuery_type() == 2) {
            commonResponse.setData(labelItemMapper.findTagItemSumaryByObjectAndState());
            return commonResponse;
        } else if (labelSummaryQuery.getQuery_type() == 3) {
            commonResponse.setData(labelItemMapper.findTagItemSumaryByCate(labelSummaryQuery.getTag_object_id()));
            return commonResponse;
        } else if (labelSummaryQuery.getQuery_type() == 4) {
            commonResponse.setData(labelItemMapper.findTagItemSumaryByCateAndState(labelSummaryQuery.getTag_object_id()));
            return commonResponse;
        } else if (labelSummaryQuery.getQuery_type() == 5) {

        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持的查询类型");
            return commonResponse;
        }
        return commonResponse;
    }
}
