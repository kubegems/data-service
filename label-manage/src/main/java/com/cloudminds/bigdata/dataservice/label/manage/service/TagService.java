package com.cloudminds.bigdata.dataservice.label.manage.service;

import com.cloudminds.bigdata.dataservice.label.manage.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagCate;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagObject;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.TagInfo;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.LabelItemMapper;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.TagCateMapper;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.TagObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TagService {
    @Autowired
    private TagObjectMapper tagObjectMapper;

    @Autowired
    private TagCateMapper tagCateMapper;

    @Autowired
    private LabelItemMapper labelItemMapper;

    @Value("${metaJdbcConnet}")
    private String metaJdbcConnet;

    @Value("${metaJdbcUser}")
    private String metaJdbcUser;

    @Value("${metaJdbcPassword}")
    private String metaJdbcPassword;

    public CommonResponse queryAllTagObject() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tagObjectMapper.queryAllTagObject());
        return commonResponse;
    }

    /*public CommonResponse queryTagObjectAttribute(int id) {
        CommonResponse commonResponse = new CommonResponse();
        TagObject tagObject = tagObjectMapper.queryTagObject(id);
        if (tagObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("对象属性不存在!");
            return commonResponse;
        }
        List<Map<String, String>> data = new ArrayList<>();
        List<ColumnAlias> columnAliases = tagObjectMapper.queryTagObjectColunmAttribute(tagObject.getDatabase(), tagObject.getTable());
        if (columnAliases == null || columnAliases.size() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(tagObject.getDatabase() + "." + tagObject.getTable() + " 对应的表没在数据服务里配置,请联系管理员");
            return commonResponse;
        } else {
            for (ColumnAlias columnAlias : columnAliases) {
                Map<String, String> dataMap = new HashMap<>();
                dataMap.put("name", columnAlias.getColumn_name());
                dataMap.put("type", columnAlias.getData_type());
                dataMap.put("comment", columnAlias.getDescr());
                data.add(dataMap);
            }
        }
        commonResponse.setData(data);
        return commonResponse;
    }*/

    public CommonResponse insertTagCate(TagCate tagCate) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(tagCate.getTag_cate_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签名不能为空！");
            return commonResponse;
        }
        TagObject tagObject = tagObjectMapper.queryTagObject(tagCate.getTag_object_id());
        if (tagObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签对象不存在！");
            return commonResponse;
        }
        if (StringUtils.isEmpty(tagCate.getPid())) {
            tagCate.setPid("0");
        }

        if (!tagCate.getPid().equals("0")) {
            TagCate parentTagCate = tagCateMapper.queryTagCateById(tagCate.getPid());
            if (parentTagCate == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("一级标签不存在！");
                return commonResponse;
            }
            if (!parentTagCate.getPid().equals("0")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("不支持三级分类的标签哦！");
                return commonResponse;
            }
        }

        if (tagCateMapper.queryTagCateByPidAndName(tagCate.getPid(), tagCate.getTag_cate_name()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("此标签已经存在,请不要重复定义！");
            return commonResponse;
        }
        //生成id编码
        String maxId = tagCateMapper.queryMaxId(tagCate.getPid(), tagCate.getTag_object_id());
        if (StringUtils.isEmpty(maxId)) {
            if (tagCate.getPid().equals("0")) {
                tagCate.setTag_cate_id(tagObject.getCode() + "001");
            } else {
                tagCate.setTag_cate_id(tagCate.getPid() + "001");
            }
        } else {
            String code = maxId.substring(maxId.length() - 3);
            code = (Integer.parseInt(code) + 1) + "";
            while (code.length() < 3) {
                code = "0" + code;
            }
            if (tagCate.getPid().equals("0")) {
                tagCate.setTag_cate_id(tagObject.getCode() + code);
            } else {
                tagCate.setTag_cate_id(tagCate.getPid() + code);
            }
        }
        //入库
        if (tagCateMapper.insertTagCate(tagCate) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("新增标签失败,请联系管理员！");
            return commonResponse;
        }
        commonResponse.setData(tagCate.getTag_cate_id());
        return commonResponse;
    }

    public CommonResponse updateTagCate(TagCate tagCate) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(tagCate.getTag_cate_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签名不能为空！");
            return commonResponse;
        }
        TagCate oldTagCate = tagCateMapper.queryTagCateById(tagCate.getTag_cate_id());
        if (oldTagCate == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始标签不存在！");
            return commonResponse;
        }
        if(StringUtils.isEmpty(tagCate.getDescr())){
            tagCate.setDescr(oldTagCate.getDescr());
        }
        if (!tagCate.getTag_cate_name().equals(oldTagCate.getTag_cate_name())) {
            if (tagCateMapper.queryTagCateByPidAndName(oldTagCate.getPid(), tagCate.getTag_cate_name()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签名已经存在,请不要重复定义！");
                return commonResponse;
            }
        }
        if (tagCateMapper.updateTagCateName(tagCate.getTag_cate_id(), tagCate.getTag_cate_name(),tagCate.getDescr()) < 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新标签名称失败,请稍好再试！");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteTagCate(TagCate tagCate) {
        CommonResponse commonResponse = new CommonResponse();
        TagCate oldTagCate = tagCateMapper.queryTagCateById(tagCate.getTag_cate_id());
        if (oldTagCate == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始标签不存在！");
            return commonResponse;
        }
        List<TagCate> sonTagCates = tagCateMapper.queryTagCateByPid(tagCate.getTag_cate_id(),oldTagCate.getTag_object_id());
        if (sonTagCates != null && sonTagCates.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("此标签下面有子标签,不能被删除！");
            return commonResponse;
        }

        List<TagInfo> tagInfos = labelItemMapper.findTagInfoFromTagItem(oldTagCate.getTag_cate_id());
        if (tagInfos != null && tagInfos.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标签列里有引用此标签,不能被删除！");
            return commonResponse;
        }
        if (tagCateMapper.deleteTagCate(oldTagCate.getTag_cate_id()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请稍后再试！");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryTagCate(int tag_object_id, String pid) {
        CommonResponse commonResponse = new CommonResponse();
        if (pid != null) {
            commonResponse.setData(tagCateMapper.queryTagCateByPid(pid,tag_object_id));
        } else {
            commonResponse.setData(tagCateMapper.queryTagCateBTagObjectId(tag_object_id));
        }
        return commonResponse;
    }

    public CommonResponse queryTagCateById(String id) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tagCateMapper.queryTagCateById(id));
        return commonResponse;
    }
}
