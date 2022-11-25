package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums.StateEnum;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums.TypeEnum;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.QuotoQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.*;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.AdjectiveMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.DimensionMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoAccessHistoryMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.utils.DateTimeUtils;
import com.cloudminds.bigdata.dataservice.quoto.manage.utils.QuotoCaculateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class QuotoService {
    @Autowired
    private QuotoMapper quotoMapper;
    @Autowired
    private QuotoAccessHistoryMapper quotoAccessHistoryMapper;
    @Value("${dataServiceUrl}")
    private String dataServiceUrl;
    @Autowired
    private DimensionMapper dimensionMapper;
    @Autowired
    private AdjectiveMapper adjectiveMapper;
    @Autowired
    RestTemplate restTemplate;

    @SuppressWarnings("deprecation")
    public CommonResponse checkUnique(CheckReq checkReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        byte flag = checkReq.getCheckflag();
        Quoto quoto = null;
        if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("check的值不能为空");
            return commonResponse;
        }
        if (flag == 0) {
            quoto = quotoMapper.findQuotoByName(checkReq.getCheckValue());
        } else if (flag == 1) {
            String checkValue = checkReq.getCheckValue();
            if (NumberUtils.isNumber(checkValue) || checkValue.contains("(") || checkValue.contains(")")
                    || checkValue.contains("+") || checkValue.contains("-") || checkValue.contains("*")
                    || checkValue.contains("/") || checkValue.contains("#") || checkValue.contains("&")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("编码不能是数或者含有()+-*/&#特殊符号");
                return commonResponse;
            }
            quoto = quotoMapper.findQuotoByField(checkReq.getCheckValue());
        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持check的类型");
            return commonResponse;
        }
        if (quoto != null) {
            commonResponse.setSuccess(false);
            commonResponse.setData(quoto);
            commonResponse.setMessage("已存在,请重新命名");
        }
        return commonResponse;
    }

    public CommonResponse queryAllBusiness(int pid) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (pid == -1) {
            commonResponse.setData(quotoMapper.queryAllBusiness());
        } else {
            commonResponse.setData(quotoMapper.queryAllBusinessByPid(pid));
        }
        return commonResponse;
    }


    public synchronized CommonResponse addBusiness(Business business) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(business.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线名字不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(business.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("code编码不能为空");
            return commonResponse;
        }
        if (quotoMapper.queryBusinessByCode(business.getCode()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("编码已存在,请重新命名");
            return commonResponse;
        }

        if (quotoMapper.queryBusiness(business.getName(), business.getPid()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("此业务线已存在,请不要重复添加");
            return commonResponse;
        }
        if (business.getPid() > 0) {
            Business pidBusiness = quotoMapper.queryBusinessById(business.getPid());
            if (pidBusiness == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("父业务线不存在");
                return commonResponse;
            }
            if (pidBusiness.getPid() > 0) {
                Business pidPidBusiness = quotoMapper.queryBusinessById(pidBusiness.getPid());
                if (pidPidBusiness.getPid() > 0) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("三级业务线下不能再添加业务线");
                    return commonResponse;
                }

            }
        } else {
            business.setPid(0);
        }
        try {
            quotoMapper.addBusiness(business);
            commonResponse.setData(business);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public synchronized CommonResponse updateBusiness(Business business) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        Business oldBusiness = quotoMapper.queryBusinessById(business.getId());
        if (oldBusiness == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线不存在");
            return commonResponse;
        }
        if (StringUtils.isEmpty(business.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线名字不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(business.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("code编码不能为空");
            return commonResponse;
        }
        if (!business.getCode().equals(oldBusiness.getCode())) {
            if (quotoMapper.queryBusinessByCode(business.getCode()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("编码已存在,请重新命名");
                return commonResponse;
            }
        }

        if (!business.getName().equals(oldBusiness.getName())) {
            if (quotoMapper.queryBusiness(business.getName(), business.getPid()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("此业务线已存在,请不要重复添加");
                return commonResponse;
            }
        }
        try {
            quotoMapper.updateBusiness(business);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据更新失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteBusiness(DeleteReq deleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        //校验参数
        if (deleteReq == null || deleteReq.getId() <= 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("id值不能为空");
            return commonResponse;
        }
        //查询业务线
        Business business = quotoMapper.queryBusinessById(deleteReq.getId());
        if (business == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线不存在");
            return commonResponse;
        }

        //查询子业务线
        if (business.getPid() == 0) {
            List<Business> subBusiness = quotoMapper.queryBusinessByPid(deleteReq.getId());
            if (subBusiness != null && subBusiness.size() > 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("该业务线下存在子业务线,不能删除");
                return commonResponse;
            }
        }

        //查询主题
        int id = deleteReq.getId();
        List<Theme> themes = quotoMapper.queryThemeByBusinessId(id);
        if (themes != null && themes.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线下有主题,不能删除");
            return commonResponse;
        }
        if (quotoMapper.deleteBusinessById(id) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse queryTheme(Integer business_id, String search_key, int page, int size, String order_name, boolean desc) {
        // TODO Auto-generated method stub
        CommonQueryResponse commonResponse = new CommonQueryResponse();
        String condition = "t.deleted=0";
        if (business_id != null) {
            Business business = quotoMapper.queryBusinessById(business_id);
            if (business == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("业务线不存在!");
                return commonResponse;
            }
            //输入的第一级业务线
            if (business.getPid() == 0) {
                condition = condition + " and bbb.id=" + business.getId();
            } else {
                Business pidBusiness = quotoMapper.queryBusinessById(business.getPid());
                //输入的是第二级业务线
                if (pidBusiness.getPid() == 0) {
                    condition = condition + " and bb.id=" + business.getId();
                } else {
                    //第三级业务线
                    condition = condition + " and b.id=" + business.getId();
                }
            }
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
        if (!StringUtils.isEmpty(search_key)) {
            condition = condition + " and t.name like '" + search_key + "%' or t.en_name like '" + search_key + "%' or t.code like '" + search_key + "%'";
        }

        condition = condition + " order by t." + order_name;
        if (desc) {
            condition = condition + " desc";
        } else {
            condition = condition + " asc";
        }
        int startLine = (page - 1) * size;
        commonResponse.setCurrentPage(page);
        commonResponse.setData(quotoMapper.queryTheme(condition, startLine, size));
        commonResponse.setTotal(quotoMapper.queryThemeTotal(condition));
        return commonResponse;
    }

    public CommonResponse queryAllTheme() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryAllTheme());
        return commonResponse;
    }

    public synchronized CommonResponse addTheme(Theme theme) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(theme.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题名字不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(theme.getEn_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题英文名字不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(theme.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题编码不能为空");
            return commonResponse;
        }
        if (quotoMapper.queryThemeByName(theme.getName()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题名已存在");
            return commonResponse;
        }
        if (quotoMapper.queryThemeByCode(theme.getCode()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题编码已存在");
            return commonResponse;
        }
        if (quotoMapper.queryThemeByEnName(theme.getEn_name()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题英文名已存在");
            return commonResponse;
        }
        Business business = quotoMapper.queryBusinessById(theme.getBusiness_id());
        if (business == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线不存在");
            return commonResponse;
        }
        //此处是一级业务线下
        if (business.getPid() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题只能加在三级业务线下");
            return commonResponse;
        }
        Business pidBusiness = quotoMapper.queryBusinessById(business.getPid());

        //此处是二级业务线下
        if (pidBusiness.getPid() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题只能加在三级业务线下");
            return commonResponse;
        }

        try {
            quotoMapper.addTheme(theme);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public synchronized CommonResponse updateTheme(Theme theme) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        Theme oldTheme = quotoMapper.queryThemeById(theme.getId());
        if (oldTheme == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题不存在");
            return commonResponse;
        }
        if (StringUtils.isEmpty(theme.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题名字不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(theme.getEn_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题英文名字不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(theme.getCode())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题编码不能为空");
            return commonResponse;
        }
        if (!oldTheme.getName().equals(theme.getName())) {
            if (quotoMapper.queryThemeByName(theme.getName()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("主题名已存在");
                return commonResponse;
            }
        }
        if (!oldTheme.getCode().equals(theme.getCode())) {
            if (quotoMapper.queryThemeByCode(theme.getCode()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("主题编码已存在");
                return commonResponse;
            }
        }
        if (!oldTheme.getEn_name().equals(theme.getEn_name())) {
            if (quotoMapper.queryThemeByEnName(theme.getEn_name()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("主题英文名已存在");
                return commonResponse;
            }
        }
        Business business = quotoMapper.queryBusinessById(theme.getBusiness_id());
        if (business == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线不存在");
            return commonResponse;
        }
        if (business.getPid() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题只能加在二级业务线下");
            return commonResponse;
        }

        try {
            quotoMapper.updateTheme(theme);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据更新失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteTheme(DeleteReq deleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq == null || deleteReq.getId() <= 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("id值不能为空");
            return commonResponse;
        }
        int id = deleteReq.getId();
        Theme theme = quotoMapper.queryThemeById(id);
        if (theme == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题不存在");
            return commonResponse;
        }
        //查询业务过程
        List<BusinessProcess> businessProcess = quotoMapper.queryAllBusinessProcess(id);
        if (businessProcess != null && businessProcess.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据域下有业务过程,不能删除");
            return commonResponse;
        }

        //查询指标
        List<Quoto> quotos = quotoMapper.queryQuotoByTheme(id);
        if (quotos != null && quotos.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题下有指标,不能删除");
            return commonResponse;
        }
        //查询表
        List<TableInfo> tableInfos = quotoMapper.queryTableByTheme(id);
        if (tableInfos != null && tableInfos.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题下有表,不能删除");
            return commonResponse;
        }
        if (quotoMapper.deleteThemeById(id) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse queryAllBusinessProcess(int dataDomainId) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryAllBusinessProcess(dataDomainId));
        return commonResponse;
    }

    public synchronized CommonResponse addBusinessProcess(BusinessProcess businessProcess) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(businessProcess.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务过程名字不能为空");
            return commonResponse;
        }
        if (quotoMapper.queryBusinessProcess(businessProcess.getName(), businessProcess.getTheme_id()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务过程已存在,请不要重复添加");
            return commonResponse;
        }
        try {
            quotoMapper.addBusinessProcess(businessProcess);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteBusinessProcess(DeleteReq deleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq == null || deleteReq.getId() <= 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("id值不能为空");
            return commonResponse;
        }
        int id = deleteReq.getId();
        //查询指标
        List<Quoto> quotos = quotoMapper.queryQuotoByBusinessProcess(id);
        if (quotos != null && quotos.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务过程下有指标,不能删除");
            return commonResponse;
        }
        if (quotoMapper.deleteBusinessProcess(id) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }


    //查询主题下支持的宽表
    public CommonResponse queryAllDataService(Integer theme_id) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (theme_id == null) {
            commonResponse.setData(quotoMapper.queryAllDataService());
        } else {
            commonResponse.setData(quotoMapper.queryAllDataServiceByThemeId(theme_id));
        }
        return commonResponse;
    }

    //// 获取表下还没被使用的指标信息
    public CommonResponse queryUsableQuotoInfoByTableId(int tableId) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryUsableQuotoInfoByTableId(tableId));
        return commonResponse;
    }

    //查询主题下支持的宽表
    public CommonResponse queryTimeColunm(int tableId) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(dimensionMapper.queryTimeColumnByTableId(tableId));
        return commonResponse;
    }


    public CommonResponse deleteQuoto(DeleteReq deleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        // 查询指标
        Quoto quoto = quotoMapper.findQuotoById(deleteReq.getId());
        if (quoto == null) {
            commonResponse.setMessage("id为：" + deleteReq.getId() + "的指标不存");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        // 指标是原始指标，是否有被派生指标引用
        if (quoto.getType() == TypeEnum.atomic_quoto.getCode()) {
            List<String> quotoNames = quotoMapper.findQuotoNameByOriginQuoto(deleteReq.getId());
            if (quotoNames != null && quotoNames.size() > 0) {
                commonResponse.setMessage("衍生指标" + quotoNames.toString() + "在使用(" + quoto.getName() + ")指标,不能删除");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        // 指标有没有被复合指标使用
        List<String> quotoNames = quotoMapper.findQuotoNameByContainQuotoId(quoto.getId());
        if (quotoNames != null && quotoNames.size() > 0) {
            commonResponse.setMessage("衍生指标" + quotoNames.toString() + "在使用(" + quoto.getName() + ")指标,不能删除");
            commonResponse.setSuccess(false);
            return commonResponse;
        }

        if (quotoMapper.deleteQuotoById(deleteReq.getId()) <= 0) {
            commonResponse.setMessage("指标(" + quoto.getName() + ")删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse batchDeleteQuoto(BatchDeleteReq batchDeleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (batchDeleteReq.getIds() == null || batchDeleteReq.getIds().length == 0) {
            commonResponse.setMessage("删除的指标id不能为空");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        for (int i = 0; i < batchDeleteReq.getIds().length; i++) {
            DeleteReq deleteReq = new DeleteReq();
            deleteReq.setId(batchDeleteReq.getIds()[i]);
            CommonResponse commonResponseDelete = deleteQuoto(deleteReq);
            if (!commonResponseDelete.isSuccess()) {
                return commonResponseDelete;
            }
        }
        return commonResponse;
    }

    //增加指标
    @SuppressWarnings("deprecation")
    public CommonResponse insertQuoto(Quoto quoto) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        // 基础校验
        if (quoto == null || StringUtils.isEmpty(quoto.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名字不能为空");
            return commonResponse;
        }

        if (StringUtils.isEmpty(quoto.getField())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字段名称不能为空");
            return commonResponse;
        }

        if (quotoMapper.findQuotoByName(quoto.getName()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名字已存在,请重新命名");
            return commonResponse;
        }

        if (!StringUtils.isEmpty(quoto.getSql())) {
            CommonResponse checkCommonResponse = checkSql(quoto.getSql());
            if (!checkCommonResponse.isSuccess()) {
                return checkCommonResponse;
            }
        }

        if (quotoMapper.findQuotoByField(quoto.getField()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字段已存在,请重新命名");
            return commonResponse;
        }
        if (quotoMapper.queryThemeById(quoto.getTheme_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题不存在");
            return commonResponse;
        }
        if (quoto.getType() == TypeEnum.atomic_quoto.getCode()) {
            if (quoto.getTable_id() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("数据服务不能为空");
                return commonResponse;
            }
            if (quoto.getCycle() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("计算周期不能为空");
                return commonResponse;
            }
            quoto.setUse_sql(false);
        } else if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            quoto.setState(StateEnum.active_state.getCode());
            if (quoto.getOrigin_quoto() <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原子指标的id必须有值");
                return commonResponse;
            }
            //检验原子指标是否存在
            Quoto originQuoto = quotoMapper.findQuotoById(quoto.getOrigin_quoto());
            if (originQuoto == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原子指标不存在");
                return commonResponse;
            }
            //判断维度是否含了时间类型
            boolean haveTime = false;
            if (quoto.getDimension() != null && quoto.getDimension().length > 0) {
                List<Dimension> dimensions = dimensionMapper.queryTimeDimensionByIds(quoto.getDimension());
                if (dimensions != null && dimensions.size() > 0) {
                    haveTime = true;
                }
            }
            //判断修饰词是否含了时间类型
            if (quoto.getAdjective() != null && quoto.getAdjective().length > 0) {
                List<Adjective> adjectives = adjectiveMapper.queryTimeAdjectiveByIds(quoto.getAdjective());
                if (adjectives != null && adjectives.size() > 0) {
                    haveTime = true;
                }
            }
            //判断时间列是否存在
            if (haveTime) {
                if (dimensionMapper.queryTimeColumnById(originQuoto.getTable_id(), quoto.getTime_column_id()) == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("此时间列不存在");
                    return commonResponse;
                }
            }
            if (quoto.isUse_sql()) {
                Set<String> sqlParameters = com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(quoto.getSql());
                if (sqlParameters != null && sqlParameters.size() > 0) {
                    if (quoto.getAdjective() == null || quoto.getAdjective().length == 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("指标sql含有变量参数,需选择相匹配的修饰词");
                        return commonResponse;
                    }
                    List<Adjective> adjectives = adjectiveMapper.queryAdjectiveByIds(quoto.getAdjective());
                    Set<String> adjectiveParameter = new HashSet<>();
                    for (Adjective adjective : adjectives) {
                        if (adjective.getReq_parm_type() == 1) {
                            adjectiveParameter.addAll(com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(adjective.getReq_parm()));
                        }
                    }
                    for (String sqlParameter : sqlParameters) {
                        if (adjectiveParameter.contains(sqlParameter)) {
                            adjectiveParameter.remove(sqlParameter);
                        } else {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("指标sql含有变量参数" + sqlParameter + ",但修饰词里面没有");
                            return commonResponse;
                        }
                    }
                    if (adjectiveParameter.size() != 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("修饰词里含有的变量参数" + adjectiveParameter.toString() + ",在指标sql里没有");
                        return commonResponse;
                    }

                }
            }

        } else {
            quoto.setUse_sql(false);
            quoto.setState(StateEnum.active_state.getCode());
            if (StringUtils.isEmpty(quoto.getExpression())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("加工方式必须有值");
                return commonResponse;
            }
            String expression = quoto.getExpression();
            expression = expression.replaceAll(" ", "").replaceAll("[(]", "").replaceAll("[)]", "")
                    .replaceAll("[+]", " ").replaceAll("[-]", " ").replaceAll("[*]", " ").replaceAll("[/]", " ").replaceAll("[&]", " ");
            String[] expressions = expression.split(" ");
            List<Integer> quotos = new ArrayList<Integer>();
            for (int i = 0; i < expressions.length; i++) {
                if (NumberUtils.isNumber(expressions[i])) {
                    continue;
                }
                Quoto quotoInfo = quotoMapper.queryQuotoByField(expressions[i]);
                if (quotoInfo == null || quotoInfo.getState() != StateEnum.active_state.getCode()) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(expressions[i] + ":此指标不存在或未激活,请重新写");
                    return commonResponse;
                }
                quotos.add(quotoInfo.getId());
            }
            if (quotos.size() > 0) {
                int[] quotosInt = new int[quotos.size()];
                for (int i = 0; i < quotos.size(); i++) {
                    quotosInt[i] = quotos.get(i);
                }
                quoto.setQuotos(quotosInt);
            }
        }


        // 插入数据库
        try {
            quotoMapper.insertQuoto(quoto);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    /**
     * 校验sql是否合法
     *
     * @param sql
     * @return
     */
    public CommonResponse checkSql(String sql) {
        CommonResponse commonResponse = new CommonResponse();
        int MAX_QUERY_COUNT = 100000;
        String sqlLower = sql.toLowerCase().trim();
        if (sqlLower.contains(" limit ")) {
            int limitLocation = sqlLower.indexOf(" limit ");
            sqlLower = sqlLower.substring(limitLocation + 7);
            while (true) {
                if (sqlLower.startsWith(" ")) {
                    sqlLower = sqlLower.substring(1);
                } else {
                    break;
                }
            }
            String limitNumStr = sqlLower;
            if (sqlLower.indexOf(" ") > 0) {
                limitNumStr = sqlLower.substring(0, sqlLower.indexOf(" "));
            }
            try {
                int limitNum = Integer.parseInt(limitNumStr);
                if (limitNum <= 0 || limitNum > MAX_QUERY_COUNT) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("指标sql limit的数据量在1到" + MAX_QUERY_COUNT);
                    return commonResponse;
                }
            } catch (Exception e) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标sql limit后要接数字");
                return commonResponse;
            }
            return commonResponse;
        } else {
            int fromLocation = sqlLower.indexOf(" from");
            if (fromLocation < 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标sql没有from");
                return commonResponse;
            }
            sqlLower = sqlLower.substring(0, fromLocation);
            if (!sqlLower.startsWith("select ")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标sql不以select 开头");
                return commonResponse;
            } else {
                sqlLower = sqlLower.substring(6).replace(" ", "");
                for (String columnName : sqlLower.split(",")) {
                    int parenthesesLocation = columnName.indexOf("(");
                    if (parenthesesLocation < 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("指标sql需要limit做数据限制");
                        return commonResponse;
                    }
                    String functionName = columnName.substring(0, parenthesesLocation);
                    if (!(functionName.equals("count") || functionName.equals("sum") || functionName.equals("avg") || functionName.equals("max") || functionName.equals("min") || functionName.equals("count_big") || functionName.equals("grouping")
                            || functionName.equals("binary_checksum") || functionName.equals("checksum_agg") || functionName.equals("checksum") || functionName.equals("stdev") || functionName.equals("stdevp") || functionName.equals("var") || functionName.equals("varp"))) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("指标sql需要limit做数据限制");
                        return commonResponse;
                    }
                }
                return commonResponse;
            }
        }
    }

    //更新指标
    @SuppressWarnings("deprecation")
    public CommonResponse updateQuoto(Quoto quoto) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        Quoto oldQuoto = quotoMapper.queryQuotoById(quoto.getId());
        if (oldQuoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在,请刷新列表界面后再试！");
            return commonResponse;
        }
        // 基础校验
        if (quoto == null || StringUtils.isEmpty(quoto.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名字不能为空");
            return commonResponse;
        }

        if (StringUtils.isEmpty(quoto.getField())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字段名称不能为空");
            return commonResponse;
        }
        if (quotoMapper.queryThemeById(quoto.getTheme_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("主题不存在");
            return commonResponse;
        }
        if (!quoto.getName().equals(oldQuoto.getName())) {
            if (quotoMapper.findQuotoByName(quoto.getName()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("名字已存在,请重新命名");
                return commonResponse;
            }
        }

        if (!StringUtils.isEmpty(quoto.getSql())) {
            if (StringUtils.isEmpty(oldQuoto.getSql()) || (!quoto.getSql().equals(oldQuoto.getSql()))) {
                CommonResponse checkCommonResponse = checkSql(quoto.getSql());
                if (!checkCommonResponse.isSuccess()) {
                    return checkCommonResponse;
                }
            }
        }

        if (!quoto.getField().equals(oldQuoto.getField())) {
            if (quotoMapper.findQuotoByField(quoto.getField()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("字段已存在,请重新命名");
                return commonResponse;
            }
        }

        if (oldQuoto.getType() == TypeEnum.atomic_quoto.getCode()
                && oldQuoto.getState() == StateEnum.active_state.getCode()) {
            if (!oldQuoto.getField().equals(quoto.getField())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("激活的原子指标不能修改字段名！");
                return commonResponse;
            }
            if (oldQuoto.getTable_id() != quoto.getTable_id()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("激活的原子指标不能修改数据服务！");
                return commonResponse;
            }
        }
        if (quoto.getType() == TypeEnum.complex_quoto.getCode()) {
            if (quoto.getExpression().equals(oldQuoto.getExpression())) {
                quoto.setQuotos(oldQuoto.getQuotos());
            } else {
                if (StringUtils.isEmpty(quoto.getExpression())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("加工方式必须有值");
                    return commonResponse;
                }
                String expression = quoto.getExpression();
                expression = expression.replaceAll(" ", "").replaceAll("[(]", "").replaceAll("[)]", "")
                        .replaceAll("[+]", " ").replaceAll("[-]", " ").replaceAll("[*]", " ").replaceAll("[/]", " ").replaceAll("[&]", " ");
                String[] expressions = expression.split(" ");
                List<Integer> quotos = new ArrayList<Integer>();
                for (int i = 0; i < expressions.length; i++) {
                    if (NumberUtils.isNumber(expressions[i])) {
                        continue;
                    }
                    Quoto quotoInfo = quotoMapper.queryQuotoByField(expressions[i]);
                    if (quotoInfo == null || quotoInfo.getState() != StateEnum.active_state.getCode()) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage(expressions[i] + ":此指标不存在或未激活,请重新写");
                        return commonResponse;
                    }
                    quotos.add(quotoInfo.getId());
                }
                if (quotos.size() > 0) {
                    int[] quotosInt = new int[quotos.size()];
                    for (int i = 0; i < quotos.size(); i++) {
                        quotosInt[i] = quotos.get(i);
                    }
                    quoto.setQuotos(quotosInt);
                }
            }
        }
        if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            if (quoto.getOrigin_quoto() <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原子指标的id必须有值");
                return commonResponse;
            }
            //检验原子指标是否存在
            Quoto originQuoto = quotoMapper.findQuotoById(quoto.getOrigin_quoto());
            if (originQuoto == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原子指标不存在");
                return commonResponse;
            }
            //判断维度是否含了时间类型
            boolean haveTime = false;
            if (quoto.getDimension() != null && quoto.getDimension().length > 0) {
                List<Dimension> dimensions = dimensionMapper.queryTimeDimensionByIds(quoto.getDimension());
                if (dimensions != null && dimensions.size() > 0) {
                    haveTime = true;
                }
            }
            //判断修饰词是否含了时间类型
            if (quoto.getAdjective() != null && quoto.getAdjective().length > 0) {
                List<Adjective> adjectives = adjectiveMapper.queryTimeAdjectiveByIds(quoto.getAdjective());
                if (adjectives != null && adjectives.size() > 0) {
                    haveTime = true;
                }
            }
            //判断时间列是否存在
            if (haveTime) {
                if (dimensionMapper.queryTimeColumnById(originQuoto.getTable_id(), quoto.getTime_column_id()) == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("此时间列不存在");
                    return commonResponse;
                }
            }
            if (quoto.isUse_sql()) {
                Set<String> sqlParameters = com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(quoto.getSql());
                if (sqlParameters != null && sqlParameters.size() > 0) {
                    if (quoto.getAdjective() == null || quoto.getAdjective().length == 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("指标sql含有变量参数,需选择相匹配的修饰词");
                        return commonResponse;
                    }
                    List<Adjective> adjectives = adjectiveMapper.queryAdjectiveByIds(quoto.getAdjective());
                    Set<String> adjectiveParameter = new HashSet<>();
                    for (Adjective adjective : adjectives) {
                        if (adjective.getReq_parm_type() == 1) {
                            adjectiveParameter.addAll(com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(adjective.getReq_parm()));
                        }
                    }
                    for (String sqlParameter : sqlParameters) {
                        if (adjectiveParameter.contains(sqlParameter)) {
                            adjectiveParameter.remove(sqlParameter);
                        } else {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("指标sql含有变量参数" + sqlParameter + ",但修饰词里面没有");
                            return commonResponse;
                        }
                    }
                    if (adjectiveParameter.size() != 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("修饰词里含有的变量参数" + adjectiveParameter.toString() + ",在指标sql里没有");
                        return commonResponse;
                    }

                }
            }
        }
        try {
            if (quotoMapper.updateQuoto(quoto) <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("编辑指标失败，请稍后再试！");
                return commonResponse;
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("编辑指标失败，请稍后再试！");
            return commonResponse;
        }
        //插入历史记录
        if (commonResponse.isSuccess()) {
            quotoMapper.insertQuotoUpdateHistory(oldQuoto);
        }
        return commonResponse;
    }

    public CommonQueryResponse queryQuoto(QuotoQuery quotoQuery) {
        // TODO Auto-generated method stub
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        if (StringUtils.isEmpty(quotoQuery.getCreator())) {
            commonQueryResponse.setSuccess(false);
            commonQueryResponse.setMessage("creator不能为空!");
            return commonQueryResponse;
        }
        String condition = "q.deleted=0";
        if (quotoQuery.getType() != -1) {
            if (quotoQuery.getType() == 3) {
                condition = condition + " and q.type!=0";
            } else {
                condition = condition + " and q.type=" + quotoQuery.getType();
            }
        }
        if (quotoQuery.getQuoto_level() != -1) {
            condition = condition + " and q.quoto_level=" + quotoQuery.getQuoto_level();
        }

        if (quotoQuery.getName() != null && (!quotoQuery.getName().equals(""))) {
            condition = condition + " and q.name like '" + quotoQuery.getName() + "%'";
        }

        if (quotoQuery.getField() != null && (!quotoQuery.getField().equals(""))) {
            condition = condition + " and q.field like '" + quotoQuery.getField() + "%'";
        }

        if (quotoQuery.getTheme_id() != -1) {
            condition = condition + " and q.theme_id=" + quotoQuery.getTheme_id();
        } else {
            if (quotoQuery.getBusinessId() != -1) {
                Business business = quotoMapper.queryBusinessById(quotoQuery.getBusinessId());
                if (business == null) {
                    commonQueryResponse.setSuccess(false);
                    commonQueryResponse.setMessage("业务线不存在!");
                    return commonQueryResponse;
                }
                //输入的第一级业务线
                if (business.getPid() == 0) {
                    condition = condition + " and tt.business_id_one_level=" + business.getId();
                } else {
                    Business pidBusiness = quotoMapper.queryBusinessById(business.getPid());
                    //输入的是第二级业务线
                    if (pidBusiness.getPid() == 0) {
                        condition = condition + " and tt.business_id_two_level=" + business.getId();
                    } else {
                        //第三级业务线
                        condition = condition + " and tt.business_id_three_level=" + business.getId();
                    }
                }

            }
        }


        if (quotoQuery.getState() != -1) {
            condition = condition + " and q.state=" + quotoQuery.getState();
        }
        if (quotoQuery.getTags() != null && quotoQuery.getTags().length > 0) {
            condition = condition + " and q.id in (select distinct quoto_id from quoto_tag where deleted=0 and creator='" + quotoQuery.getCreator() + "' and tag_id in " + Arrays.toString(quotoQuery.getTags()).replace("[", "(").replace("]", ")") + ")";
        }


        condition = condition + " order by q.update_time desc";
        int page = quotoQuery.getPage();
        int size = quotoQuery.getSize();
        int startLine = (page - 1) * size;
        commonQueryResponse.setData(quotoMapper.queryQuoto(condition, startLine, size, quotoQuery.getCreator()));
        commonQueryResponse.setCurrentPage(quotoQuery.getPage());
        commonQueryResponse.setTotal(quotoMapper.queryQuotoCount(condition));
        return commonQueryResponse;
    }

    public CommonResponse queryQuotoUpdateHistory(int id) {
        CommonResponse commonResponse = new CommonResponse();
        Quoto quoto = quotoMapper.queryQuotoById(id);
        if (quoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在,请核实id值是否正确");
            return commonResponse;
        }
        commonResponse.setData(quotoMapper.queryQuotoUpdateHistoryById(id));
        return commonResponse;
    }

    public CommonResponse queryAllQuoto(QuotoQuery quotoQuery) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        String condition = "deleted=0 and state=1";
        if (quotoQuery.getType() != -1) {
            if (quotoQuery.getType() == 3) {
                condition = condition + " and type!=0";
            } else {
                condition = condition + " and type=" + quotoQuery.getType();
            }
        }

        if (quotoQuery.getBusiness_process_id() != -1) {
            condition = condition + " and business_process_id=" + quotoQuery.getBusiness_process_id();
        }

        if (quotoQuery.getTheme_id() != -1) {
            condition = condition + " and theme_id=" + quotoQuery.getTheme_id();
        }

        condition = condition + " order by name asc";

        commonResponse.setData(quotoMapper.queryAllQuoto(condition));
        return commonResponse;
    }

    public CommonResponse queryAllCycle() {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryAllCycle());
        return commonResponse;
    }

    public CommonResponse queryQuotoById(int id) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        Quoto quoto = quotoMapper.queryQuotoById(id);
        if (quoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在,请核实id值是否正确");
            return commonResponse;
        }
        commonResponse.setData(quotoMapper.queryQuotoById(id));
        return commonResponse;
    }

    public CommonResponse activeQuoto(int id) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        Quoto quoto = quotoMapper.findQuotoById(id);
        if (quoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在,请核实id值是否正确");
            return commonResponse;
        }
        if (quoto.getType() != TypeEnum.atomic_quoto.getCode()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("只有原子指标才有激活的操作");
            return commonResponse;
        }
        if (quoto.getState() == StateEnum.active_state.getCode()) {
            commonResponse.setSuccess(true);
            commonResponse.setMessage("指标已激活");
            return commonResponse;
        }

        QuotoInfo quotoInfo = quotoMapper.queryQuotoInfo(quoto.getField());
        if (quotoInfo == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据服务没有此指标的配置,请前往数据服务-服务管理页配置此指标");
            return commonResponse;
        }
        if (quotoInfo.getState() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据服务此指标不可用,请前往数据服务-服务管理页启用此指标");
            return commonResponse;
        }

        // 激活指标
        if (quotoMapper.updateQuotoState(StateEnum.active_state.getCode(), id) != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("激活失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryFuzzy(QuotoQuery quotoQuery) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        String condition = "deleted=0";
        if (quotoQuery.getType() != -1) {
            condition = condition + " and type=" + quotoQuery.getType();
        }

        if (quotoQuery.getName() != null && (!quotoQuery.getName().equals(""))) {
            condition = condition + " and name like '" + quotoQuery.getName() + "%'";
        }
        commonResponse.setData(quotoMapper.queryQuotoFuzzy(condition));
        return commonResponse;
    }

    public DataCommonResponse queryQuotoData(Integer id, String quotoName, String fildName, Integer page, Integer count,
                                             Set<String> order, Boolean acs, Map<String, Object> parm_value) {
        // TODO Auto-generated method stub
        DataCommonResponse commonResponse = new DataCommonResponse();
        // 查询指标
        Quoto quoto = null;
        if (id != null && id > 0) {
            quoto = quotoMapper.queryQuotoById(id);
        }
        if (quoto == null && (!StringUtils.isEmpty(quotoName))) {
            quoto = quotoMapper.queryQuotoByName(quotoName);
        }

        if (quoto == null && (!StringUtils.isEmpty(fildName))) {
            quoto = quotoMapper.queryQuotoByField(fildName);
        }

        if (page == null) {
            page = 0;
        }
        if (quoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在");
            return commonResponse;
        }
        if (quoto.getState() != StateEnum.active_state.getCode()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("非激活状态的指标不可以查询数据");
            return commonResponse;
        }
        if (count == null || count <= 0) {
            count = 1000;
        }

        if (count > 10000) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("count最大值为10000");
            return commonResponse;
        }
        commonResponse = queryDataFromDataService(quoto, page, count, order, acs, parm_value);
        QuotoAccessHistory quotoAccessHistory = new QuotoAccessHistory();
        quotoAccessHistory.setQuoto_id(quoto.getId());
        quotoAccessHistory.setQuoto_name(quoto.getName());
        String business_name = "";
        if (!StringUtils.isEmpty(quoto.getBusiness_name_one_level())) {
            business_name = business_name + quoto.getBusiness_name_one_level();
        }
        if (!StringUtils.isEmpty(quoto.getBusiness_name_two_level())) {
            business_name = business_name + "/" + quoto.getBusiness_name_two_level();
        }
        if (!StringUtils.isEmpty(quoto.getBusiness_name_three_level())) {
            business_name = business_name + "/" + quoto.getBusiness_name_three_level();
        }
        quotoAccessHistory.setBusiness(business_name);
        quotoAccessHistory.setTheme(quoto.getTheme_name());
        quotoAccessHistory.setLevel(quoto.getQuoto_level());
        quotoAccessHistory.setType(quoto.getType());
        quotoAccessHistory.setSuccess(commonResponse.isSuccess());
        quotoAccessHistory.setMessage(commonResponse.getMessage());
        quotoAccessHistoryMapper.insertAccessHistory(quotoAccessHistory);
        if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            Quoto atomicQuoto = quotoMapper.queryQuotoById(quoto.getOrigin_quoto());
            String auoto_business_name = "";
            if (!StringUtils.isEmpty(atomicQuoto.getBusiness_name_one_level())) {
                auoto_business_name = auoto_business_name + atomicQuoto.getBusiness_name_one_level();
            }
            if (!StringUtils.isEmpty(atomicQuoto.getBusiness_name_two_level())) {
                auoto_business_name = auoto_business_name + "/" + atomicQuoto.getBusiness_name_two_level();
            }
            if (!StringUtils.isEmpty(atomicQuoto.getBusiness_name_three_level())) {
                auoto_business_name = auoto_business_name + "/" + atomicQuoto.getBusiness_name_three_level();
            }
            quotoAccessHistory.setQuoto_id(atomicQuoto.getId());
            quotoAccessHistory.setQuoto_name(atomicQuoto.getName());
            quotoAccessHistory.setBusiness(auoto_business_name);
            quotoAccessHistory.setTheme(atomicQuoto.getTheme_name());
            quotoAccessHistory.setLevel(atomicQuoto.getQuoto_level());
            quotoAccessHistory.setType(atomicQuoto.getType());
            quotoAccessHistory.setSuccess(commonResponse.isSuccess());
            quotoAccessHistory.setMessage(commonResponse.getMessage());
            quotoAccessHistoryMapper.insertAccessHistory(quotoAccessHistory);
        }
        return commonResponse;
    }

    public DataCommonResponse queryDataFromDataService(Quoto quoto, int page, int count, Set<String> orders,
                                                       Boolean acs, Map<String, Object> parm_value) {
        DataCommonResponse commonResponse = new DataCommonResponse();

        // 复合指标处理逻辑
        if (quoto.getType() == TypeEnum.complex_quoto.getCode()) {
            try {
                DataCommonResponse dataCommonResponse = caculate(quoto.getExpression().replace(" ", "") + "#", page,
                        count, orders, acs, parm_value);
                if (dataCommonResponse.isSuccess()) {
                    quoto.setCycle(dataCommonResponse.getCycle());
                    quoto.setDimension(dataCommonResponse.getDimensionIds());
                    quotoMapper.updateQuoto(quoto);
                }
                return dataCommonResponse;
            } catch (Exception e) {
                // TODO: handle exception
                commonResponse.setSuccess(false);
                commonResponse.setMessage(e.getMessage());
                return commonResponse;
            }
        }
        // 非复合指标处理逻辑
        Quoto atomicQuoto = quoto;
        if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            atomicQuoto = quotoMapper.findQuotoById(quoto.getOrigin_quoto());
        }
        Set<String> fileds = new HashSet<>();
        fileds.add(atomicQuoto.getField());
        commonResponse.setFields(fileds);
        commonResponse.setCycle(atomicQuoto.getCycle());
        // 查询数据服务对应的信息
        ServicePathInfo servicePathInfo = quotoMapper.queryServicePathInfo(atomicQuoto.getTable_id());
        if (servicePathInfo == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标对应的服务不可用,请联系管理员排查");
            return commonResponse;
        }
        String url = dataServiceUrl + servicePathInfo.getPath();
        String bodyRequest = "{'[]':{'" + servicePathInfo.getTableName() + "':{'@column':'" + atomicQuoto.getField();
        if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            //判断是否启用的指标sql
            if (quoto.isUse_sql()) {
                if (quoto.getDimension() != null && quoto.getDimension().length > 0) {
                    List<DimensionExtend> dimensionName = quotoMapper.queryDimensionByQuotoId(quoto.getId());
                    Set<String> dimensionSet = new HashSet<>();
                    commonResponse.setDimensionIds(quoto.getDimension());
                    for (DimensionExtend dimension : dimensionName) {
                        dimensionSet.add(dimension.getCode());
                    }
                    commonResponse.setDimensions(dimensionSet);
                }
                Set<String> parameter_names = com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(quoto.getSql());
                if (parameter_names != null & parameter_names.size() > 0) {
                    if (parm_value == null || parm_value.size() < 1) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("使用了可变参数,需要传入parm_value参数");
                        return commonResponse;
                    }
                    for (String parameter_name : parameter_names) {
                        Object value = parm_value.get(parameter_name);
                        if (value == null) {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("请在参数parm_value中传入 " + parameter_name + " 的值");
                            return commonResponse;
                        }
                        String apijsonValue = "";
                        if (value instanceof String || value instanceof Integer) {
                            apijsonValue = value.toString();
                        } else if (value instanceof ArrayList) {
                            ArrayList<Object> valueList = (ArrayList<Object>) value;
                            if (valueList == null || valueList.size() < 1) {
                                apijsonValue = "()";
                            } else {
                                apijsonValue = "(";
                                String symbol = "";
                                if (valueList.get(0) instanceof String) {
                                    symbol = "'";
                                }
                                for (int j = 0; j < valueList.size(); j++) {
                                    apijsonValue = apijsonValue + symbol + valueList.get(j).toString() + symbol;
                                    if (j != valueList.size() - 1) {
                                        apijsonValue = apijsonValue + ",";
                                    }
                                }
                                apijsonValue = apijsonValue + ")";
                            }

                        } else {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("请求参数parm_value中 " + parameter_name + " 的值类型不支持,目前只支持string,int,array");
                            return commonResponse;
                        }
                        quoto.setSql(quoto.getSql().replace("${" + parameter_name + "}", apijsonValue));
                    }
                }
                quoto.setSql(quoto.getSql().replace("'", "\\'"));
                bodyRequest = "{'[]':{'" + servicePathInfo.getTableName() + "':{'@sql':'" + quoto.getSql() + "'";

            } else {
                ColumnAlias columnAlias = dimensionMapper.queryTimeColumnById(atomicQuoto.getTable_id(), quoto.getTime_column_id());
                // 添加维度的请求参数
                if (quoto.getDimension() != null && quoto.getDimension().length > 0) {
                    String group = "'@group':'";
                    bodyRequest = bodyRequest + ";";
                    // 查询维度的名称
                    List<DimensionExtend> dimensionName = quotoMapper.queryDimensionByQuotoId(quoto.getId());
                    Set<String> dimensionSet = new HashSet<>();
                    commonResponse.setDimensionIds(quoto.getDimension());
                    int i = 0;
                    for (DimensionExtend dimension : dimensionName) {
                        dimensionSet.add(dimension.getCode());
                        String columnRequest = dimension.getCode();

                        //判断是否是时间维度
                        if (dimension.getDimension_object_code().equals("time")) {
                            if (columnAlias == null) {
                                commonResponse.setSuccess(false);
                                commonResponse.setMessage("使用了时间维度,指标需要配置可用的时间列");
                                return commonResponse;
                            }
                            columnRequest = dimension.getCode() + "(" + columnAlias.getColumn_alias() + ")" + ":" + dimension.getCode();
                        }
                        if (i == dimensionName.size() - 1) {
                            group = group + dimension.getCode() + "'";
                            bodyRequest = bodyRequest + columnRequest + "'";
                        } else {
                            group = group + dimension.getCode() + ",";
                            bodyRequest = bodyRequest + columnRequest + ";";
                        }
                        i++;
                    }
                    commonResponse.setDimensions(dimensionSet);
                    bodyRequest = bodyRequest + "," + group;
                } else {
                    bodyRequest = bodyRequest + "'";
                }
                // 添加修饰词的请求参数
                if (quoto.getAdjective() != null && quoto.getAdjective().length > 0) {
                    // 查询修饰词信息
                    List<AdjectiveExtend> adjectives = quotoMapper.queryAdjective(quoto.getId());
                    for (int i = 0; i < adjectives.size(); i++) {
                        //若是时间修饰词,将时间字段填入进去
                        if (adjectives.get(i).getType() == 1) {
                            if (columnAlias == null) {
                                commonResponse.setSuccess(false);
                                commonResponse.setMessage("使用了时间修饰词,指标需要配置可用的时间列");
                                return commonResponse;
                            }
                            adjectives.get(i).setColumn_name(columnAlias.getColumn_alias());
                            adjectives.get(i).setDescr(columnAlias.getData_type());
                        } else {
                            //带变量的修饰词
                            if (adjectives.get(i).getReq_parm_type() == 1) {
                                if (parm_value == null || parm_value.size() < 1) {
                                    commonResponse.setSuccess(false);
                                    commonResponse.setMessage("使用了可变参数,需要传入parm_value参数");
                                    return commonResponse;
                                }
                                Set<String> parameter_names = com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(adjectives.get(i).getReq_parm());
                                for (String parameter_name : parameter_names) {
                                    Object value = parm_value.get(parameter_name);
                                    if (value == null) {
                                        commonResponse.setSuccess(false);
                                        commonResponse.setMessage("请在参数parm_value中传入 " + parameter_name + " 的值");
                                        return commonResponse;
                                    }
                                    String apijsonValue = "";
                                    if (value instanceof String || value instanceof Integer) {
                                        apijsonValue = value.toString();
                                    } else if (value instanceof ArrayList) {
                                        ArrayList<Object> valueList = (ArrayList<Object>) value;
                                        if (valueList == null || valueList.size() < 1) {
                                            apijsonValue = "[]";
                                        } else {
                                            apijsonValue = "[";
                                            String symbol = "";
                                            if (valueList.get(0) instanceof String) {
                                                symbol = "'";
                                            }
                                            for (int j = 0; j < valueList.size(); j++) {
                                                apijsonValue = apijsonValue + symbol + valueList.get(j).toString() + symbol;
                                                if (j != valueList.size() - 1) {
                                                    apijsonValue = apijsonValue + ",";
                                                }
                                            }
                                            apijsonValue = apijsonValue + "]";
                                        }

                                    } else {
                                        commonResponse.setSuccess(false);
                                        commonResponse.setMessage("请求参数parm_value中 " + parameter_name + " 的值类型不支持,目前只支持string,int,array");
                                        return commonResponse;
                                    }
                                    adjectives.get(i).setReq_parm(adjectives.get(i).getReq_parm().replace("${" + parameter_name + "}", apijsonValue));
                                }
                            }
                        }
                        bodyRequest = bodyRequest + "," + getAdjectiveReq(adjectives.get(i));
                    }
                }
            }
        } else {
            bodyRequest = bodyRequest + "'";
        }

        if (orders != null && orders.size() > 0) {
            boolean isOrder = true;
            for (String order : orders) {
                if ((commonResponse.getFields() == null || (!commonResponse.getFields().contains(order))) && (commonResponse.getDimensions() == null || (!commonResponse.getDimensions().contains(order)))) {
                    isOrder = false;
                    break;
                }
            }
            if (isOrder) {
                bodyRequest = bodyRequest + ",'@order':'" + StringUtils.join(orders.toArray(), ",");
                if (acs != null && acs) {
                    bodyRequest = bodyRequest + "+'";
                } else {
                    bodyRequest = bodyRequest + "-'";
                }
            }
        }
        bodyRequest = bodyRequest + "},'page':" + page + ",'count':" + count + "}}";
        System.out.println(bodyRequest);
        // 请求数据服务
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("token", "L0V91TZWH4K8YZPBBG3M");
        // 将请求头部和参数合成一个请求
        HttpEntity<String> requestEntity = new HttpEntity<>(bodyRequest, headers);
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(url, requestEntity, String.class);
        if (!responseEntity.getStatusCode().is2xxSuccessful()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标对应的服务不可用,请联系管理员排查");
            return commonResponse;
        } else {
            JSONObject result = JSONObject.parseObject(responseEntity.getBody().toString());
            DataServiceResponse dataServiceResponse = JSONObject.toJavaObject(
                    JSONObject.parseObject(responseEntity.getBody().toString()), DataServiceResponse.class);
            commonResponse.setSuccess(dataServiceResponse.isOk());
            commonResponse.setMessage(dataServiceResponse.getMsg());
            if (dataServiceResponse.isOk()) {
                if (result.get("[]") == null) {
                    commonResponse.setType(0);
                    return commonResponse;
                }
                List<JSONObject> list = JSONObject.parseArray(result.get("[]").toString(), JSONObject.class);
                if (list != null) {
                    if (list.size() == 1) {
                        commonResponse.setType(1);
                        commonResponse.setData(list.get(0).get(servicePathInfo.getTableName()));
                    } else {
                        List<Object> data = new ArrayList<Object>();
                        for (int i = 0; i < list.size(); i++) {
                            data.add(list.get(i).get(servicePathInfo.getTableName()));
                        }
                        commonResponse.setType(2);
                        commonResponse.setData(data);
                    }
                }
            }
        }
        if (quoto.getType() == TypeEnum.derive_quoto.getCode() && quoto.isUse_sql()) {
            Map<String,Set<String>> columnAndGroup = getColumnAndGroup(quoto.getSql());
            if (!columnAndGroup.get("column").isEmpty()) {
                commonResponse.setFields(columnAndGroup.get("column"));
            }
            if (!columnAndGroup.get("group").isEmpty()) {
                commonResponse.setDimensions(columnAndGroup.get("group"));
            }
        }
        return commonResponse;
    }

    /**
     * 根据修饰词信息组装请求的参数
     *
     * @param adjective
     * @return
     */
    public String getAdjectiveReq(AdjectiveExtend adjective) {
        // 1为时间修饰词
        if (adjective.getType() == 1) {
            int timeType = 1;//1代表正常的yyyy-MM-dd hh:mm:ss 2代表日期yyyy-MM-dd
            if (!StringUtils.isEmpty(adjective.getDescr()) && adjective.getDescr().toLowerCase().equals("date")) {
                timeType = 2;
            }
            String result = "'" + adjective.getColumn_name();
            if (adjective.getCode().equals("last1HOUR")) {
                result = result + ">=':'" + DateTimeUtils.getlast1HOUR(timeType) + "'";
            } else if (adjective.getCode().equals("last1DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(0, timeType) + "'";
            } else if (adjective.getCode().equals("last2DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-1, timeType) + "'";
            } else if (adjective.getCode().equals("last3DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-2, timeType) + "'";
            } else if (adjective.getCode().equals("last7DAY") || adjective.getCode().equals("last1WEEK")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-6, timeType) + "'";
            } else if (adjective.getCode().equals("last14DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-13, timeType) + "'";
            } else if (adjective.getCode().equals("last15DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-15, timeType) + "'";
            } else if (adjective.getCode().equals("last30DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-30, timeType) + "'";
            } else if (adjective.getCode().equals("last60DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-60, timeType) + "'";
            } else if (adjective.getCode().equals("last90DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-90, timeType) + "'";
            } else if (adjective.getCode().equals("last180DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-180, timeType) + "'";
            } else if (adjective.getCode().equals("last360DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-360, timeType) + "'";
            } else if (adjective.getCode().equals("last365DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-365, timeType) + "'";
            } else if (adjective.getCode().equals("last1MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-1, timeType) + "'";
            } else if (adjective.getCode().equals("last2MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-2, timeType) + "'";
            } else if (adjective.getCode().equals("last3MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-3, timeType) + "'";
            } else if (adjective.getCode().equals("last6MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-6, timeType) + "'";
            } else if (adjective.getCode().equals("last7MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-7, timeType) + "'";
            } else if (adjective.getCode().equals("last8MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-8, timeType) + "'";
            } else if (adjective.getCode().equals("last1YEAR")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByYear(-1, timeType) + "'";
            } else if (adjective.getCode().equals("last2YEAR")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByYear(-2, timeType) + "'";
            } else if (adjective.getCode().equals("ftDate(w)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateWeek(timeType) + "'";
            } else if (adjective.getCode().equals("ftDate(m)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateMonth(timeType) + "'";
            } else if (adjective.getCode().equals("ftDate(q)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateQuarter(timeType) + "'";
            } else if (adjective.getCode().equals("ftDate(y)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateYear(timeType) + "'";
            } else if (adjective.getCode().equals("pre1MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-1, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre2MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-2, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre3MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-3, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre4MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-4, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre5MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-5, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre6MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-6, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre12MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-12, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre24MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-24, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre1DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-1, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre2DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-2, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre3DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-3, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre7DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-7, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre14DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-14, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre15DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-15, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre30DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-30, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre60DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-60, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre90DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-90, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre180DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-180, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre360DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-360, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre365DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-365, timeType) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre1YEAR")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByYear(-1, timeType) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByYear(0, timeType) + "\\''";
            } else if (adjective.getCode().equals("pre1QUARTER")) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                Calendar cal = Calendar.getInstance();
                int currentMonth = cal.get(Calendar.MONTH) + 1;
                if (currentMonth >= 1 && currentMonth <= 3) {
                    cal.set(Calendar.MONTH, 0);
                } else if (currentMonth >= 4 && currentMonth <= 6) {
                    cal.set(Calendar.MONTH, 3);
                } else if (currentMonth >= 7 && currentMonth <= 9) {
                    cal.set(Calendar.MONTH, 4);
                } else if (currentMonth >= 10 && currentMonth <= 12) {
                    cal.set(Calendar.MONTH, 9);
                }
                cal.set(Calendar.DATE, 1);
                String end = format.format(cal.getTime());
                cal.add(Calendar.MONTH, -3);
                String start = format.format(cal.getTime());
                if (timeType == 1) {
                    end = end + " 00:00:00";
                    start = start + " 00:00:00";
                }
                result = result + "&{}':'>=\\'" + start + "\\',<\\'" + end + "\\''";
            }
            return result;
        }

        // 业务修饰词
        String result = "'";
        if (StringUtils.isEmpty(adjective.getDimension_code())) {
            result = result + adjective.getColumn_name();
        } else {
            result = result + adjective.getDimension_code();
        }
        return result + adjective.getReq_parm().substring(1);
    }

    /**
     * 解析计算公式获取数据
     *
     * @param str   计算公式
     * @param page  查询数据的页数
     * @param count 每页的条数
     * @return
     */
    public DataCommonResponse caculate(String str, int page, int count, Set<String> order, Boolean acs, Map<String, Object> parm_value) {
        Stack<Character> priStack = new Stack<Character>();// 操作符栈
        Stack<DataCommonResponse> numStack = new Stack<DataCommonResponse>();
        ;// 操作数栈
        // 1.判断string当中有没有非法字符
        String temp;// 用来临时存放读取的字符
        // 2.循环开始解析字符串，当字符串解析完，且符号栈为空时，则计算完成
        StringBuffer tempNum = new StringBuffer();// 用来临时存放数字字符串(当为多位数时)
        StringBuffer string = new StringBuffer().append(str);// 用来保存，提高效率

        while (string.length() != 0) {
            temp = string.substring(0, 1);
            string.delete(0, 1);
            // 判断temp，当temp为操作符时
            if (QuotoCaculateUtils.isOperator(temp)) {
                // 1.此时的tempNum内即为需要操作的数，取出数，压栈，并且清空tempNum
                if (!"".equals(tempNum.toString())) {
                    // 当表达式的第一个符号为括号
                    String num = tempNum.toString();
                    numStack.push(getCalculateValue(num, page, count, order, acs, parm_value));
                    tempNum.delete(0, tempNum.length());
                }
                // 用当前取得的运算符与栈顶运算符比较优先级：若高于，则因为会先运算，放入栈顶；若等于，因为出现在后面，所以会后计算，所以栈顶元素出栈，取出操作数运算；
                // 若小于，则同理，取出栈顶元素运算，将结果入操作数栈。

                // 判断当前运算符与栈顶元素优先级，取出元素，进行计算(因为优先级可能小于栈顶元素，还小于第二个元素等等，需要用循环判断)
                while (!QuotoCaculateUtils.compare(temp.charAt(0), priStack) && (!priStack.empty())) {
                    DataCommonResponse a = numStack.pop();// 第二个运算数
                    DataCommonResponse b = numStack.pop();// 第一个运算数
                    char ope = priStack.pop();
                    DataCommonResponse result = null;// 运算结果
                    switch (ope) {
                        // 如果是加号或者减号，则
                        case '+':
                            result = QuotoCaculateUtils.CalculateValue(b, a, "+");
                            // 将操作结果放入操作数栈
                            numStack.push(result);
                            break;
                        case '&':
                            result = QuotoCaculateUtils.CalculateValue(b, a, "&");
                            // 将操作结果放入操作数栈
                            numStack.push(result);
                            break;
                        case '-':
                            result = QuotoCaculateUtils.CalculateValue(b, a, "-");
                            // 将操作结果放入操作数栈
                            numStack.push(result);
                            break;
                        case '*':
                            result = QuotoCaculateUtils.CalculateValue(b, a, "*");
                            // 将操作结果放入操作数栈
                            numStack.push(result);
                            break;
                        case '/':
                            result = QuotoCaculateUtils.CalculateValue(b, a, "/");
                            numStack.push(result);
                            break;
                    }

                }
                // 判断当前运算符与栈顶元素优先级， 如果高，或者低于平，计算完后，将当前操作符号，放入操作符栈
                if (temp.charAt(0) != '#') {
                    priStack.push(new Character(temp.charAt(0)));
                    if (temp.charAt(0) == ')') {// 当栈顶为'('，而当前元素为')'时，则是括号内以算完，去掉括号
                        priStack.pop();
                        priStack.pop();
                    }
                }
            } else
                // 当为非操作符时（数字）
                tempNum = tempNum.append(temp);// 将读到的这一位数接到以读出的数后(当不是个位数的时候)
        }
        return numStack.pop();
    }

    /**
     * @param fieldName 根据指标名称获取数据
     * @param page
     * @param count
     * @return
     */
    @SuppressWarnings("deprecation")
    private DataCommonResponse getCalculateValue(String fieldName, int page, int count, Set<String> order,
                                                 Boolean acs, Map<String, Object> parm_value) {
        if (NumberUtils.isNumber(fieldName)) {
            DataCommonResponse calculateValue = new DataCommonResponse();
            calculateValue.setData(new BigDecimal(fieldName));
            calculateValue.setType(3);
            return calculateValue;
        }
        DataCommonResponse commonResponse = queryQuotoData(0, null, fieldName, page, count, order, acs, parm_value);
        if (!commonResponse.isSuccess()) {
            // 抛异常
            throw new UnsupportedOperationException("指标(" + fieldName + ")获取失败：" + commonResponse.getMessage());
        } else {
            return commonResponse;
        }

    }

    //查询字标变量参数信息
    public CommonResponse queryQuotoNeedParm(int id) {
        CommonResponse commonResponse = new CommonResponse();
        Quoto quoto = quotoMapper.queryQuotoById(id);
        if (quoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在,请稍后再试");
            return commonResponse;
        }
        if (quoto.getAdjective() == null || quoto.getAdjective().length == 0) {
            return commonResponse;
        }
        List<Adjective> adjectives = adjectiveMapper.queryAdjectiveByIds(quoto.getAdjective());
        List<Field> fileds = new ArrayList<>();
        for (Adjective adjective : adjectives) {
            if (adjective.getReq_parm_type() == 1) {
                if (adjective.getFields() != null && adjective.getFields().size() > 0) {
                    fileds.addAll(adjective.getFields());
                }
            }
        }
        if (fileds.size() == 0) {
            return commonResponse;
        }
        commonResponse.setData(fileds);
        return commonResponse;
    }

    //获取指标调用文档
    public CommonResponse queryQuotoApiDoc(int id) {
        CommonResponse commonResponse = new CommonResponse();
        QuotoApiDoc quotoApiDoc = new QuotoApiDoc();
        Quoto quoto = quotoMapper.queryQuotoById(id);
        if (quoto == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标不存在");
            return commonResponse;
        }
        if (quoto.getType() == TypeEnum.atomic_quoto.getCode()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原子指标没有api调用文档");
            return commonResponse;
        }
        List<ExtendField> extendFields = new ArrayList<>();
        ExtendField extendFieldId = new ExtendField();
        extendFieldId.setName("id");
        extendFieldId.setType("int");
        extendFieldId.setAllowBlank(true);
        extendFieldId.setSample(id);
        extendFieldId.setDesc("指标的id(id,name,field选填一个就可以了)");
        extendFields.add(extendFieldId);

        ExtendField extendFieldName = new ExtendField();
        extendFieldName.setName("name");
        extendFieldName.setType("String");
        extendFieldName.setAllowBlank(true);
        extendFieldName.setSample(quoto.getName());
        extendFieldName.setDesc("指标名称");
        extendFields.add(extendFieldName);

        ExtendField extendFieldField = new ExtendField();
        extendFieldField.setName("field");
        extendFieldField.setType("String");
        extendFieldField.setAllowBlank(true);
        extendFieldField.setSample(quoto.getField());
        extendFieldField.setDesc("指标字段名称");
        extendFields.add(extendFieldField);

        //是否有变量
        if (quoto.getAdjective() != null && quoto.getAdjective().length > 0) {
            List<Adjective> adjectives = adjectiveMapper.queryAdjectiveByIds(quoto.getAdjective());
            List<Field> fields = new ArrayList<>();
            for (Adjective adjective : adjectives) {
                if (adjective.getReq_parm_type() == 1) {
                    if (adjective.getFields() != null && adjective.getFields().size() > 0) {
                        fields.addAll(adjective.getFields());
                    }
                }
            }
            if (fields.size() > 0) {
                ExtendField extendFieldParmValue = new ExtendField();
                extendFieldParmValue.setName("parm_value");
                extendFieldParmValue.setType("json");
                extendFieldParmValue.setAllowBlank(false);
                Map<String, Object> parm_value = new HashMap<>();
                String desc = "";
                for (int i = 0; i < fields.size(); i++) {
                    JSONObject jsonObject = (JSONObject) JSONObject.toJSON(fields.get(i));
                    if (jsonObject.get("type").equals("string")) {
                        parm_value.put(jsonObject.get("name").toString(), "XXXX");
                    } else if (jsonObject.get("type").equals("string")) {
                        String[] sample = {"XXX", "XXX"};
                        parm_value.put(jsonObject.get("name").toString(), sample);
                    } else if (jsonObject.get("type").equals("int")) {
                        parm_value.put(jsonObject.get("name").toString(), 10);
                    } else if (jsonObject.get("type").equals("int[]")) {
                        int[] sample = {1, 2};
                        parm_value.put(jsonObject.get("name").toString(), sample);
                    } else {
                        parm_value.put(jsonObject.get("name").toString(), null);
                    }

                    if (!StringUtils.isEmpty(jsonObject.get("desc").toString())) {
                        desc = desc + jsonObject.get("name").toString() + ":" + jsonObject.get("desc").toString();
                        if (i < fields.size() - 1) {
                            desc = desc + "\n";
                        }
                    }
                }
                extendFieldParmValue.setSample(parm_value);
                extendFieldParmValue.setDesc(desc);
                extendFields.add(extendFieldParmValue);
            }
        }

        ExtendField extendFieldOrder = new ExtendField();
        extendFieldOrder.setName("order");
        extendFieldOrder.setType("String[]");
        extendFieldOrder.setAllowBlank(true);
        String[] orders = {"XXX", "XXX"};
        extendFieldOrder.setSample(orders);
        extendFieldOrder.setDesc("排序的参数组合");
        if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            String desc = "可排序的参数名：";
            if(quoto.isUse_sql()){
                Map<String,Set<String>> columnAndGroup = getColumnAndGroup(quoto.getSql());
                if (!columnAndGroup.get("column").isEmpty()) {
                    for(String cl: columnAndGroup.get("column")){
                        desc = desc+cl+",";
                    }
                }
                if (!columnAndGroup.get("group").isEmpty()) {
                    for(String cl: columnAndGroup.get("group")){
                        desc = desc+cl+",";
                    }
                }
                if(desc.charAt(desc.length()-1)==','){
                    desc=desc.substring(0,desc.length()-1);
                }
            }else {
                Quoto originQuoto = quotoMapper.queryQuotoById(quoto.getOrigin_quoto());
                List<DimensionExtend> dimensionInfo = quotoMapper.queryDimensionByQuotoId(quoto.getId());
                desc = "可排序的参数名：" + originQuoto.getField();

                if (dimensionInfo != null && dimensionInfo.size() > 0) {
                    for (DimensionExtend dimensionExtend : dimensionInfo) {
                        desc = desc + "," + dimensionExtend.getCode();
                    }
                }
            }
            extendFieldOrder.setDesc(desc);
        }
        extendFields.add(extendFieldOrder);

        ExtendField extendFieldAcs = new ExtendField();
        extendFieldAcs.setName("acs");
        extendFieldAcs.setType("boolean");
        extendFieldAcs.setAllowBlank(true);
        extendFieldAcs.setSample("true");
        extendFieldAcs.setDesc("true升序 false降序 默认false");
        extendFields.add(extendFieldAcs);

        ExtendField extendFieldPage = new ExtendField();
        extendFieldPage.setName("page");
        extendFieldPage.setType("int");
        extendFieldPage.setAllowBlank(true);
        extendFieldPage.setSample(0);
        extendFieldPage.setDesc("页码从0开始,默认值为0");
        extendFields.add(extendFieldPage);

        ExtendField extendFieldCount = new ExtendField();
        extendFieldCount.setName("count");
        extendFieldCount.setType("int");
        extendFieldCount.setAllowBlank(true);
        extendFieldCount.setSample(1000);
        extendFieldCount.setDesc("每页的数量,默认值为1000");
        extendFields.add(extendFieldCount);
        quotoApiDoc.setRequestParament(extendFields);
        commonResponse.setData(quotoApiDoc);
        return commonResponse;
    }

    public Map<String,Set<String>> getColumnAndGroup(String sql){
        sql = sql.toLowerCase();
        Map<String,Set<String>> result = new HashMap<>();
        Set<String> columns = new HashSet<>();
        Set<String> groups = new HashSet<>();
        if (sql.indexOf("select ") != -1 && sql.indexOf(" from ") != -1) {
            String select = sql.substring(sql.indexOf("select ") + 7, sql.indexOf(" from ")).trim();
            String[] columnSelect = select.split(",");
            List<String> columnsTmp = new ArrayList<>();
            for(int i=0;i<columnSelect.length;i++){
                if(judgeIsRight(columnSelect[i])){
                    columnsTmp.add(columnSelect[i]);
                }else{
                    if(i<columnSelect.length-1){
                        columnSelect[i+1]=columnSelect[i]+columnSelect[i+1];
                    }
                }
            }
            for (String column : columnsTmp) {
                column = column.trim();
                if (column.contains(" as ")) {
                    column = column.substring(column.indexOf(" as ") + 4).trim().replace("\"", "");
                } else if (column.equals("*") || StringUtils.isEmpty(column)) {
                    continue;
                } else if (column.contains(".")) {
                    column = column.substring(column.indexOf(".") + 1);
                }
                columns.add(column);
            }

        }
        if (sql.lastIndexOf(" group by ") != -1) {
            String group = sql.substring(sql.lastIndexOf(" group by ") + 10).trim();
            int index = group.indexOf(" having");
            if (index == -1) {
                index = group.indexOf(" order ");
                if (index == -1) {
                    index = group.indexOf(" limit ");
                }
            }
            if (index != -1) {
                group = group.substring(0, index).trim();
            }
            for (String groupValue : group.split(",")) {
                groups.add(groupValue);
            }
        }
        groups.retainAll(columns);
        columns.removeAll(groups);
        result.put("group",groups);
        result.put("column",columns);
        return result;
    }
    public boolean judgeIsRight(String colunm){
        int a=0;
        int b=0;
        for(int i=0;i<colunm.length();i++){
            if(colunm.charAt(i)=='('){
                a=a+1;
            }else if(colunm.charAt(i)==')'){
                b=b+1;
            }
        }
        if(a==b){
            return true;
        }else{
            return false;
        }
    }
}
