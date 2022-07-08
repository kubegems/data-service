package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.BusinessProcess;
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
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums.StateEnum;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums.TypeEnum;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.QuotoQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DataCommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoAccessHistoryMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.utils.DateTimeUtils;
import com.cloudminds.bigdata.dataservice.quoto.manage.utils.QuotoCaculateUtils;

@Service
public class QuotoService {
    @Autowired
    private QuotoMapper quotoMapper;
    @Autowired
    private QuotoAccessHistoryMapper quotoAccessHistoryMapper;
    @Value("${dataServiceUrl}")
    private String dataServiceUrl;
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

    public CommonResponse queryAllBusiness() {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryAllBusiness());
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
        if (quotoMapper.queryBusiness(business.getName()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("此业务线已存在,请不要重复添加");
            return commonResponse;
        }
        try {
            quotoMapper.addBusiness(business);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteBusiness(DeleteReq deleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        //查询数据域
        if (deleteReq == null || deleteReq.getId() <= 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("id值不能为空");
            return commonResponse;
        }
        int id = deleteReq.getId();
        List<Business> dataDomain = quotoMapper.queryAllDataDomain(id);
        if (dataDomain != null && dataDomain.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("业务线下有数据域,不能删除");
            return commonResponse;
        }
        if (quotoMapper.deleteBusinessById(id) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse queryAllDataDomain(int businessId) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryAllDataDomain(businessId));
        return commonResponse;
    }

    public synchronized CommonResponse addDataDomain(DataDomain dataDomain) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(dataDomain.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据域名字不能为空");
            return commonResponse;
        }
        if (quotoMapper.queryDataDomain(dataDomain.getName(), dataDomain.getBusiness_id()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据域已存在,请不要重复添加");
            return commonResponse;
        }
        try {
            quotoMapper.addDataDomain(dataDomain);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据插入失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteDataDomain(DeleteReq deleteReq) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq == null || deleteReq.getId() <= 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("id值不能为空");
            return commonResponse;
        }
        int id = deleteReq.getId();
        //查询业务过程
        List<Business> businessProcess = quotoMapper.queryAllBusinessProcess(id);
        if (businessProcess != null && businessProcess.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据域下有业务过程,不能删除");
            return commonResponse;
        }
        if (quotoMapper.deleteDataDomainById(id) <= 0) {
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
        if (quotoMapper.queryBusinessProcess(businessProcess.getName(), businessProcess.getData_domain_id()) != null) {
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

    public CommonResponse queryAllDataService(Integer business_process_id) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        if(business_process_id==null) {
            commonResponse.setData(quotoMapper.queryAllDataService());
        }else {
            BusinessProcess businessProcess = quotoMapper.queryBusinessProcessById(business_process_id);
            if(businessProcess == null){
                commonResponse.setMessage("此业务过程不存在,请确认后再试");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            commonResponse.setData(quotoMapper.queryAllDataServiceByDataDomainId(businessProcess.getData_domain_id()));
        }
        return commonResponse;
    }

    public CommonResponse queryAllDimension(int tableId) {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoMapper.queryAllDimension(tableId));
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

        if (quotoMapper.findQuotoByField(quoto.getField()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字段已存在,请重新命名");
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
        } else if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            quoto.setState(StateEnum.active_state.getCode());
            if (quoto.getOrigin_quoto() <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原子指标的id必须有值");
                return commonResponse;
            }
        } else {
            quoto.setState(StateEnum.active_state.getCode());
            if (StringUtils.isEmpty(quoto.getExpression())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("加工方式必须有值");
                return commonResponse;
            }
            String expression = quoto.getExpression();
            expression = expression.replaceAll(" ", "").replaceAll("[(]", "").replaceAll("[)]", "")
                    .replaceAll("[+]", " ").replaceAll("[-]", " ").replaceAll("[*]", " ").replaceAll("[/]", " ");
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

        if (!quoto.getName().equals(oldQuoto.getName())) {
            if (quotoMapper.findQuotoByName(quoto.getName()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("名字已存在,请重新命名");
                return commonResponse;
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
                        .replaceAll("[+]", " ").replaceAll("[-]", " ").replaceAll("[*]", " ").replaceAll("[/]", " ");
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
        try {
            if (quotoMapper.updateQuoto(quoto) <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("编辑指标失败，请稍后再试！");
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("编辑指标失败，请稍后再试！");
        }
        return commonResponse;
    }

    public CommonQueryResponse queryQuoto(QuotoQuery quotoQuery) {
        // TODO Auto-generated method stub
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "deleted=0";
        if (quotoQuery.getType() != -1) {
            condition = condition + " and type=" + quotoQuery.getType();
        }

        if (quotoQuery.getName() != null && (!quotoQuery.getName().equals(""))) {
            condition = condition + " and name like '" + quotoQuery.getName() + "%'";
        }

        if (quotoQuery.getField() != null && (!quotoQuery.getField().equals(""))) {
            condition = condition + " and field like '" + quotoQuery.getField() + "%'";
        }

        if (quotoQuery.getBusinessId() != -1) {
            condition = condition + " and business_id=" + quotoQuery.getBusinessId();
        }

        if (quotoQuery.getState() != -1) {
            condition = condition + " and state=" + quotoQuery.getState();
        }

        condition = condition + " order by update_time desc";
        int page = quotoQuery.getPage();
        int size = quotoQuery.getSize();
        int startLine = (page - 1) * size;
        commonQueryResponse.setData(quotoMapper.queryQuoto(condition, startLine, size));
        commonQueryResponse.setCurrentPage(quotoQuery.getPage());
        commonQueryResponse.setTotal(quotoMapper.queryQuotoCount(condition));
        return commonQueryResponse;
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
                                             Set<String> order, Boolean acs) {
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
        commonResponse = queryDataFromDataService(quoto, page, count, order, acs);
        QuotoAccessHistory quotoAccessHistory = new QuotoAccessHistory();
        quotoAccessHistory.setQuoto_id(quoto.getId());
        quotoAccessHistory.setQuoto_name(quoto.getName());
        quotoAccessHistory.setBusiness(quoto.getBusiness_name());
        quotoAccessHistory.setDomain(quoto.getData_domain_name());
        quotoAccessHistory.setProcess(quoto.getBusiness_process_name());
        quotoAccessHistory.setLevel(quoto.getQuoto_level());
        quotoAccessHistory.setType(quoto.getType());
        quotoAccessHistory.setSuccess(commonResponse.isSuccess());
        quotoAccessHistory.setMessage(commonResponse.getMessage());
        quotoAccessHistoryMapper.insertAccessHistory(quotoAccessHistory);
        if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
            Quoto atomicQuoto = quotoMapper.queryQuotoById(quoto.getOrigin_quoto());
            quotoAccessHistory.setQuoto_id(atomicQuoto.getId());
            quotoAccessHistory.setQuoto_name(atomicQuoto.getName());
            quotoAccessHistory.setBusiness(atomicQuoto.getBusiness_name());
            quotoAccessHistory.setDomain(atomicQuoto.getData_domain_name());
            quotoAccessHistory.setProcess(atomicQuoto.getBusiness_process_name());
            quotoAccessHistory.setLevel(atomicQuoto.getQuoto_level());
            quotoAccessHistory.setType(atomicQuoto.getType());
            quotoAccessHistory.setSuccess(commonResponse.isSuccess());
            quotoAccessHistory.setMessage(commonResponse.getMessage());
            quotoAccessHistoryMapper.insertAccessHistory(quotoAccessHistory);
        }
        return commonResponse;
    }

    public DataCommonResponse queryDataFromDataService(Quoto quoto, int page, int count, Set<String> orders,
                                                       Boolean acs) {
        DataCommonResponse commonResponse = new DataCommonResponse();

        // 复合指标处理逻辑
        if (quoto.getType() == TypeEnum.complex_quoto.getCode()) {
            try {
                DataCommonResponse dataCommonResponse = caculate(quoto.getExpression().replace(" ", "") + "#", page,
                        count, orders, acs);
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
            // 添加维度的请求参数
            if (quoto.getDimension() != null && quoto.getDimension().length > 0) {
                String group = "'@group':'";
                bodyRequest = bodyRequest + ";";
                // 查询维度的名称
                List<Dimension> dimensionName = quotoMapper.queryDimensionName(quoto.getId());
                Set<String> dimensionSet = new HashSet<>();

                commonResponse.setDimensionIds(quoto.getDimension());
                int i = 0;
                for (Dimension dimension : dimensionName) {
                    dimensionSet.add(dimension.getAlias());
                    String columnRequest = dimension.getCode();
                    if (!dimension.getAlias().equals(dimension.getCode())) {
                        columnRequest = columnRequest + ":" + dimension.getAlias();
                    }
                    if (i == dimensionName.size() - 1) {
                        group = group + dimension.getAlias() + "'";
                        bodyRequest = bodyRequest + columnRequest + "'";
                    } else {
                        group = group + dimension.getAlias() + ",";
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
                List<Adjective> adjectives = quotoMapper.queryAdjective(quoto.getId());
                for (int i = 0; i < adjectives.size(); i++) {
                    bodyRequest = bodyRequest + "," + getAdjectiveReq(adjectives.get(i));
                }
            }
        } else {
            bodyRequest = bodyRequest + "'";
        }

        if (orders != null && orders.size() > 0) {
            boolean isOrder = true;
            for (String order : orders) {
                if (!commonResponse.getFields().contains(order) && !commonResponse.getDimensions().contains(order)) {
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
        headers.add("token","L0V91TZWH4K8YZPBBG3M");
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
        return commonResponse;
    }

    /**
     * 根据修饰词信息组装请求的参数
     *
     * @param adjective
     * @return
     */
    public String getAdjectiveReq(Adjective adjective) {
        // 1为时间修饰词
        if (adjective.getType() == 1) {
            String result = "'" + adjective.getName();
            if (adjective.getCode_name().equals("last1HOUR")) {
                result = result + ">=':'" + DateTimeUtils.getlast1HOUR() + "'";
            } else if (adjective.getCode_name().equals("last1DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(0) + "'";
            } else if (adjective.getCode_name().equals("last2DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-1) + "'";
            } else if (adjective.getCode_name().equals("last3DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-2) + "'";
            } else if (adjective.getCode_name().equals("last7DAY") || adjective.getCode_name().equals("last1WEEK")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-6) + "'";
            } else if (adjective.getCode_name().equals("last14DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-13) + "'";
            } else if (adjective.getCode_name().equals("last15DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-15) + "'";
            } else if (adjective.getCode_name().equals("last30DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-30) + "'";
            } else if (adjective.getCode_name().equals("last60DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-60) + "'";
            } else if (adjective.getCode_name().equals("last90DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-90) + "'";
            } else if (adjective.getCode_name().equals("last180DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-180) + "'";
            } else if (adjective.getCode_name().equals("last360DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-360) + "'";
            } else if (adjective.getCode_name().equals("last365DAY")) {
                result = result + ">=':'" + DateTimeUtils.getLastDateByDay(-365) + "'";
            } else if (adjective.getCode_name().equals("last1MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-1) + "'";
            } else if (adjective.getCode_name().equals("last2MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-2) + "'";
            } else if (adjective.getCode_name().equals("last3MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-3) + "'";
            } else if (adjective.getCode_name().equals("last6MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-6) + "'";
            } else if (adjective.getCode_name().equals("last7MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-7) + "'";
            } else if (adjective.getCode_name().equals("last8MONTH")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByMonth(-8) + "'";
            } else if (adjective.getCode_name().equals("last1YEAR")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByYear(-1) + "'";
            } else if (adjective.getCode_name().equals("last2YEAR")) {
                result = result + ">=':'" + DateTimeUtils.getlastDateByYear(-2) + "'";
            } else if (adjective.getCode_name().equals("ftDate(w)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateWeek() + "'";
            } else if (adjective.getCode_name().equals("ftDate(m)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateMonth() + "'";
            } else if (adjective.getCode_name().equals("ftDate(q)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateQuarter() + "'";
            } else if (adjective.getCode_name().equals("ftDate(y)")) {
                result = result + ">=':'" + DateTimeUtils.ftDateYear() + "'";
            } else if (adjective.getCode_name().equals("pre1MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-1) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre2MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-2) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre3MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-3) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre4MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-4) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre5MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-5) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre6MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-6) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre12MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-12) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre24MONTH")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByMonth(-24) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByMonth(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre1DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-1) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre2DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-2) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre3DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-3) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre7DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-7) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre14DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-14) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre15DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-15) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre30DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-30) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre60DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-60) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre90DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-90) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre180DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-180) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre360DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-360) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre365DAY")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getLastDateByDay(-365) + "\\',<\\'"
                        + DateTimeUtils.getLastDateByDay(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre1YEAR")) {
                result = result + "&{}':'>=\\'" + DateTimeUtils.getPreDateByYear(-1) + "\\',<\\'"
                        + DateTimeUtils.getPreDateByYear(0) + "\\''";
            } else if (adjective.getCode_name().equals("pre1QUARTER")) {
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
                String end = format.format(cal.getTime()) + " 00:00:00";
                cal.add(Calendar.MONTH, -3);
                String start = format.format(cal.getTime()) + " 00:00:00";
                result = result + "&{}':'>=\\'" + start + "\\',<\\'" + end + "\\''";
            }
            return result;
        }

        // 其它修饰词
        return "'" + adjective.getName() + adjective.getCode().substring(1);
    }

    /**
     * 解析计算公式获取数据
     *
     * @param str   计算公式
     * @param page  查询数据的页数
     * @param count 每页的条数
     * @return
     */
    public DataCommonResponse caculate(String str, int page, int count, Set<String> order, Boolean acs) {
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
                    numStack.push(getCalculateValue(num, page, count, order, acs));
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
                                                 Boolean acs) {
        if (NumberUtils.isNumber(fieldName)) {
            DataCommonResponse calculateValue = new DataCommonResponse();
            calculateValue.setData(new BigDecimal(fieldName));
            calculateValue.setType(3);
            return calculateValue;
        }
        DataCommonResponse commonResponse = queryQuotoData(0, null, fieldName, page, count, order, acs);
        if (!commonResponse.isSuccess()) {
            // 抛异常
            throw new UnsupportedOperationException("指标(" + fieldName + ")获取失败：" + commonResponse.getMessage());
        } else {
            return commonResponse;
        }

    }

}
