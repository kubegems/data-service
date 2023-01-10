package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.*;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.DataSetMapper;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.SearchMapper;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
public class DataSetService {
    @Autowired
    private DataSetMapper dataSetMapper;
    @Autowired
    private ESQueryService eSQueryService;
    @Autowired
    private SearchMapper searchMapper;
    @Value("${dataServiceUrl}")
    private String dataServiceUrl;
    @Autowired
    RestTemplate restTemplate;

    public CommonResponse addDirectory(Directory directory) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (StringUtils.isEmpty(directory.getName()) || StringUtils.isEmpty(directory.getCreator())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹名和创建者不能为空");
            return commonResponse;
        }
        //校验pid是否存在
        if (directory.getPid() != 0) {
            Directory pidDirectory = dataSetMapper.findDirectoryById(directory.getPid());
            if (pidDirectory == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("层级目录不存在");
                return commonResponse;
            }
            if (pidDirectory.getPid() != 0) {
                Directory pidPidDirectory = dataSetMapper.findDirectoryById(pidDirectory.getPid());
                if (pidPidDirectory == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("层级目录不存在");
                    return commonResponse;
                }
                if (pidPidDirectory.getPid() != 0) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("只支持到3个层级,请不要在这下面建文件夹啦");
                    return commonResponse;
                }
            }

        }
        //校验目录名字是否重复
        if (dataSetMapper.findDirectoryByNameAndCreator(directory.getName(), directory.getCreator(), directory.getPid()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹已经存在,请重新命名");
            return commonResponse;
        }

        //增加目录
        if (dataSetMapper.addDirectory(directory) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹创建失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse updateDirectory(Directory directory) {
        CommonResponse commonResponse = new CommonResponse();
        //校验文件夹是否存在
        Directory oldDirectory = dataSetMapper.findDirectoryById(directory.getId());
        if (oldDirectory == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹不存在");
            return commonResponse;
        }
        //校验参数是否为空
        if (StringUtils.isEmpty(directory.getName()) || StringUtils.isEmpty(directory.getCreator())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹名和创建者不能为空");
            return commonResponse;
        }
        if (directory.getPid() != oldDirectory.getPid()) {
            //校验pid是否存在
            if (directory.getPid() != 0) {
                Directory pidDirectory = dataSetMapper.findDirectoryById(directory.getPid());
                if (pidDirectory == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("层级目录不存在");
                    return commonResponse;
                }
                if (pidDirectory.getPid() != 0) {
                    Directory pidPidDirectory = dataSetMapper.findDirectoryById(pidDirectory.getPid());
                    if (pidPidDirectory == null) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("层级目录不存在");
                        return commonResponse;
                    }
                    if (pidPidDirectory.getPid() != 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("只支持到3个层级,此文件夹下不能再放文件夹了");
                        return commonResponse;
                    }
                }
            }
            //校验名称
            if (dataSetMapper.findDirectoryByNameAndCreator(directory.getName(), directory.getCreator(), directory.getPid()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("文件夹已经存在,请重新命名");
                return commonResponse;
            }
        } else {
            if (!directory.getName().equals(oldDirectory.getName())) {
                //校验名称
                if (dataSetMapper.findDirectoryByNameAndCreator(directory.getName(), directory.getCreator(), directory.getPid()) != null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("文件夹已经存在,请重新命名");
                    return commonResponse;
                }
            }
        }
        //更新目录
        if (dataSetMapper.updateDirectory(directory) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹更新失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteDirectory(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        Directory directory = dataSetMapper.findDirectoryById(deleteReq.getId());
        //校验文件夹是否存在
        if (directory == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹不存在");
            return commonResponse;
        }
        //校验文件夹下是否存在子文件夹
        List<Directory> directoryList = dataSetMapper.findDirectoryByPid(directory.getId());
        if (directoryList != null && directoryList.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该文件夹下存在子文件夹,不能被删除");
            return commonResponse;
        }
        //校验文件夹下是否存在数据集
        List<DataSet> dataSetList = dataSetMapper.findDataSetByDirectoryId(directory.getId());
        if (dataSetList != null && dataSetList.size() > 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该文件夹下存在数据集,不能被删除");
            return commonResponse;
        }
        //删除文件夹
        if (dataSetMapper.deleteDirectory(deleteReq.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹删除失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryDirectory(String creator, int pid) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "deleted=0";
        if (pid != -1) {
            condition = condition + " and pid=" + pid;
        }
        if (!StringUtils.isEmpty(creator)) {
            condition = condition + " and creator='" + creator + "'";
        }
        commonResponse.setData(dataSetMapper.queryDirectory(condition));
        return commonResponse;
    }

    public CommonResponse addDataset(DataSet dataSet) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (dataSet == null || StringUtils.isEmpty(dataSet.getName()) || StringUtils.isEmpty(dataSet.getData_source_name()) || StringUtils.isEmpty(dataSet.getData_rule())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名称,数据来源名称,创建者,规则不能为空");
            return commonResponse;
        }
        //校验目录是否存在
        if (dataSet.getDirectory_id() != 0) {
            Directory directory = dataSetMapper.findDirectoryById(dataSet.getDirectory_id());
            if (directory == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("目录不存在在");
                return commonResponse;
            }
            if (!directory.getCreator().equals(dataSet.getCreator())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("只能建到自己的目录下");
                return commonResponse;
            }
        }
        //校验目录下的名字是否有重复
        if (dataSetMapper.findDataSetByByNameAndCreator(dataSet.getName(), dataSet.getCreator(), dataSet.getDirectory_id()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该目录已有同名的数据集,请重新命名");
            return commonResponse;
        }
        //校验任务类型
        if (dataSet.getData_type() == 2) {
            if (dataSet.getData_columns() == null || dataSet.getData_columns().size() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("列信息不能为空");
                return commonResponse;
            }
            //校验规则
            AnalyseFilter analyseFilterReult = analyseFilter(dataSet.getData_rule(), dataSet.getData_source_id());
            if (!analyseFilterReult.isSuccess()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage(analyseFilterReult.getMessage());
                return commonResponse;
            }
            dataSet.setTag_enum_values(analyseFilterReult.getTag_enum_values());
            dataSet.setTag_item_complexs(analyseFilterReult.getTag_item_complexs());
        } else if (dataSet.getData_type() != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("暂不支持的创建方式");
            return commonResponse;
        }
        //校验连接类型
        if (dataSet.getData_connect_type() != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持的连接类型");
            return commonResponse;
        }
        //插入数据
        if (dataSetMapper.addDataSet(dataSet) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("新建数据集失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse updateDataset(DataSet dataSet) {
        CommonResponse commonResponse = new CommonResponse();
        DataSet oldDataSet = dataSetMapper.findDataSetByById(dataSet.getId());
        //校验原始数据集
        if (oldDataSet == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        //校验参数是否为空
        if (dataSet == null || StringUtils.isEmpty(dataSet.getName()) || StringUtils.isEmpty(dataSet.getData_source_name()) || StringUtils.isEmpty(dataSet.getData_rule())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名称,数据来源名称,创建者,规则不能为空");
            return commonResponse;
        }
        //校验目录是否存在
        if (dataSet.getDirectory_id() != 0 && oldDataSet.getDirectory_id() != dataSet.getDirectory_id()) {
            Directory directory = dataSetMapper.findDirectoryById(dataSet.getDirectory_id());
            if (directory == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("目录不存在在");
                return commonResponse;
            }
            if (!directory.getCreator().equals(dataSet.getCreator())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("只能建到自己的目录下");
                return commonResponse;
            }
        }
        //校验目录下的名字是否有重复
        if (dataSet.getDirectory_id() != oldDataSet.getDirectory_id() || (!dataSet.getName().equals(oldDataSet.getName()))) {
            if (dataSetMapper.findDataSetByByNameAndCreator(dataSet.getName(), dataSet.getCreator(), dataSet.getDirectory_id()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("该目录已有同名的数据集,请重新命名");
                return commonResponse;
            }
        }
        //校验任务类型
        if (dataSet.getData_type() == 2) {
            if (dataSet.getData_columns() == null || dataSet.getData_columns().size() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("列信息不能为空");
                return commonResponse;
            }
            if (!oldDataSet.getData_rule().equals(dataSet.getData_rule())) {
                //校验规则
                AnalyseFilter analyseFilterReult = analyseFilter(dataSet.getData_rule(), dataSet.getData_source_id());
                if (!analyseFilterReult.isSuccess()) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(analyseFilterReult.getMessage());
                    return commonResponse;
                }
                dataSet.setTag_enum_values(analyseFilterReult.getTag_enum_values());
                dataSet.setTag_item_complexs(analyseFilterReult.getTag_item_complexs());
            } else {
                dataSet.setTag_item_complexs(oldDataSet.getTag_item_complexs());
                dataSet.setTag_enum_values(oldDataSet.getTag_enum_values());
            }
        } else if (dataSet.getData_type() != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("暂不支持的创建方式");
            return commonResponse;
        }
        //校验连接类型
        if (dataSet.getData_connect_type() != 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持的连接类型");
            return commonResponse;
        }
        //插入数据
        if (dataSetMapper.updateDataSet(dataSet) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新数据集失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteDataset(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("需要传数据集的id");
            return commonResponse;
        }
        DataSet dataSet = dataSetMapper.findDataSetByById(deleteReq.getId());
        if (dataSet == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        if (dataSetMapper.deleteDataSet(deleteReq.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集删除失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryAllDateSet(String creator, int directory_id) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "deleted=0";
        if (directory_id != -1) {
            condition = condition + " and directory_id=" + directory_id;
        }
        if (!StringUtils.isEmpty(creator)) {
            condition = condition + " and creator='" + creator + "'";
        }
        commonResponse.setData(dataSetMapper.queryAllDataSet(condition));
        return commonResponse;
    }

    public CommonResponse queryDateSet(DataSetQuery dataSetQuery) {
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "deleted=0";
        if (!StringUtils.isEmpty(dataSetQuery.getCreator())) {
            condition = condition + " and creator='" + dataSetQuery.getCreator() + "'";
        }
        if (dataSetQuery.getData_type() != -1) {
            condition = condition + " and data_type=" + dataSetQuery.getData_type();
        }
        if (!StringUtils.isEmpty(dataSetQuery.getName())) {
            condition = condition + " and name like '" + dataSetQuery.getName() + "%'";
        }
        int startLine = (dataSetQuery.getPage() - 1) * dataSetQuery.getSize();
        commonQueryResponse.setCurrentPage(dataSetQuery.getPage());
        commonQueryResponse.setData(dataSetMapper.queryDataSet(condition, startLine, dataSetQuery.getSize()));
        commonQueryResponse.setTotal(dataSetMapper.queryDataSetCount(condition));
        return commonQueryResponse;
    }

    public AnalyseFilter analyseFilter(String request, int data_source_id) {
        AnalyseFilter commonResponse = new AnalyseFilter();
        Set<String> tag_item_complexs = new HashSet<>();
        Set<String> tag_enum_values = new HashSet<>();
        JSONObject jsonObjectRequest = JSONObject.parseObject(request);
        if (jsonObjectRequest == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("rule不是个json结构");
            return commonResponse;
        }
        JSONArray jsonArray = jsonObjectRequest.getJSONArray("filter");
        if (jsonArray == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("filter必须有值");
            return commonResponse;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject subJsonObjectRequest = jsonArray.getJSONObject(i);
            JSONArray subJsonArray = subJsonObjectRequest.getJSONArray("filter");
            if (subJsonArray == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("嵌套里的filter必须有值");
                return commonResponse;
            }
            if (subJsonArray.isEmpty()) {
                continue;
            }
            BoolQueryBuilder subBoolQuery = QueryBuilders.boolQuery();
            for (int j = 0; j < subJsonArray.size(); j++) {
                JSONObject finalJsonObject = subJsonArray.getJSONObject(j);
                //处理组合标签
                if (finalJsonObject.containsKey("item_complex_name")) {
                    String complexFilter = searchMapper.findTagItemComplexByName(finalJsonObject.getString("item_complex_name"), data_source_id);
                    if (StringUtils.isEmpty(complexFilter)) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("组合标签不存在：" + finalJsonObject.getString("item_complex_name"));
                        return commonResponse;
                    }
                    tag_item_complexs.add(finalJsonObject.getString("item_complex_name"));
                } else {
                    List<String> tagValues = JSONArray.parseArray(finalJsonObject.getString("tag_values"), String.class);
                    if (tagValues == null || tagValues.isEmpty()) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("tag_values必须有值");
                        return commonResponse;
                    }
                    tag_enum_values.addAll(tagValues);
                }
            }
        }
        if (tag_item_complexs.isEmpty() && tag_enum_values.isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("rule参数有问题,解析不出来标签");
            return commonResponse;
        }
        commonResponse.setTag_enum_values(tag_item_complexs.toArray(new String[tag_item_complexs.size()]));
        commonResponse.setTag_item_complexs(tag_enum_values.toArray(new String[tag_enum_values.size()]));
        return commonResponse;
    }

    public CommonResponse queryData(QueryDataReq queryDataReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (queryDataReq == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("id不能为空");
            return commonResponse;
        }
        DataSet dataSet = dataSetMapper.findDataSetByById(queryDataReq.getId());
        if (dataSet == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        if (queryDataReq.getQuery() != 1) {
            if (queryDataReq.getCount() <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("查数据count需大于0");
                return commonResponse;
            }
            if (queryDataReq.getPage() < 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("page需大于0");
                return commonResponse;
            }
        }
        if (dataSet.getData_type() == 1) {
            //查询数据服务
            return queryDataService(queryDataReq, dataSet);
        } else if (dataSet.getData_type() == 2) {
            //查询搜索服务
            return querySearchService(queryDataReq, dataSet);
        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该数据集不支持查询数据");
            return commonResponse;
        }
    }

    public CommonResponse queryDataService(QueryDataReq queryDataReq, DataSet dataSet) {
        CommonResponse commonResponse = new CommonResponse();
        String sql = "";
        if (queryDataReq.getQuery() == 1) {
            sql = "select count(*) from (" + dataSet.getData_rule() + ") source";
        } else {
            sql = "select * from (" + dataSet.getData_rule() + ") source";
            sql = sql + " LIMIT " + queryDataReq.getCount();
            if (queryDataReq.getPage() > 0) {
                sql = sql + " offset " + queryDataReq.getCount() * queryDataReq.getPage();
            }
        }
        ServicePathInfo servicePathInfo = dataSetMapper.queryServicePathInfo(dataSet.getData_source_id());
        if (servicePathInfo == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标对应的服务不可用,请联系管理员排查");
            return commonResponse;
        }
        String url = dataServiceUrl + servicePathInfo.getPath();
        String bodyRequest = "{'[]':{'" + servicePathInfo.getTableName() + "':{'@sql':'" + sql + "'";
        bodyRequest = bodyRequest + "},'page':" + queryDataReq.getPage() + ",'count':" + queryDataReq.getCount() + "}}";
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
                    return commonResponse;
                }
                List<JSONObject> list = JSONObject.parseArray(result.get("[]").toString(), JSONObject.class);
                if (list != null) {
                    if (list.size() == 1) {
                        commonResponse.setData(list.get(0).get(servicePathInfo.getTableName()));
                    } else {
                        List<Object> data = new ArrayList<Object>();
                        for (int i = 0; i < list.size(); i++) {
                            data.add(list.get(i).get(servicePathInfo.getTableName()));
                        }
                        commonResponse.setData(data);
                    }
                }
            }
        }
        return commonResponse;
    }

    public CommonResponse querySearchService(QueryDataReq queryDataReq, DataSet dataSet) {
        CommonResponse commonResponse = new CommonResponse();
        DataInfoQueryReq dataInfoQueryReq = JSONObject.toJavaObject(
                JSONObject.parseObject(dataSet.getData_rule()), DataInfoQueryReq.class);
        TagObject tagObject = searchMapper.queryTagObjectByCode(dataSet.getData_source_name());
        if (tagObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(dataSet.getData_source_name() + ":标签对象不存在");
            return commonResponse;
        }
        dataInfoQueryReq.setObject_code(tagObject.getCode());
        dataInfoQueryReq.setQuery(queryDataReq.getQuery());
        if (dataInfoQueryReq.getQuery() != 1) {
            dataInfoQueryReq.setPage(queryDataReq.getPage() + 1);
            dataInfoQueryReq.setCount(queryDataReq.getCount());
            dataInfoQueryReq.setScroll_id(queryDataReq.getScroll_id());
        }
        List<Column> columns = dataSet.getData_columns();
        String column = "";
        for (int i = 0; i < columns.size(); i++) {
            column = column + columns.get(i).getName();
            if (i != columns.size() - 1) {
                column = column + ",";
            }

        }
        dataInfoQueryReq.setColumn(column);
        return eSQueryService.queryDataInfo(JSON.toJSONString(dataInfoQueryReq));
    }

    public CommonResponse checkSql(CheckSqlReq checkSqlReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (checkSqlReq == null || StringUtils.isEmpty(checkSqlReq.getSql())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("sql参数不能为空");
            return commonResponse;
        }

        return commonResponse;
    }
}
