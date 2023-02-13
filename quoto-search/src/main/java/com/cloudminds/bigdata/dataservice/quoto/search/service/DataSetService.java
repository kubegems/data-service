package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.search.config.CsvExportUtil;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.*;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.DataSetMapper;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.DataSetTaskMapper;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.SearchMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
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
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

@Service
public class DataSetService {
    @Autowired
    private DataSetMapper dataSetMapper;
    @Autowired
    private DataSetTaskMapper dataSetTaskMapper;
    @Autowired
    private ESQueryService eSQueryService;
    @Autowired
    private SearchMapper searchMapper;
    @Autowired
    private SaveCsvDataService saveCsvData;
    @Value("${dataServiceUrl}")
    private String dataServiceUrl;
    @Value("${ckDataSetDB}")
    private String ckDataSetDB;
    @Value("${ckUrl}")
    private String ckUrl;
    @Value("${ckUser}")
    private String ckUser;
    @Value("${ckPassword}")
    private String ckPassword;
    @Value("${hiveUrl}")
    private String hiveUrl;
    @Value("${hiveUser}")
    private String hiveUser;
    @Value("${hivePassword}")
    private String hivePassword;
    @Autowired
    RestTemplate restTemplate;
    @Autowired
    HttpServletResponse response;

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
        commonResponse.setData(directory.getId());
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
        if (StringUtils.isEmpty(directory.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("文件夹名不能为空");
            return commonResponse;
        }
        if (directory.getPid() != oldDirectory.getPid()) {
            //校验pid是否存在
            if (directory.getPid() == directory.getId()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("父文件夹不能为自己");
                return commonResponse;
            }
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
        String conditionDataset = "deleted=0";
        if (pid != -1) {
            condition = condition + " and pid=" + pid;
            conditionDataset = conditionDataset + " and directory_id=" + pid;
        }
        if (!StringUtils.isEmpty(creator)) {
            condition = condition + " and creator='" + creator + "'";
            conditionDataset = conditionDataset + " and creator='" + creator + "'";
        }
        Map<String, Object> data = new HashMap<>();
        data.put("directory", dataSetMapper.queryDirectory(condition));
        data.put("dataset", dataSetMapper.queryAllDataSet(conditionDataset));
        commonResponse.setData(data);
        return commonResponse;
    }

    public CommonResponse addDataset(DataSetAddReq dataSet) {
        CommonResponse commonResponse = new CommonResponse();
        dataSet.setState(2);
        //校验参数是否为空
        if (dataSet == null || StringUtils.isEmpty(dataSet.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名称,创建者");
            return commonResponse;
        }
        if (dataSet.getData_type() != 3) {
            if(dataSet.getData_source_type()==null || (!dataSet.getData_source_type().equals("hive"))){
                if (StringUtils.isEmpty(dataSet.getData_source_name())){
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("数据来源不能为空");
                    return commonResponse;
                }
            }
            if (StringUtils.isEmpty(dataSet.getData_rule())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则不能为空");
                return commonResponse;
            }
            if (dataSet.getData_type() == 2) {
                if (dataSet.getData_connect_type() != 1) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("标签不支持的抽取连接类型");
                    return commonResponse;
                }
            }
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
        dataSet.setData_columns(JSONObject.parseArray(dataSet.getData_column_string(), Column.class));
        if (dataSet.getData_columns() == null || dataSet.getData_columns().isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("输出的列必须有值");
            return commonResponse;
        }

        //校验目录下的名字是否有重复
        if (dataSetMapper.findDataSetByByNameAndCreator(dataSet.getName(), dataSet.getCreator(), dataSet.getDirectory_id()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("该目录已有同名的数据集,请重新命名");
            return commonResponse;
        }
        //校验任务类型
        Iterator<String[]> iterator = null;
        if (dataSet.getData_type() == 2) {
            //校验规则
            AnalyseFilter analyseFilterReult = analyseFilter(dataSet.getData_rule(), dataSet.getData_source_id());
            if (!analyseFilterReult.isSuccess()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage(analyseFilterReult.getMessage());
                return commonResponse;
            }
            dataSet.setTag_enum_values(analyseFilterReult.getTag_enum_values());
            dataSet.setTag_item_complexs(analyseFilterReult.getTag_item_complexs());
            if (dataSet.getData_connect_type() != 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签类不支持抽取方式");
                return commonResponse;
            }
        } else if (dataSet.getData_type() == 1) {
            //校验sql
            String sql = dataSet.getData_rule().toLowerCase();
            if (sql.contains(" order by") || sql.contains(" limit")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("数据集里的sql不能写order by和limit");
                return commonResponse;
            }
        } else if (dataSet.getData_type() == 3) {
            dataSet.setState(0);
            dataSet.setMessage("排队等待上传");
            //csv文件上传
            if (dataSet.getData_connect_type() != 2) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("csv只支持抽取");
                return commonResponse;
            }
            //判断文件是否为空
            if (dataSet.getFile() == null || dataSet.getFile().isEmpty()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("csv文件必传且不能为空");
                return commonResponse;
            }
        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("暂不支持的创建方式");
            return commonResponse;
        }

        //抽取创建表
        if (dataSet.getData_connect_type() == 2) {
            //创建表
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            Date date = new Date();
            String tablePrex = "sql_";
            if (dataSet.getData_type() == 3) {
                tablePrex = "csv_";
            }
            String tableName = tablePrex + format.format(date.getTime());
            dataSet.setMapping_ck_table(tableName);
            String[] createSql = createTableSql(dataSet.getData_columns(), tableName);
            Connection conn = null;
            PreparedStatement pStemt = null;
            try {
                if (dataSet.getData_type() == 3) {
                    CSVReader csvReader = new CSVReaderBuilder(
                            new BufferedReader(
                                    new InputStreamReader(dataSet.getFile().getInputStream(), "utf-8"))).build();
                    iterator = csvReader.iterator();
                    iterator.next();
                }
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                pStemt = conn.prepareStatement(createSql[0]);
                pStemt.execute();
                pStemt = conn.prepareStatement(createSql[1]);
                pStemt.execute();
            } catch (Exception e) {
                e.printStackTrace();
                commonResponse.setSuccess(false);
                commonResponse.setMessage(e.getMessage());
                return commonResponse;
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }

        //插入数据
        if (dataSetMapper.addDataSet(dataSet) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("新建数据集失败,请稍后再试");
            return commonResponse;
        }
        if (dataSet.getData_type() == 3) {
            csvInsertData(dataSet, iterator,dataSet.getFile().getOriginalFilename(),false);
        }
        commonResponse.setData(dataSet.getId());
        return commonResponse;
    }

    public void csvInsertData(DataSet dataSet, Iterator<String[]> iterator,String fileName,boolean cover) {
        saveCsvData.csvInsertData(dataSet, iterator,fileName,cover);
    }

    public String[] createTableSql(List<Column> data_columns, String table) {
        String sql="";
        for (int i = 0; i < data_columns.size(); i++) {
            Column column = data_columns.get(i);
            sql = sql + column.getName() + " ";
            if (column.getType().equals("int")) {
                sql = sql + "Int32";
            }else if (column.getType().equals("long")) {
                sql = sql + "Int64";
            } else if (column.getType().equals("float")) {
                sql = sql + "Float32";
            } else if (column.getType().equals("double")) {
                sql = sql + "Float64";
            } else {
                sql = sql + "String";
            }
            if (i != data_columns.size() - 1) {
                sql = sql + ",";
            }
        }
        String tableSql="create table " + ckDataSetDB+"."+table + " ON CLUSTER cm_ck_cluster ("+sql + ")ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/"+ ckDataSetDB+"."+table+"', '{replica}')"+" ORDER BY " + data_columns.get(0).getName();
        String vSql = "create table " + ckDataSetDB+".dis_"+table + " ON CLUSTER cm_ck_cluster ("+sql + ")ENGINE = Distributed('cm_ck_cluster', '"+ckDataSetDB+"', '"+table+"', rand())";

        return new String[]{tableSql,vSql};
    }

    public CommonResponse updateDataset(DataSetAddReq dataSet) {
        CommonResponse commonResponse = new CommonResponse();
        DataSet oldDataSet = dataSetMapper.findDataSetByById(dataSet.getId());
        dataSet.setMapping_ck_table(oldDataSet.getMapping_ck_table());
        //校验原始数据集
        if (oldDataSet == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        //校验参数是否为空
        if (dataSet == null || StringUtils.isEmpty(dataSet.getName())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名称,创建者");
            return commonResponse;
        }
        //判断文件是否为空
        if (dataSet.getFile() != null) {
            if (dataSet.getFile().isEmpty()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("csv文件必传且不能为空");
                return commonResponse;
            }
            if (dataSet.getData_type() != 3) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("只有创建方式为csv的才能再次上传数据");
                return commonResponse;
            }
            if (oldDataSet.getState() == 1 || oldDataSet.getState() == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("数据集正在或等待写入数据,请稍等再传数据");
                return commonResponse;
            }
        }

        if (dataSet.getData_type() != 3) {
            if(dataSet.getData_source_type()==null || (!dataSet.getData_source_type().equals("hive"))){
                if (StringUtils.isEmpty(dataSet.getData_source_name())){
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("数据来源不能为空");
                    return commonResponse;
                }
            }
            if (StringUtils.isEmpty(dataSet.getData_rule())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("规则不能为空");
                return commonResponse;
            }
            if (dataSet.getData_type() == 2) {
                if (dataSet.getData_connect_type() != 1) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("标签不支持的抽取连接类型");
                    return commonResponse;
                }
            }
        }
        dataSet.setData_columns(JSONObject.parseArray(dataSet.getData_column_string(), Column.class));
        if (dataSet.getData_columns() == null || dataSet.getData_columns().isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("输出的列必须有值");
            return commonResponse;
        }
        List<Column> oldColumn = new ArrayList<>();
        if (dataSet.isTable_reCreate() || dataSet.isCover()) {
            List<Column> columns = oldDataSet.getData_columns();
            if (columns != null && columns.size() > 0) {
                for (int i = 0; i < columns.size(); i++) {
                    Map<String, String> column = (Map<String, String>) columns.get(i);
                    Column columnNew = new Column();
                    columnNew.setName(column.get("name"));
                    columnNew.setType(column.get("type"));
                    oldColumn.add(columnNew);
                }
            }
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
        Iterator<String[]> iterator = null;
        //校验任务类型
        if (dataSet.getData_type() == 2) {
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
        } else if (dataSet.getData_type() == 1) {
            //校验sql
            String sql = dataSet.getData_rule().toLowerCase();
            if (sql.contains(" order by") || sql.contains(" limit")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("数据集里的sql不能写order by和limit");
                return commonResponse;
            }
        } else if (dataSet.getData_type() == 3) {
            //取出文件里的数据
            if (dataSet.getFile() != null) {
                try {
                    CSVReader csvReader = new CSVReaderBuilder(
                            new BufferedReader(
                                    new InputStreamReader(dataSet.getFile().getInputStream(), "utf-8"))).build();
                    iterator = csvReader.iterator();
                    //去除头
                    iterator.next();
                } catch (Exception e) {
                    e.printStackTrace();
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(e.getMessage());
                    return commonResponse;
                }
            }
        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("暂不支持的创建方式");
            return commonResponse;
        }
        //重建表或清除表数据
        if ((dataSet.getData_type() == 1 && dataSet.getData_connect_type() == 2 && dataSet.isTable_reCreate()) || (dataSet.getData_type() == 3 && dataSet.isCover())) {
            Connection conn = null;
            PreparedStatement pStemt = null;
            boolean deleteTable = false;
            try {
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                String sql = "";
                if (dataSet.getData_type() == 1) {
                    //删除表
                    sql = "drop table " +ckDataSetDB+"."+oldDataSet.getMapping_ck_table()+" on cluster cm_ck_cluster";
                    pStemt = conn.prepareStatement(sql);
                    pStemt.execute();
                    sql = "drop table " +ckDataSetDB+"."+"dis_"+oldDataSet.getMapping_ck_table()+" on cluster cm_ck_cluster";
                    pStemt = conn.prepareStatement(sql);
                    pStemt.execute();
                    deleteTable = true;
                    //重做表
                    String[] createSql = createTableSql(dataSet.getData_columns(), oldDataSet.getMapping_ck_table());
                    pStemt = conn.prepareStatement(createSql[0]);
                    pStemt.execute();
                    pStemt = conn.prepareStatement(createSql[1]);
                    pStemt.execute();
                } else {
                    sql = "truncate table " +ckDataSetDB+"."+oldDataSet.getMapping_ck_table()+" on cluster cm_ck_cluster";
                    pStemt = conn.prepareStatement(sql);
                    pStemt.execute();
                }

            } catch (Exception ee) {
                ee.printStackTrace();
                commonResponse.setSuccess(false);
                commonResponse.setMessage(ee.getMessage());
                if (deleteTable) {
                    //回退
                    try {
                        String[] createSql = createTableSql(oldColumn, oldDataSet.getMapping_ck_table());
                        pStemt = conn.prepareStatement(createSql[0]);
                        pStemt.execute();
                        pStemt = conn.prepareStatement(createSql[1]);
                        pStemt.execute();
                    } catch (Exception e) {
                        e.printStackTrace();
                        //更新任务为任务异常状态
                        dataSetMapper.updateDataSetState(-1, "抽取存储的数据表被删除,任务异常,请手动删除表", dataSet.getId());
                    }
                }
                return commonResponse;
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }
        //更新数据
        if (dataSetMapper.updateDataSet(dataSet) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新数据集失败,请稍后再试");
            return commonResponse;
        }
        if (oldDataSet.getData_type() == 3 && iterator != null) {
            //更新数据库状态
            dataSetMapper.updateDataSetState(0, "排队等待上传", dataSet.getId());
            dataSet.setData_rule(oldDataSet.getData_rule());
            csvInsertData(dataSet, iterator,dataSet.getFile().getOriginalFilename(),dataSet.isCover());
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

        if (dataSet.getState() == 1 || dataSet.getState() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集数据正在处理中,不能删除");
            return commonResponse;
        }
        List<DataSetTask> dataSetTasks = dataSetTaskMapper.findDataSetTaskByDatasetId(deleteReq.getId());
        if (dataSetTasks != null && dataSetTasks.size()>0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集下面有任务,请先去详情里的同步状态里删除");
            return commonResponse;
        }

        if (dataSetMapper.deleteDataSet(deleteReq.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集删除失败,请联系管理员");
            return commonResponse;
        }
        if ((!StringUtils.isEmpty(dataSet.getMapping_ck_table())) && dataSet.getState() != -1) {
            //删除中间表
            Connection conn = null;
            PreparedStatement pStemt = null;
            try {
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                pStemt = conn.prepareStatement("drop table " +ckDataSetDB+"."+dataSet.getMapping_ck_table()+" on cluster cm_ck_cluster");
                pStemt.execute();
                pStemt = conn.prepareStatement("drop table " +ckDataSetDB+".dis_"+dataSet.getMapping_ck_table()+" on cluster cm_ck_cluster");
                pStemt.execute();
            } catch (Exception ee) {
                ee.printStackTrace();
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
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
        commonResponse.setTag_item_complexs(tag_item_complexs.toArray(new String[tag_item_complexs.size()]));
        commonResponse.setTag_enum_values(tag_enum_values.toArray(new String[tag_enum_values.size()]));
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
        if (dataSet.getData_type() == 1 || dataSet.getData_type() == 3) {
            if (queryDataReq.getCount() > 50000) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("查数据count不能超过50000");
                return commonResponse;
            }
            //查询数据服务
            return queryDataService(queryDataReq, dataSet);
        } else if (dataSet.getData_type() == 2) {
            if (queryDataReq.getCount() > 5000) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("查数据count不能超过5000");
                return commonResponse;
            }
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
            if (dataSet.getData_connect_type() == 1) {
                sql = dataSet.getData_rule().toLowerCase().replaceAll("\n", " ");
                if (sql.contains(" group by") || whereHasAlias(sql)) {
                    sql = "select count(*) as total from (" + sql + ") source";
                } else {
                    String lowerSql = sql.toLowerCase();
                    int start = lowerSql.indexOf("select ");
                    int end = lowerSql.indexOf(" from ");
                    if (start == -1 || end == -1) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("规则里的sql不合法");
                        return commonResponse;
                    }
                    sql = sql.substring(0, 7) + "count(*) as total" + sql.substring(end);
                }
            } else {
                sql = "select count(*) as total from " +ckDataSetDB+".dis_"+dataSet.getMapping_ck_table();
            }
        } else {
            if (dataSet.getData_connect_type() == 1) {
                sql = dataSet.getData_rule().toLowerCase().replaceAll("\n", " ");
            } else {
                sql = "select * from " +ckDataSetDB+".dis_"+dataSet.getMapping_ck_table();
            }
            if (queryDataReq.getOrder() != null && queryDataReq.getOrder().size() > 0) {
                List<Column> columns = dataSet.getData_columns();
                Set<String> columnName = new HashSet<>();
                if (columns != null && columns.size() > 0) {
                    for (int i = 0; i < columns.size(); i++) {
                        Map<String, String> column = (Map<String, String>) columns.get(i);
                        columnName.add(column.get("name"));
                    }
                    for (String order : queryDataReq.getOrder()) {
                        if (!columnName.contains(order)) {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("不支持排序字段：" + order);
                            return commonResponse;
                        }
                    }
                    sql = sql + " order by " + StringUtils.join(queryDataReq.getOrder().toArray(), ",");
                } else {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("此数据集不支持排序列");
                    return commonResponse;
                }

            }
            sql = sql + " limit " + queryDataReq.getCount();
            if (queryDataReq.getPage() > 0) {
                sql = sql + " offset " + queryDataReq.getCount() * queryDataReq.getPage();
            }
        }
        ServicePathInfo servicePathInfo = new ServicePathInfo();
        if (dataSet.getData_connect_type() == 1) {
            servicePathInfo = dataSetMapper.queryServicePathInfo(dataSet.getData_source_id());
        } else {
            servicePathInfo.setPath("/data/quoto/get");
            servicePathInfo.setTableName(dataSet.getMapping_ck_table());
        }
        if (servicePathInfo == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标对应的服务不可用,请联系管理员排查");
            return commonResponse;
        }
        String url = dataServiceUrl + servicePathInfo.getPath();
        String bodyRequest = "{\"[]\":{\"" + servicePathInfo.getTableName() + "\":{\"@sql\":\"" + sql + "\"";
        bodyRequest = bodyRequest + "},\"page\":" + queryDataReq.getPage() + ",\"count\":" + queryDataReq.getCount() + "}}";
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
                        if (queryDataReq.getQuery() == 1) {
                            commonResponse.setData(list.get(0).getJSONObject(servicePathInfo.getTableName()).get("total"));
                        } else {
                            commonResponse.setData(list.get(0).get(servicePathInfo.getTableName()));
                        }
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

    public boolean whereHasAlias(String sql) {
        sql = sql.toLowerCase();
        if (!sql.contains(" where ")) {
            return false;
        }
        Set<String> columns = new HashSet<>();
        if (sql.indexOf("select ") != -1 && sql.indexOf(" from ") != -1) {
            String select = sql.substring(sql.indexOf("select ") + 7, sql.indexOf(" from ")).trim();
            String[] columnSelect = select.split(",");
            List<String> columnsTmp = new ArrayList<>();
            for (int i = 0; i < columnSelect.length; i++) {
                if (judgeIsRight(columnSelect[i])) {
                    columnsTmp.add(columnSelect[i]);
                } else {
                    if (i < columnSelect.length - 1) {
                        columnSelect[i + 1] = columnSelect[i] + columnSelect[i + 1];
                    }
                }
            }
            for (String column : columnsTmp) {
                column = column.trim();
                if (column.contains(" as ")) {
                    column = column.substring(column.indexOf(" as ") + 4).trim().replace("\"", "");
                    columns.add(column);
                }

            }
        }
        if (columns.isEmpty()) {
            return false;
        }
        String where = sql.substring(sql.lastIndexOf(" where ") + 7);
        for (String column : columns) {
            if (where.contains(column)) {
                return true;
            }
        }
        return false;
    }

    public static boolean judgeIsRight(String colunm) {
        int a = 0;
        int b = 0;
        for (int i = 0; i < colunm.length(); i++) {
            if (colunm.charAt(i) == '(') {
                a = a + 1;
            } else if (colunm.charAt(i) == ')') {
                b = b + 1;
            }
        }
        if (a == b) {
            return true;
        } else {
            return false;
        }
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
            dataInfoQueryReq.setScroll_search(queryDataReq.isScroll_search());
            String columnResult = "";
            List<Column> columns = dataSet.getData_columns();
            Map<String, String> columnAttribute = new HashMap<>();
            if (columns != null && columns.size() > 0) {
                for (int i = 0; i < columns.size(); i++) {
                    Map<String, String> column = (Map<String, String>) columns.get(i);
                    columnResult = columnResult + column.get("name");
                    if (i != columns.size() - 1) {
                        columnResult = columnResult + ",";
                    }
                }
            }
            dataInfoQueryReq.setColumn(columnResult);
        }
        return eSQueryService.queryDataInfo(JSON.toJSONString(dataInfoQueryReq));
    }

    public CommonResponse checkSql(CheckSqlReq checkSqlReq) {
        List<Column> columns = new ArrayList<>();
        String url = "";
        String user = "";
        String password = "";
        CommonResponse commonResponse = new CommonResponse();
        if (checkSqlReq == null || StringUtils.isEmpty(checkSqlReq.getSql())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("sql参数不能为空");
            return commonResponse;
        }
        if (checkSqlReq.isHive_type()) {
            url = hiveUrl;
            user = hiveUser;
            password = hivePassword;
        } else {
            if (checkSqlReq.getTable_id() > 0) {
                DbInfo dbInfo = dataSetMapper.queryDbInfo(checkSqlReq.getTable_id());
                if (dbInfo == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("此表没有数据配置数据库的连接信息,请联系管理员");
                    return commonResponse;
                }
                if (StringUtils.isEmpty(dbInfo.getDb_url()) || StringUtils.isEmpty(dbInfo.getDb_url()) || StringUtils.isEmpty(dbInfo.getUserName())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("此表配置的数据库的连接信息有缺失,请联系管理员");
                    return commonResponse;
                }
                url = dbInfo.getDb_url();
                user = dbInfo.getUserName();
                password = dbInfo.getPassword();
            } else {
                if (StringUtils.isEmpty(checkSqlReq.getUrl()) || StringUtils.isEmpty(checkSqlReq.getUrl()) || StringUtils.isEmpty(checkSqlReq.getPassword())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("数据源没有配置url和连接用户信息,请联系管理员");
                    return commonResponse;
                }
                url = checkSqlReq.getUrl();
                user = checkSqlReq.getUser();
                password = checkSqlReq.getPassword();
            }
        }

        //去连接库看字段信息
        Connection conn = null;
        PreparedStatement pStemt = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            String sql = checkSqlReq.getSql() + " limit 1";
            pStemt = conn.prepareStatement(sql);
            ResultSet set = pStemt.executeQuery();
            ResultSetMetaData resultSetMetaData = set.getMetaData();
            for (int i = 1; i < resultSetMetaData.getColumnCount() + 1; i++) {
                Column column = new Column();
                column.setName(resultSetMetaData.getColumnName(i));
                if (column.getName().contains(".")) {
                    column.setName(column.getName().substring(column.getName().indexOf(".") + 1));
                }
                String type = resultSetMetaData.getColumnTypeName(i).toLowerCase();
                if (type.startsWith("int") || type.startsWith("smallint") || type.startsWith("bigint") || type.startsWith("mediumint") || type.startsWith("integer") || type.startsWith("bit") || type.startsWith("serial") || type.startsWith("smallserial") || type.startsWith("bigserial")) {
                    type = "int";
                } else if (type.startsWith("float") || type.startsWith("double") || type.startsWith("decimal") || type.startsWith("numeric") || type.startsWith("real")) {
                    type = "float";
                } else {
                    type = "String";
                }
                column.setType(type);
                columns.add(column);
            }
            commonResponse.setData(columns);
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        } finally {
            try {
                pStemt.close();
                conn.close();
            } catch (Exception ee) {
                ee.printStackTrace();
            }
        }
        return commonResponse;
    }

    public CommonResponse dataAccount(DataAccountReq dataAccountReq) {
        Map<String, Object> data = new HashMap<>();
        CommonResponse commonResponse = new CommonResponse();
        if (dataAccountReq == null || StringUtils.isEmpty(dataAccountReq.getData_rule()) || StringUtils.isEmpty(dataAccountReq.getData_source_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("规则不能为空");
            return commonResponse;
        }
        QueryDataReq queryDataReq = new QueryDataReq();
        DataSet dataSet = new DataSet();
        queryDataReq.setQuery(1);
        dataSet.setData_rule(dataAccountReq.getData_rule());
        dataSet.setData_source_name(dataAccountReq.getData_source_name());
        dataSet.setData_source_id(dataAccountReq.getData_source_id());
        if (dataAccountReq.getData_type() == 1) {
            CommonResponse commonResponseCount = queryDataService(queryDataReq, dataSet);
            if (!commonResponseCount.isSuccess()) {
                return commonResponseCount;
            }
            dataSet.setData_rule("select * from " + dataAccountReq.getData_source_name());
            CommonResponse commonResponseTotal = queryDataService(queryDataReq, dataSet);
            if (!commonResponseTotal.isSuccess()) {
                return commonResponseTotal;
            }
            data.put("cnt", commonResponseCount.getData());
            data.put("total", commonResponseTotal.getData());
            commonResponse.setData(data);
            return commonResponse;
        } else if (dataAccountReq.getData_type() == 2) {
            CommonResponse commonResponseCount = querySearchService(queryDataReq, dataSet);
            if (!commonResponseCount.isSuccess()) {
                return commonResponseCount;
            }
            dataSet.setData_rule("{'op':'and','filter':[]}");
            CommonResponse commonResponseTotal = querySearchService(queryDataReq, dataSet);
            if (!commonResponseTotal.isSuccess()) {
                return commonResponseTotal;
            }
            data.put("cnt", commonResponseCount.getData());
            data.put("total", commonResponseTotal.getData());
            commonResponse.setData(data);
            return commonResponse;
        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持的数据类型");
            return commonResponse;
        }
    }

    public CommonResponse downloadData(int id) {
        CommonResponse commonResponse = new CommonResponse();
        DataSet dataSet = dataSetMapper.findDataSetByById(id);
        if (dataSet == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        List<Column> columns = dataSet.getData_columns();
        String title = "";
        Map<String, String> columnAttribute = new HashMap<>();
        if (columns != null && columns.size() > 0) {
            for (int i = 0; i < columns.size(); i++) {
                Map<String, String> column = (Map<String, String>) columns.get(i);
                title = title + column.get("name");
                if (i != columns.size() - 1) {
                    title = title + ",";
                }
            }
        }
        QueryDataReq queryDataReq = new QueryDataReq();
        queryDataReq.setQuery(2);
        queryDataReq.setPage(0);
        String name = dataSet.getName();
        CommonResponse commonResponseData = null;
        if (dataSet.getData_type() == 1 || dataSet.getData_type() == 3) {
            queryDataReq.setCount(50000);
            commonResponseData = queryDataService(queryDataReq, dataSet);
        } else {
            queryDataReq.setCount(10000);
            commonResponseData = querySearchService(queryDataReq, dataSet);
        }
        if (!commonResponseData.isSuccess()) {
            return commonResponseData;
        }
        List<Map<String, Object>> list = (List<Map<String, Object>>) commonResponseData.getData();
        try {
            OutputStream os = response.getOutputStream();
            CsvExportUtil.responseSetProperties(name, response);
            CsvExportUtil.doExport(list, title, title, os);
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return null;
    }

    public CommonResponse queryDataSetApiDoc(int id) {
        CommonResponse commonResponse = new CommonResponse();
        ApiDoc datasetApiDoc = new ApiDoc();
        datasetApiDoc.setService_path("/search/dataset/queryData");
        DataSet dataSet = dataSetMapper.findDataSetByById(id);
        if (dataSet == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        List<ExtendField> extendFields = new ArrayList<>();
        ExtendField extendFieldId = new ExtendField();
        extendFieldId.setName("id");
        extendFieldId.setType("int");
        extendFieldId.setAllowBlank(false);
        extendFieldId.setSample(id);
        extendFieldId.setDesc("数据集id值");
        extendFields.add(extendFieldId);

        ExtendField extendFieldQuery = new ExtendField();
        extendFieldQuery.setName("query");
        extendFieldQuery.setType("int");
        extendFieldQuery.setAllowBlank(true);
        extendFieldQuery.setSample(2);
        extendFieldQuery.setDesc("默认值为2查询数据明细,1查询总条数");
        extendFields.add(extendFieldQuery);

        if (dataSet.getData_type() == 1 || dataSet.getData_type() == 3) {
            ExtendField extendFieldPage = new ExtendField();
            extendFieldPage.setName("page");
            extendFieldPage.setType("int");
            extendFieldPage.setAllowBlank(true);
            extendFieldPage.setSample(0);
            extendFieldPage.setDesc("page从0页开始");
            extendFields.add(extendFieldPage);

            ExtendField extendFieldOrder = new ExtendField();
            extendFieldOrder.setName("order");
            extendFieldOrder.setType("String[]");
            extendFieldOrder.setAllowBlank(true);
            String[] orders = {"XXX", "XXX"};
            extendFieldOrder.setSample(orders);
            extendFieldOrder.setDesc("排序的参数组合");
            String desc = "可排序的参数名：";
            List<Column> columns = dataSet.getData_columns();
            desc = "可排序的参数名：";
            Map<String, String> columnAttribute = new HashMap<>();
            if (columns != null && columns.size() > 0) {
                for (int i = 0; i < columns.size(); i++) {
                    Map<String, String> column = (Map<String, String>) columns.get(i);
                    desc = desc + column.get("name");
                    if (i != columns.size() - 1) {
                        desc = desc + ",";
                    }
                }
            }
            extendFieldOrder.setDesc(desc);
            extendFields.add(extendFieldOrder);
        }
        ExtendField extendFieldCount = new ExtendField();
        extendFieldCount.setName("count");
        extendFieldCount.setType("int");
        extendFieldCount.setAllowBlank(true);
        extendFieldCount.setSample(10);
        if (dataSet.getData_type() == 1) {
            extendFieldCount.setDesc("返回的数据量,最大为50000条");
        } else {
            extendFieldCount.setDesc("返回的数据量,最大为5000条");
        }
        extendFields.add(extendFieldCount);

        if (dataSet.getData_type() == 2) {
            ExtendField extendFieldScroll = new ExtendField();
            extendFieldScroll.setName("scroll_id");
            extendFieldScroll.setType("String");
            extendFieldQuery.setAllowBlank(true);
            extendFieldScroll.setSample("");
            extendFieldScroll.setDesc("首次查询不传或传空串,后面查询传入前一次查询结果里的scroll_id值");
            extendFields.add(extendFieldScroll);
        }
        commonResponse.setData(extendFields);
        return commonResponse;
    }

    public CommonResponse addDatasetTask(DataSetTask dataSetTask) {
        CommonResponse commonResponse = new CommonResponse();
        if (dataSetTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("需要传任务的信息进来");
            return commonResponse;
        }
        if (dataSetMapper.findDataSetByById(dataSetTask.getData_set_id()) == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("数据集不存在");
            return commonResponse;
        }
        if (dataSetTaskMapper.addDataSetTask(dataSetTask) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务创建失败,请联系管理员");
            return commonResponse;
        }

        return commonResponse;
    }

    public CommonResponse updateDatasetTask(DataSetTask dataSetTask) {
        CommonResponse commonResponse = new CommonResponse();
        if (dataSetTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("参数不能为空");
            return commonResponse;
        }
        DataSetTask oldDataSetTask = dataSetTaskMapper.findDataSetTaskById(dataSetTask.getId());
        if (oldDataSetTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务不存在");
            return commonResponse;
        }
        if (dataSetTask.getData_set_id() != oldDataSetTask.getData_set_id()) {
            if (dataSetMapper.findDataSetByById(dataSetTask.getData_set_id()) == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("数据集不存在");
                return commonResponse;
            }
        }
        if (dataSetTaskMapper.updateDataSetTask(dataSetTask) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteDatasetTask(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (deleteReq == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("需要传数据集的id");
            return commonResponse;
        }
        DataSetTask dataSetTask = dataSetTaskMapper.findDataSetTaskById(deleteReq.getId());
        if (dataSetTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务不存在");
            return commonResponse;
        }
        if (dataSetTaskMapper.deleteDataSetTaskById(deleteReq.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务删除失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryDatasetTask(int id) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(dataSetTaskMapper.findDataSetTaskByDatasetId(id));
        return commonResponse;
    }

    public CommonResponse updateTaskState(UpdateTaskStateReq updateTaskStateReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (updateTaskStateReq == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("需要传任务信息");
            return commonResponse;
        }
        DataSetTask dataSetTask = dataSetTaskMapper.findDataSetTaskById(updateTaskStateReq.getId());
        if (dataSetTask == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务不存在");
            return commonResponse;
        }
        //更新
        if (dataSetTaskMapper.updateTaskState(updateTaskStateReq.getId(), updateTaskStateReq.getState(), updateTaskStateReq.getRun_info()) < 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("任务更新失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryDateSetById(int id) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(dataSetMapper.findDataSetByById(id));
        return commonResponse;
    }

    public CommonResponse querySysInfo() {
        CommonResponse commonResponse = new CommonResponse();
        Map<String,Object> result = new HashMap<>();
        result.put("ckDataSetDB",ckDataSetDB);
        commonResponse.setData(result);
        return commonResponse;
    }
}
