package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.Column;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.CsvImportRecord;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.DataSet;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.DataSetMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class SaveCsvDataService {
    @Value("${ckUrl}")
    private String ckUrl;
    @Value("${ckUser}")
    private String ckUser;
    @Value("${ckPassword}")
    private String ckPassword;
    @Value("${ckDataSetDB}")
    private String ckDataSetDB;
    @Autowired
    private DataSetMapper dataSetMapper;

    @Async("csvTaskExecutor")
    public void csvInsertData(DataSet dataSet, Iterator<String[]> iterator, String fileName, boolean cover) {
        dataSetMapper.updateDataSetState(1, "上传中", dataSet.getId());
        //判断哪几列是String,需要加单引号
        Set<Integer> stringColumn = new HashSet<>();
        for (int j = 0; j < dataSet.getData_columns().size(); j++) {
            Column column = dataSet.getData_columns().get(j);
            if (column.getType().toLowerCase().equals("string")) {
                stringColumn.add(j);
            }
        }
        Connection conn = null;
        PreparedStatement pStemt = null;
        try {
            conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
            //导入数据
            int i = 0;
            String sql = "";
            String insertSqlStart = "insert into " + ckDataSetDB + "." + dataSet.getMapping_ck_table() + " values ";
            while (iterator.hasNext()) {
                i++;
                String[] next = iterator.next();
                for (int m : stringColumn) {
                    next[m] = "'" + next[m] + "'";
                }
                sql = sql + "(" + StringUtils.join(next, ",") + "),";
                if (i == 50000) {
                    //批量插入ck
                    pStemt = conn.prepareStatement(insertSqlStart + sql);
                    pStemt.execute();
                    i = 0;
                    sql = "";
                }
            }
            if (i != 0) {
                System.out.println(insertSqlStart + sql);
                pStemt = conn.prepareStatement(insertSqlStart + sql);
                pStemt.execute();
            }

        } catch (Exception e) {
            e.printStackTrace();
            //更新表状态
            dataSetMapper.updateDataSetState(3, e.getMessage(), dataSet.getId());
            return;
        } finally {
            try {
                pStemt.close();
                conn.close();
            } catch (Exception ee) {
                ee.printStackTrace();
            }
        }
        //更新表导入信息和状态
        String dataRule = dataSet.getData_rule();
        List<CsvImportRecord> csvImportRecordList = null;
        if (StringUtils.isEmpty(dataRule)) {
            csvImportRecordList = new ArrayList<>();
        } else {
            try {
                csvImportRecordList = JSONArray.parseArray(dataRule, CsvImportRecord.class);
            } catch (Exception e) {
                csvImportRecordList = new ArrayList<>();
            }
        }
        CsvImportRecord csvImportRecord = new CsvImportRecord();
        csvImportRecord.setName(fileName);
        csvImportRecord.setUserName(dataSet.getCreator());
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String createTime = dateFormat.format(now);//格式化然后放入字符串
        csvImportRecord.setTime(createTime);
        csvImportRecord.setCover(cover);
        csvImportRecordList.add(csvImportRecord);
        dataRule = JSON.toJSONString(csvImportRecordList);
        dataSetMapper.updateDataSetDataRule(dataSet.getId(), dataRule);
        dataSetMapper.updateDataSetState(2, "执行完成", dataSet.getId());
    }
}
