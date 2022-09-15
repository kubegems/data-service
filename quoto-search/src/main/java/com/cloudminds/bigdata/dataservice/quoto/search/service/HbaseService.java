package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class HbaseService {
    @Autowired
    private Connection hbaseConnection;

    public boolean isExists(String tableName) {
        boolean tableExists = false;
        try {
            TableName table = TableName.valueOf(tableName);
            tableExists = hbaseConnection.getAdmin().tableExists(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableExists;
    }

    /**
     * 获取数据（根据rowkey）
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @return map
     */
    public Map<String, String> getData(String tableName, String rowKey) {
        HashMap<String, String> map = new HashMap<>();
        try {
            Table table = hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                for (Cell cell : result.listCells()) {
                    //列族
                    String family = Bytes.toString(cell.getFamilyArray(),
                            cell.getFamilyOffset(), cell.getFamilyLength());
                    //列
                    String qualifier = Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength());
                    //值
                    String data = Bytes.toString(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    map.put(family + ":" + qualifier, data);
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    public List<Map<String, Object>> getDataBatch(String tableName, List<String> rowKeys, Map<String, String> columnAttribute) {
        List<Map<String, Object>> dataResult = new ArrayList<Map<String, Object>>();
        if (rowKeys == null || rowKeys.size() == 0) {
            return dataResult;
        }
        List getList = new ArrayList<>();
        try {
            Table table = hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(tableName));
            rowKeys.forEach(rowKey -> {
                Get get = new Get(Bytes.toBytes(rowKey));
                getList.add(get);
            });
            Result[] results = table.get(getList);
            Arrays.stream(results).forEach(result -> {
                Map<String, Object> data = new HashMap<>();
                Arrays.stream(result.rawCells()).forEach(cell -> {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if(columnAttribute!=null&&(!StringUtils.isEmpty(columnAttribute.get(qualifier)))){
                        String type = columnAttribute.get(qualifier).toLowerCase();
                        if(type.contains("int")){
                            data.put(qualifier, Bytes.toInt(CellUtil.cloneValue(cell)));
                        }else if(type.contains("float")){
                            data.put(qualifier, Bytes.toFloat(CellUtil.cloneValue(cell)));
                        }else{
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            if(value.startsWith("{")&&value.endsWith("}")){
                                data.put(qualifier, JSONObject.parseObject(value));
                            }else{
                                data.put(qualifier, value);
                            }
                        }
                    }else{
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        if(value.startsWith("{")&&value.endsWith("}")){
                            data.put(qualifier, JSONObject.parseObject(value));
                        }else{
                            data.put(qualifier, value);
                        }
                    }
                });
                dataResult.add(data);
            });
        } catch (IOException e) {
            e.printStackTrace();
            return dataResult;
        }
        return dataResult;
    }

}
