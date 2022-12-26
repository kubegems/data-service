package com.cloudminds.bigdata.dataservice.quoto.config.service;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.MetaDataTableMapper;
import com.linkedin.common.FabricType;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.Domains;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class MetaDataService {
    @Autowired
    private MetaDataTableMapper metaDataTableMapper;
    @Autowired
    private RestEmitter emitter;
    private String hiveInstance = "prod_hive";

    public CommonResponse addTable(MetaDataTable metaDataTable) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (metaDataTable == null || StringUtils.isEmpty(metaDataTable.getName()) || StringUtils.isEmpty(metaDataTable.getDatabase_name()) || StringUtils.isEmpty(metaDataTable.getStorage_format())
                || StringUtils.isEmpty(metaDataTable.getModel_level()) || StringUtils.isEmpty(metaDataTable.getData_domain()) || StringUtils.isEmpty(metaDataTable.getDdl())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表名,库名,存储格式,模型层级,数据域,生成的ddl不能为空");
            return commonResponse;
        }
        //判断名字是否重复
        if (metaDataTableMapper.findMetaDataTableByName(metaDataTable.getDatabase_name(), metaDataTable.getName(), metaDataTable.getTable_type()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表已经存在了,请不要重复添加");
            return commonResponse;
        }

        //非默认存储位置需要传入存储位置
        if (!metaDataTable.isSystem_storage_location()) {
            if (StringUtils.isEmpty(metaDataTable.getStorage_location())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("非默认存储位置,存储位置不能为空");
                return commonResponse;
            }
        }

        //非默认分隔符需要传入分隔符
        if (!metaDataTable.isSystem_delimiter()) {
            if (StringUtils.isEmpty(metaDataTable.getDelimiter())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("非默认分隔符,分隔符不能为空");
                return commonResponse;
            }
        }
        //插入数据库
        if (metaDataTableMapper.insertMetaDataTable(metaDataTable) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表创建失败,请联系管理员");
            return commonResponse;
        }
        try {
            //去datahub上创建节点
            DatasetUrn datasetUrn = null;
            if(metaDataTable.getTable_type()==1) {
                datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), hiveInstance + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
            }else{
                return commonResponse;
            }
            //创建表
            StringMap customerPro = new StringMap();
            customerPro.put("CreateTime", new Date().toString());
            customerPro.put("Database", metaDataTable.getDatabase_name());
            customerPro.put("Owner", metaDataTable.getCreator());
            customerPro.put("OwnerType", "USER");
            DatasetProperties datasetProperties = new DatasetProperties().setName(metaDataTable.getName()).setCustomProperties(customerPro);
            addMetadata("dataset", datasetUrn, datasetProperties, emitter);
            //关联数据域
            Domains domains = new Domains();
            UrnArray urnArray = new UrnArray();
            urnArray.add(new Urn(metaDataTable.getData_domain()));
            domains.setDomains(urnArray);
            addMetadata("dataset", datasetUrn, domains, emitter);
            //设置表类型
            StringArray types = new StringArray();
            types.add("Table");
            SubTypes subTypes = new SubTypes();
            subTypes.setTypeNames(types);
            addMetadata("dataset", datasetUrn, subTypes, emitter);
        } catch (Exception e) {
            e.printStackTrace();
            return commonResponse;
        }

        return commonResponse;
    }

    public void addMetadata(String entityType, Urn urn, RecordTemplate recordTemplate, RestEmitter emitter) throws IOException, ExecutionException, InterruptedException {
        MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                .entityType(entityType)
                .entityUrn(urn)
                .upsert()
                .aspect(recordTemplate)
                .build();
        // Blocking call using future
        Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null);
        MetadataWriteResponse mwr = requestFuture.get();
        if (!mwr.isSuccess()) {
            HttpResponse httpResponse = (HttpResponse) mwr.getUnderlyingResponse();
            System.out.println(String.format("失败发送元数据事件: %s, aspect: %s 状态码是: %d",
                    mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
        }
    }

    public CommonResponse updateTable(MetaDataTable metaDataTable) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (metaDataTable == null || StringUtils.isEmpty(metaDataTable.getName()) || StringUtils.isEmpty(metaDataTable.getStorage_format())
                || StringUtils.isEmpty(metaDataTable.getModel_level()) || StringUtils.isEmpty(metaDataTable.getData_domain()) || StringUtils.isEmpty(metaDataTable.getDdl())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表名,存储格式,模型层级,数据域,生成的ddl不能为空");
            return commonResponse;
        }

        //查询原始表
        MetaDataTable oldMetaDataTable = metaDataTableMapper.findMetaDataTableById(metaDataTable.getId());
        if(oldMetaDataTable==null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始表不存在");
            return commonResponse;
        }

        //判断名字是否重复
        if(!oldMetaDataTable.getName().equals(metaDataTable.getName())) {
            if (metaDataTableMapper.findMetaDataTableByName(oldMetaDataTable.getDatabase_name(), metaDataTable.getName(), oldMetaDataTable.getTable_type()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("表已经存在了,请不要重复添加");
                return commonResponse;
            }
        }

        //非默认存储位置需要传入存储位置
        if (!metaDataTable.isSystem_storage_location()) {
            if (StringUtils.isEmpty(metaDataTable.getStorage_location())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("非默认存储位置,存储位置不能为空");
                return commonResponse;
            }
        }

        //非默认分隔符需要传入分隔符
        if (!metaDataTable.isSystem_delimiter()) {
            if (StringUtils.isEmpty(metaDataTable.getDelimiter())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("非默认分隔符,分隔符不能为空");
                return commonResponse;
            }
        }

        //更新表
        if(metaDataTableMapper.updateMetaDataTable(metaDataTable)<1){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败,请联系管理员");
            return commonResponse;
        }
        DatasetUrn datasetUrn = null;
        if(metaDataTable.getTable_type()==1) {
            datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), hiveInstance + "." + oldMetaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
        }else{
            return commonResponse;
        }

        if(!metaDataTable.getName().equals(oldMetaDataTable.getName())){
            try {
                //去datahub上创建节点
                //创建表
                StringMap customerPro = new StringMap();
                customerPro.put("CreateTime", new Date().toString());
                customerPro.put("Database", oldMetaDataTable.getDatabase_name());
                customerPro.put("Owner", metaDataTable.getCreator());
                customerPro.put("OwnerType", "USER");
                DatasetProperties datasetProperties = new DatasetProperties().setName(metaDataTable.getName()).setCustomProperties(customerPro);
                addMetadata("dataset", datasetUrn, datasetProperties, emitter);
                //关联数据域
                Domains domains = new Domains();
                UrnArray urnArray = new UrnArray();
                urnArray.add(new Urn(metaDataTable.getData_domain()));
                domains.setDomains(urnArray);
                addMetadata("dataset", datasetUrn, domains, emitter);
                //设置表类型
                StringArray types = new StringArray();
                types.add("Table");
                SubTypes subTypes = new SubTypes();
                subTypes.setTypeNames(types);
                addMetadata("dataset", datasetUrn, subTypes, emitter);
            } catch (Exception e) {
                e.printStackTrace();
                return commonResponse;
            }
        }else {
            //更新datahub
            if (!metaDataTable.getData_domain().equals(oldMetaDataTable.getData_domain())) {
                try {
                    Domains domains = new Domains();
                    UrnArray urnArray = new UrnArray();
                    urnArray.add(new Urn(metaDataTable.getData_domain()));
                    domains.setDomains(urnArray);
                    addMetadata("dataset", datasetUrn, domains, emitter);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return commonResponse;
    }

    public CommonResponse deleteTable(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        //查询原始表

        MetaDataTable metaDataTable = metaDataTableMapper.findMetaDataTableById(deleteReq.getId());
        if(metaDataTable==null){
            if((!StringUtils.isEmpty(deleteReq.getDatabase_name()))&&(!StringUtils.isEmpty(deleteReq.getName()))){
                metaDataTable = metaDataTableMapper.findMetaDataTableByName(deleteReq.getDatabase_name(), deleteReq.getName(), deleteReq.getTable_type());
            }
            if(metaDataTable==null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原始表不存在");
                return commonResponse;
            }
        }
        if(metaDataTableMapper.deleteMetaDataTableById(metaDataTable.getId())<1){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse findTable(QueryMetaDataTableReq queryMetaDataTableReq) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "m.deleted=0";
        if(!StringUtils.isEmpty(queryMetaDataTableReq.getDatabase_name())){
            condition = condition + " and m.database_name='"+queryMetaDataTableReq.getDatabase_name()+"'";
        }
        if(queryMetaDataTableReq.getTheme_id()>0){
            condition = condition + " and m.theme_id="+queryMetaDataTableReq.getTheme_id();
        }
        if(queryMetaDataTableReq.getTable_type()>0){
            condition = condition + " and m.table_type="+queryMetaDataTableReq.getTable_type();
        }
        if(!StringUtils.isEmpty(queryMetaDataTableReq.getModel_level())){
            condition = condition + " and m.model_level='"+queryMetaDataTableReq.getModel_level()+"'";
        }
        if(!StringUtils.isEmpty(queryMetaDataTableReq.getData_domain())){
            condition = condition + " and m.data_domain='"+queryMetaDataTableReq.getData_domain()+"'";
        }
        commonResponse.setData(metaDataTableMapper.findMetaDataTable(condition));
        return commonResponse;
    }

    public CommonResponse findTableByTableName(int table_type, String database_name, String table_name) {
        CommonResponse commonResponse = new CommonResponse();
        if(StringUtils.isEmpty(database_name)||StringUtils.isEmpty((table_name))){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("库名和表名不能为空");
            return commonResponse;
        }
        if(table_type!=1){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("暂不支持的表类型");
            return commonResponse;
        }
        commonResponse.setData(metaDataTableMapper.findMetaDataTableByName(database_name,table_name,table_type));
        return commonResponse;
    }

    public CommonResponse analysisFile(MultipartFile file) {
        CommonResponse commonResponse = new CommonResponse();
        List<Column> columns = new ArrayList<>();
        try {
            CSVReader csvReader = new CSVReaderBuilder(
                    new BufferedReader(
                            new InputStreamReader(file.getInputStream(), "utf-8"))).build();
            Iterator<String[]> iterator = csvReader.iterator();
            while (iterator.hasNext()) {
                String[] next = iterator.next();
                Column column = new Column();
                if(next.length>=1){
                    column.setName(next[0]);
                }
                if(next.length>=2){
                    column.setZh_name(next[1]);
                }
                if(next.length>=3){
                    column.setType(next[2]);
                }
                if(next.length>=4){
                    column.setLength(Integer.parseInt(next[3]));
                }
                if(next.length>=5){
                    column.setDesc(next[4]);
                }
                columns.add(column);
            }
            commonResponse.setData(columns);
            return commonResponse;
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
    }
}
