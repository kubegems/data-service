package com.cloudminds.bigdata.dataservice.quoto.config.handler;

import com.alibaba.nacos.api.utils.StringUtils;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ArrayVarcharHandlerSpecial extends BaseTypeHandler<String[]> {
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, String[] parameter, JdbcType jdbcType) throws SQLException {
    	String data="";
    	for(int j=0;j<parameter.length;j++) {
    		if(j==0) {
    			data=data+parameter[j];
    		}else {
    			data=data+";"+parameter[j];
    		}
    	}
    	ps.setString(i, data);
    }

    @Override
    public String[] getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String str = rs.getString(columnName);
        if (StringUtils.isEmpty(str))
            return null;

        return str.split(";");
    }

    @Override
    public String[] getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String str = rs.getString(columnIndex);
        if (StringUtils.isEmpty(str))
            return null;

        return str.split(";");
    }

    @Override
    public String[] getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String str = cs.getString(columnIndex);
        if (StringUtils.isEmpty(str))
            return null;

        return str.split(";");
    }
}

