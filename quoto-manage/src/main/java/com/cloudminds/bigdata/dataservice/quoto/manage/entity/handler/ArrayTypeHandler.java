package com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

public class ArrayTypeHandler extends BaseTypeHandler<int[]> {
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, int[] parameter, JdbcType jdbcType) throws SQLException {
    	String data="";
    	for(int j=0;j<parameter.length;j++) {
    		if(j==0) {
    			data=data+parameter[j];
    		}else {
    			data=data+","+parameter[j];
    		}
    	}
    	ps.setString(i, data);
    }

    @Override
    public int[] getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String str = rs.getString(columnName);
        if (rs.wasNull())
            return null;

        return Arrays.asList(str.split(",")).stream().mapToInt(Integer::parseInt).toArray();
    }

    @Override
    public int[] getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String str = rs.getString(columnIndex);
        if (rs.wasNull())
            return null;

        return Arrays.asList(str.split(",")).stream().mapToInt(Integer::parseInt).toArray();
    }

    @Override
    public int[] getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String str = cs.getString(columnIndex);
        if (cs.wasNull())
            return null;

        return Arrays.asList(str.split(",")).stream().mapToInt(Integer::parseInt).toArray();
    }
}

