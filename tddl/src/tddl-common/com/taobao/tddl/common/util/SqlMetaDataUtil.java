package com.taobao.tddl.common.util;

import java.sql.PreparedStatement;
import java.sql.Statement;

import com.taobao.tddl.common.channel.SqlMetaData;
import com.taobao.tddl.common.standard.jdbc.TPreparedStatement;
import com.taobao.tddl.common.standard.jdbc.TStatement;

public class SqlMetaDataUtil {

	public static void fillStatement(Statement statement, SqlMetaData sqlMetaData){
		if(statement instanceof TStatement) ((TStatement)statement).fillMetaData(sqlMetaData);;
	}

	public static void fillStatement(PreparedStatement ps, SqlMetaData sqlMetaData){
		if(ps instanceof TPreparedStatement) ((TPreparedStatement)ps).fillMetaData(sqlMetaData);
	}
}
