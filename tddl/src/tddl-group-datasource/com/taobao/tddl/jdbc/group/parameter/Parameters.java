package com.taobao.tddl.jdbc.group.parameter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;

/**
 * @author yangzhu
 * 
 */
public class Parameters {
	public static void setParameters(PreparedStatement ps, Map<Integer, ParameterContext> parameterSettings) throws SQLException {
		ParameterMethod.setParameters(ps, parameterSettings);
	}

}
