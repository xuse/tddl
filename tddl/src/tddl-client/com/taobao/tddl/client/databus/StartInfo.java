package com.taobao.tddl.client.databus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * 存放一次查询或者一次数据更新开始时外部传入的数据
 * 
 * @author junyu
 * 
 */
public class StartInfo {
	/**
	 * 原始sql
	 */
	private String sql;

	/**
	 * 参数表
	 */
	private Map<Integer, ParameterContext> sqlParam;

	/**
	 * sql类型
	 */
	private SqlType sqlType;

	/**
	 * 自动提交
	 */
	private boolean autoCommit;
	
	/**
	 * 数据库类型
	 */
	private DBType dbType;

	/**
	 * 是否是有参数的批量执行
	 */
	private boolean isParameterBatch = false;

	/**
	 * 目标sql和参数，其中key为原始sql,value中的key是目标sql,List为参数列表
	 * batch使用
	 */
	private Map<String, Map<String, List<List<ParameterContext>>>> targetSqls;

	/**
	 * 不带参数的sql列表，batch使用
	 */
	private Map<String, List<String>> targetSqlsNoParameter;

	/**
	 * sql参数
	 */
	private List<Object> sqlArgs;
	
	/**
	 * 特殊条件
	 */
	private RouteCondition rc ;
	
	/**
	 * 直接指定Rule或者指定数据源绕开sql解析和路由计算
	 */
	private DirectlyRouteCondition directlyRouteCondition;

	/**
	 * 将参数通过List的形式交给业务
	 * 
	 * @return
	 */
	public List<Object> getSqlParameters() {
		if (sqlParam != null) {
			List<Object> parameters = new ArrayList<Object>();
			for (ParameterContext context : sqlParam.values()) {
				if (context.getParameterMethod() != ParameterMethod.setNull1
						&& context.getParameterMethod() != ParameterMethod.setNull2) {
					parameters.add(context.getArgs()[1]);
				} else {
					parameters.add(null);
				}

			}
			return parameters;
		} else if (sqlArgs != null) {
			return sqlArgs; // getDBAndTables测试会用到
		} else {
			return Collections.emptyList();
		}
	}

	public boolean isParameterBatch() {
		return isParameterBatch;
	}

	public void setParameterBatch(boolean isParameterBatch) {
		this.isParameterBatch = isParameterBatch;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Map<Integer, ParameterContext> getSqlParam() {
		return sqlParam;
	}

	public void setSqlParam(Map<Integer, ParameterContext> sqlParam) {
		this.sqlParam = sqlParam;
	}

	public SqlType getSqlType() {
		return sqlType;
	}

	public void setSqlType(SqlType sqlType) {
		this.sqlType = sqlType;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}
	
	public Map<String, Map<String, List<List<ParameterContext>>>> getTargetSqls() {
		return targetSqls;
	}

	public void setTargetSqls(
			Map<String, Map<String, List<List<ParameterContext>>>> targetSqls) {
		this.targetSqls = targetSqls;
	}

	public Map<String, List<String>> getTargetSqlsNoParameter() {
		return targetSqlsNoParameter;
	}

	public void setTargetSqlsNoParameter(
			Map<String, List<String>> targetSqlsNoParameter) {
		this.targetSqlsNoParameter = targetSqlsNoParameter;
	}

	public void setSqlArgs(List<Object> sqlArgs) {
		this.sqlArgs = sqlArgs;
	}

	public RouteCondition getRc() {
		return rc;
	}

	public void setRc(RouteCondition rc) {
		this.rc = rc;
	}

	public DirectlyRouteCondition getDirectlyRouteCondition() {
		return directlyRouteCondition;
	}

	public void setDirectlyRouteCondition(
			DirectlyRouteCondition directlyRouteCondition) {
		this.directlyRouteCondition = directlyRouteCondition;
	}

	public DBType getDbType() {
		return dbType;
	}

	public void setDbType(DBType dbType) {
		this.dbType = dbType;
	}
}
