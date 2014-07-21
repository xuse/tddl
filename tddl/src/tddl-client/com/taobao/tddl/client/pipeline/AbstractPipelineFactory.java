//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.common.SQLPreParser;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.parser.ParserCache;
import com.taobao.tddl.rule.bean.LogicTable;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * @description 抽象类,提供sqlDispatcher选择,sql类型选择,sql预解析和表级别
 *              数据库类型选择 等功能,继承PipelineFactory接口
 *              DefaultPipelineFactory和NewRulePipelineFactory继承此类,
 *              需要实现getPipeline()方法.
 *              
 *              自定义的PipelineFactory实现这个抽象类,以便提供自定义handler的
 *              pipeline
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-15下午03:24:42
 */
public abstract class AbstractPipelineFactory implements PipelineFactory {
	public static final Log logger=LogFactory.getLog(AbstractPipelineFactory.class);
	protected SqlDispatcher defaultDispatcher;
	protected Map<String, SqlDispatcher> dispatcherMap;
	
	private static final Pattern SELECT_FOR_UPDATE_PATTERN = Pattern.compile(
			"^select\\s+.*\\s+for\\s+update.*$", Pattern.CASE_INSENSITIVE);
	
	private static final ParserCache globalCache = ParserCache.instance();
	
	public abstract Pipeline getPipeline();

	public SqlDispatcher selectSqlDispatcher(String selectKey)
			throws SQLException {
		if (selectKey == null) {
			return defaultDispatcher;
		}
		SqlDispatcher sqlDispatcher = dispatcherMap.get(selectKey);
		if (sqlDispatcher == null) {
			throw new IllegalArgumentException("can't find selector by key :"
					+ selectKey);
		} else {
			return sqlDispatcher;
		}
	}

	public void setDefaultDispatcher(SqlDispatcher defaultDispatcher) {
		this.defaultDispatcher = defaultDispatcher;
	}

	public void setDispatcherMap(Map<String, SqlDispatcher> dispatcherMap) {
		this.dispatcherMap = dispatcherMap;
	}
	
	public DirectlyRouteCondition sqlPreParse(String sql) throws SQLException {
		//如果用户指定了ROUTE_CONDITION或者DB_SELECTOR，那么跳过预解析，防止干扰
		if (null != ThreadLocalMap.get(ThreadLocalString.ROUTE_CONDITION)
				|| null != ThreadLocalMap.get(ThreadLocalString.DB_SELECTOR)
				|| null != ThreadLocalMap.get(ThreadLocalString.RULE_SELECTOR)) {
			return null;
		}

		String firstTable = SQLPreParser.findTableName(sql);
		if (null != firstTable) {
			Map<String, LogicTable> logicTableMap = this.defaultDispatcher
					.getRoot().getLogicTableMap();
			if(null!=logicTableMap.get(firstTable)){
				return null;
			}	
		}

		logger.debug("no logic table in defaultDispather's logicTableMap,try to produce DirectlyRouteCondition");
		
		if(null==this.defaultDispatcher.getRoot()
				.getDefaultDBSelectorID()){
		    throw new SQLException("the defaultDispatcher have no defaultDbIndex");	
		}
		
		//表名不存在logicTable map中，那么生成Condition，指定执行dbIndex返回
		DirectlyRouteCondition condition = new DirectlyRouteCondition();
		condition.setDBId(this.defaultDispatcher.getRoot()
				.getDefaultDBSelectorID());
		
		return condition;
	}
	
	public DBType decideDBType(String sql,SqlDispatcher sqlDispatcher)throws SQLException{
		String firstTable = SQLPreParser.findTableName(sql);
		if (null != firstTable) {
			Map<String, LogicTable> logicTableMap = sqlDispatcher.getRoot().getLogicTableMap();
			DBType findInLogicTab=null;
			if(null!=logicTableMap.get(firstTable)){
				findInLogicTab=logicTableMap.get(firstTable).getDbType();
			}
			
			return findInLogicTab;
		}
		
		return (DBType) sqlDispatcher.getRoot().getDBType();
	}
}
