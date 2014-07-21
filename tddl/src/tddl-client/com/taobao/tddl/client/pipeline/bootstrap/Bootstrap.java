//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline.bootstrap;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.databus.StartInfo;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.common.jdbc.ParameterContext;

/**
 * @description 管线任务流转启动器接口方法定义,提供给TStatementImp使用,
 *              包括普通执行计划生成流程,batch操作的targetSql list生成,
 *              提供给测试得到分库分表结果的方法.
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-09-02下午02:43:34
 */
public interface Bootstrap {
    /**
     * 正常执行SQL解析，规则计算
     * @param startInfo  包括sql,parameters,sqlType信息
     * @return
     * @throws SQLException
     */
	ExecutionPlan bootstrap(StartInfo startInfo) throws SQLException;
	
    /**
     * 执行无参batch sql,只需要取得目标sql集合即可
     * @param startInfo
     * @param needRowCopy
     * @param targetSqls
     * @param selectKey
     * @throws SQLException
     */
	void bootstrapForBatch(StartInfo startInfo, boolean needRowCopy,
			Map<String, List<String>> targetSqls, String selectKey)
			throws SQLException;

    /**
     * 执行有参数的batch sql,只需要取得目标sql集合即可
     * @param startInfo
     * @param needRowCopy
     * @param targetSqls
     * @param selectKey
     * @throws SQLException
     */
	void bootstrapForPrepareBatch(StartInfo startInfo, boolean needRowCopy,
			Map<String, Map<String, List<List<ParameterContext>>>> targetSqls,
			String selectKey) throws SQLException;

	/**
	 * 方便测试，根据RouteCondition取得目标数据库和表(即只进行sql解析和规则计算，
	 * 不进行执行计划生成)
	 * @param rc
	 * @param sqlDispatcher
	 * @return
	 * @throws SQLException
	 */
	DispatcherResult bootstrapForGetDBAndTabs(RouteCondition rc,
			SqlDispatcher sqlDispatcher) throws SQLException;

	/**
	 * 方便测试，根据具体sql和args取得目标数据库和表(只进行sql解析和规则计算，
	 * 不进行执行计划生成)
	 * @param sql
	 * @param args
	 * @param sqlDispatcher
	 * @return
	 * @throws SQLException
	 */
	DispatcherResult bootstrapForGetDBAndTabs(String sql, List<Object> args,
			SqlDispatcher sqlDispatcher) throws SQLException;
}
