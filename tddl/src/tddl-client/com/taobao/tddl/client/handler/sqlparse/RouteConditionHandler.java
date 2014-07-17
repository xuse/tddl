//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler.sqlparse;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.handler.AbstractHandler;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.JoinCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.RuleRouteCondition;

/**
 * @description 这个handler主要是根据RouteCondition生成sql解析结果,
 *              功能与SqlParserHandler平行,但与其不同的是,这个handler
 *              只是模拟了下SqlParser的行为,其解析出来的分库分表字段
 *              以及其他属性(参见SimpleCondition及其子类)是用户手动 指定,而不是根据sql解析得出.
 * 
 *              用户指定分库分表字段可以实例话一个SimpleCondition,并且
 *              设定相应属性,最终将实例放入ThreadLocalMap即可(详见示例).
 * 
 *              这样可以达到的一种效果就是sql中不需要加入分库分表字段,即可
 *              进行分库分表,但是ThreadLocalMap很容易被干扰(因为每次执行后
 *              即被清掉,所以如果在正式sql前执行一条不相关的sql,导致正式
 *              执行sql时,SimpleCondition已经被清除从而出错),所以不推荐这么 使用.
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.4
 * @since 1.6
 * @date 2010-09-08下午03:33:32
 */
public class RouteConditionHandler extends AbstractHandler {
	public static final String HANDLER_NAME = "RouteConditionHandler";
	private final Log log = LogFactory.getLog(RouteConditionHandler.class);

	/**
	 * RouteConditionHandler只会对NOSQLPARSE类型的执行进行处理，其余全部略过
	 */
	public void handleDown(DataBus dataBus) throws SQLException {
		FlowType flowType = getPipeLineRuntimeInfo(dataBus).getFlowType();
		if (FlowType.NOSQLPARSE == flowType || FlowType.DBANDTAB_RC == flowType
			|| FlowType.BATCH_NOSQLPARSER == flowType) {
			parse(dataBus);
		}
	}

	/**
	 * 解析SQL入口，RouteCondition主要是得到一个SqlParserResult结构以便 之后的Handler使用。
	 * 
	 * @param dataBus
	 */
	protected void parse(DataBus dataBus) {
		PipelineRuntimeInfo runtime = getPipeLineRuntimeInfo(dataBus);
		RouteCondition rc = runtime.getStartInfo().getRc();

		if (rc instanceof RuleRouteCondition) {
			SqlParserResult sqlParserResult = ((RuleRouteCondition) rc)
					.getSqlParserResult();

			setResult(sqlParserResult, false, runtime);

			if (rc instanceof JoinCondition) {
				runtime.setVirtualJoinTableNames(((JoinCondition) rc)
						.getVirtualJoinTableNames());
			}
		} else {
			throw new IllegalArgumentException("wrong RouteCondition type:"
					+ rc.getClass().getName());
		}

		debugLog(log, new Object[] { "route condition sql parse end." });
	}

	/**
	 * 设置结果
	 * 
	 * @param sqlParserResult
	 * @param logicTableName
	 * @param isRealSqlParsed
	 * @param isAllowReverseOutput
	 * @param runtime
	 */
	private void setResult(SqlParserResult sqlParserResult,
			boolean isRealSqlParsed, PipelineRuntimeInfo runtime) {
		runtime.setSqlParserResult(sqlParserResult);
		runtime.setLogicTableNames(sqlParserResult.getTableName());
		runtime.setIsSqlParsed(isRealSqlParsed);
	}
}
