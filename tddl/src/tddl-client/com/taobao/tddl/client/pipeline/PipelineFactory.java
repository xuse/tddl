//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * @description 管线工厂接口定义,AbstractPipelineFactory实现了这个接口,
 *              但这个实现类是抽象类,getPipeline留给具体的子类去实现,以达到
 *              定义不同handler的管线以及提供一些相同服务的目的
 *           
 *              一般实现自定义PipelineFactory不直接实现本接口,继承
 *              AbstractPipelineFactory是一种正确且简便的方法.
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-15下午03:45:43
 */
public interface PipelineFactory {
	/**
	 * 取得管线
	 * @return
	 */
	public Pipeline getPipeline();
	
	/**
	 * 设置默认sqlDispatcher
	 * @param defaultDispatcher
	 */
	public void setDefaultDispatcher(SqlDispatcher defaultDispatcher);
	
	/**
	 * 设定dispatcherMap,初始化时候使用
	 * @param dispatcherMap
	 */
	public void setDispatcherMap(Map<String, SqlDispatcher> dispatcherMap);
	
	/**
	 * 根据指定的规则键，选择规则
	 * @param selectKey
	 * @return
	 * @throws SQLException
	 */
	public SqlDispatcher selectSqlDispatcher(String selectKey) throws SQLException;
	
	/**
	 * sql预解析，自动判定是否自动执行
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public DirectlyRouteCondition sqlPreParse(String sql) throws SQLException ;
	
	/**
	 * 支持多规则多表的DBType混用
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public DBType decideDBType(String sql,SqlDispatcher sqlDispatcher)throws SQLException;
}
