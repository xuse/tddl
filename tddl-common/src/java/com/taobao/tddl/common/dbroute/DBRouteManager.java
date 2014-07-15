//package com.taobao.tddl.common.dbroute;
//
//import java.util.List;
//
//import com.taobao.tddl.common.exception.DBRouterException;
//
//public class DBRouteManager {
//	private DBRouteConfig dbRouteConfig;
//
//	/**
//	 * @return Returns the dbRouteConfig.
//	 */
//	public DBRouteConfig getDbRouteConfig() {
//		return dbRouteConfig;
//	}
//
//	/**
//	 * @param dbRouteConfig
//	 *            The dbRouteConfig to set.
//	 */
//	public void setDbRouteConfig(DBRouteConfig dbRouteConfig) {
//		this.dbRouteConfig = dbRouteConfig;
//	}
//
//	public List<String> getSqlExecutors(DBRoute dbRoute, String sqlId)
//			throws DBRouterException {
//		if (dbRoute == null) {
//		throw new DBRouterException("必须指定DBRoute,不知道确切库可以用空DBRoute传入");
//		}
//		if (null == dbRoute && null == sqlId) {
//			throw new DBRouterException("无法确定路由，请检查参数！");
//		}
//
//		return getDbRouteConfig().routingDB(dbRoute, sqlId);
//	}
//}
