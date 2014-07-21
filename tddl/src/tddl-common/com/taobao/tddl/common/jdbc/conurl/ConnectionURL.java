package com.taobao.tddl.common.jdbc.conurl;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.interact.rule.bean.DBType;

/**连接地址抽象类需要通过ConnectionURLParser 解析后获得具体的对象
 * @author qihao
 *
 */
public abstract class ConnectionURL {
	private String ip;

	private String port;

	private String dbName;

	public abstract DBType getDbType();

	public abstract String renderURL();

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = StringUtil.trim(ip);
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = StringUtil.trim(port);
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName =  StringUtil.trim(dbName);
	}
}
