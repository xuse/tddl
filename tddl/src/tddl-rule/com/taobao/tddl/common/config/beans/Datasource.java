package com.taobao.tddl.common.config.beans;

/**
 * 一个数据源配置的抽象
 * 
 * 优先级： 1. beanId 2. jndiName 3. jdbcUrl，userName，passWord
 * 
 * @author linxuan
 */
public class Datasource {
	private String beanId;

	private String jndiName;

	private String jdbcUrl;
	private String userName;
	private String password;

	public String getBeanId() {
		return beanId;
	}

	public void setBeanId(String beanId) {
		this.beanId = beanId;
	}

	public String getJndiName() {
		return jndiName;
	}

	public void setJndiName(String jndiName) {
		this.jndiName = jndiName;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
