package com.taobao.tddl.jdbc.druid;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.tddl.common.standard.atom.AtomDbStatusEnum;
import com.taobao.tddl.common.standard.atom.AtomDbTypeEnum;
import com.taobao.tddl.jdbc.druid.config.object.DruidDsConfDO;
import com.taobao.tddl.jdbc.druid.exception.DruidAlreadyInitException;

/**
 * 静态剥离的jboss数据源，不支持动态改参数
 * 主要用来方便测试
 * @author qihao
 *
 */
public class StaticTDruidDataSource extends AbstractTDruidDataSource {

	private static Log logger = LogFactory.getLog(StaticTDruidDataSource.class);
	/**
	 * 数据源配置信息
	 */
	private DruidDsConfDO confDO = new DruidDsConfDO();

	/**
	 * Jboss数据源通过init初始化
	 */
	private DruidDataSource druidDataSource;

	private volatile boolean init;
	
	@Override
	public void init(String appName, String dsKey, String unitName) throws Exception {
		init();
	}

	public void init() throws Exception {
		if (init) {
			throw new DruidAlreadyInitException("[AlreadyInit] double call Init !");
		}
		DruidDataSource localDruidDataSource = DruidDsConfHandle.convertTAtomDsConf2DruidConf(confDO.getIp(), confDO, confDO
				.getDbName());
		boolean checkPram = DruidDsConfHandle.checkLocalTxDataSourceDO(localDruidDataSource);
		if (checkPram) {
			localDruidDataSource.init();
			//druidDataSource = TaobaoDataSourceFactory.createLocalTxDataSource(localDruidDataSource);
			druidDataSource = localDruidDataSource;
			init = true;
		} else {
			throw new Exception("Init DataSource Error Pleace Check!");
		}
	}

	public void destroyDataSource() throws Exception {
		if (null != this.druidDataSource) {
			logger.warn("[DataSource Stop] Start!");
			this.druidDataSource.close();
			logger.warn("[DataSource Stop] End!");
		}
	}

	public void flushDataSource() {
		if (null != this.druidDataSource) {
			logger.warn("[DataSource Flush] Start!");
			DruidDataSource tempDataSource = this.druidDataSource.cloneDruidDataSource();
			this.druidDataSource.close();
			this.druidDataSource = tempDataSource;
			logger.warn("[DataSource Flush] End!");
		}
	}

	protected DataSource getDataSource() throws SQLException {
		return druidDataSource;
	}

	public String getIp() {
		return confDO.getIp();
	}

	public void setIp(String ip) {
		this.confDO.setIp(ip);
	}

	public String getPort() {
		return this.confDO.getPort();
	}

	public void setPort(String port) {
		this.confDO.setPort(port);
	}

	public String getDbName() {
		return this.confDO.getDbName();
	}

	public void setDbName(String dbName) {
		this.confDO.setDbName(dbName);
	}

	public String getUserName() {
		return this.confDO.getUserName();
	}

	public void setUserName(String userName) {
		this.confDO.setUserName(userName);
	}

	public String getPasswd() {
		return this.confDO.getPasswd();
	}

	public void setPasswd(String passwd) {
		this.confDO.setPasswd(passwd);
	}

	public String getDriverClass() {
		return this.confDO.getDriverClass();
	}

	public void setDriverClass(String driverClass) {
		this.confDO.setDriverClass(driverClass);
	}

	public String getSorterClass() {
		return this.confDO.getSorterClass();
	}

	public void setSorterClass(String sorterClass) {
		this.confDO.setSorterClass(sorterClass);
	}

	public int getPreparedStatementCacheSize() {
		return this.confDO.getPreparedStatementCacheSize();
	}

	public void setPreparedStatementCacheSize(int preparedStatementCacheSize) {
		this.confDO.setPreparedStatementCacheSize(preparedStatementCacheSize);
	}

	public int getMinPoolSize() {
		return this.confDO.getMinPoolSize();
	}

	public void setMinPoolSize(int minPoolSize) {
		this.confDO.setMinPoolSize(minPoolSize);
	}

	public int getMaxPoolSize() {
		return this.confDO.getMaxPoolSize();
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.confDO.setMaxPoolSize(maxPoolSize);
	}

	public int getBlockingTimeout() {
		return this.confDO.getBlockingTimeout();
	}

	public void setBlockingTimeout(int blockingTimeout) {
		this.confDO.setBlockingTimeout(blockingTimeout);
	}

	public long getIdleTimeout() {
		return this.confDO.getIdleTimeout();
	}

	public void setIdleTimeout(long idleTimeout) {
		this.confDO.setIdleTimeout(idleTimeout);
	}

	public void setDbType(String dbType) {
		this.confDO.setDbType(dbType);
	}

	public String getOracleConType() {
		return this.confDO.getOracleConType();
	}

	public void setOracleConType(String oracleConType) {
		this.confDO.setOracleConType(oracleConType);
	}

	public Map<String, String> getConnectionProperties() {
		return this.confDO.getConnectionProperties();
	}

	public void setConnectionProperties(Map<String, String> connectionProperties) {
		this.confDO.setConnectionProperties(connectionProperties);
	}

	@Override
	public AtomDbStatusEnum getDbStatus() {
		return confDO.getDbStautsEnum();
	}
	
	@Override
	public AtomDbTypeEnum getDbType() {
		return confDO.getDbTypeEnum();
	}
}
