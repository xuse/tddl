package com.taobao.tddl.common.jdbc.conurl;

import com.alibaba.common.lang.StringUtil;

/**数据源连接地址解析类
 * @author qihao
 *
 */
public abstract class ConnectionURLParser {
	
	static String ORACLE_FIX="jdbc:oracle:";
	static String ORACLE_THIN_FIX="jdbc:oracle:thin:@";
	static String ORACLE_OCI_FIX="jdbc:oracle:oci:@";
	static String MYSQL_FIX="jdbc:mysql://";
	
	public static ConnectionURL parserConnectionURL(String url){
		ConnectionURL connectionURL=null;
		if(StringUtil.isBlank(url)){
			return connectionURL;
		}
		if(StringUtil.contains(url, ORACLE_FIX)){
			OracleConnectionURL oracleConUrl=new OracleConnectionURL();
			if(StringUtil.contains(url, ORACLE_THIN_FIX)){
				//jdbc:oracle:thin:@IP:Port:SID
				String dbinfo=StringUtil.substringAfterLast(url, "@");
				String[] ipPortAndSid=StringUtil.split(dbinfo,":");
				oracleConUrl.setIp(ipPortAndSid[0]);
				oracleConUrl.setPort(ipPortAndSid[1]);
				oracleConUrl.setDbName(ipPortAndSid[2]);
				oracleConUrl.setConType(OracleConnectionURL.THIN_TYPE);
			}else if(StringUtil.contains(url, ORACLE_OCI_FIX)){
				if(StringUtil.contains(url, "(")){
					//jdbc:oracle:oci:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.13.41.90)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICAT(SID=tddltest)))
					String ip=StringUtil.substringBefore(StringUtil.substringAfter(url, "HOST="), ")");
					String port=StringUtil.substringBefore(StringUtil.substringAfter(url, "PORT="), ")");
					oracleConUrl.setIp(ip);
					oracleConUrl.setPort(port);
					if(StringUtil.contains(url, "SID=")){
						oracleConUrl.setConType(OracleConnectionURL.OCI_IP_PORT_SID_TYPE);
						oracleConUrl.setDbName(StringUtil.substringBefore(StringUtil.substringAfter(url, "SID="), ")"));
					}else{
						oracleConUrl.setConType(OracleConnectionURL.OCI_IP_PORT_NAME_TYPE);
						oracleConUrl.setDbName(StringUtil.substringBefore(StringUtil.substringAfter(url, "SERVICE_NAME="), ")"));
					}
				}else{
					//jdbc:oracle:oci:@SID，这种方式无法获得IP和端口
					oracleConUrl.setDbName(StringUtil.substringAfterLast(url, "@"));
					oracleConUrl.setConType(OracleConnectionURL.OCI_SID_TYPE);
				}
			}
			connectionURL=oracleConUrl;
		}else if(StringUtil.contains(url, MYSQL_FIX)){
			//jdbc:mysql://hostname:port/dbname?param1=value1&m2=value2
			MySqlConnectionURL mySqlConURL=new MySqlConnectionURL();
			mySqlConURL.setPramStr(StringUtil.substringAfter(url, "?"));
			//截取DBName
			String dbInfoString=StringUtil.substringBefore(url, "?");
			String dbName=StringUtil.substringAfterLast(dbInfoString, "/");
			mySqlConURL.setDbName(dbName);
			//截取IP和PORT
			String hostString=StringUtil.substringBeforeLast(dbInfoString, "/");
			hostString=StringUtil.substringAfterLast(hostString, MYSQL_FIX);
			String[] ipAndPort=StringUtil.split(hostString,":");
			mySqlConURL.setIp(ipAndPort[0]);
			mySqlConURL.setPort(ipAndPort[1]);
			connectionURL=mySqlConURL;
		}
		return connectionURL;
	}
}
