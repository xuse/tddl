package com.taobao.tddl.jdbc.group.druid;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.tddl.common.client.util.DataSourceType;
import com.taobao.tddl.jdbc.group.TGroupDataSource;

public class DruidDsChooseTest {
	@Test
    public void testDataSourceType(){
    	ApplicationContext context=new ClassPathXmlApplicationContext("/conf/DruidTGroupDataSource.xml");
    	TGroupDataSource tds=(TGroupDataSource) context.getBean("druidDs");
    	tds.init();
    }
	
	@Test
	public void testDataSourceTypeWithString(){
		TGroupDataSource tds=new TGroupDataSource();
		tds.setDbGroupKey("DAYU_GROUP");
		tds.setAppName("DAYU_APP");
		tds.setDataSourceType("DruidDataSource");
		tds.init();
	}
	
	@Test
	public void testDataSourceTypeWithEnum(){
		TGroupDataSource tds=new TGroupDataSource();
		tds.setDbGroupKey("DAYU_GROUP");
		tds.setAppName("DAYU_APP");
		tds.setDataSourceType(DataSourceType.DruidDataSource);
		tds.init();
	}
}
