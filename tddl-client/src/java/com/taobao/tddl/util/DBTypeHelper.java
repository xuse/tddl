package com.taobao.tddl.util;

import java.lang.reflect.Method;

import javax.sql.DataSource;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.jdbc.druid.TDruidDataSource;
import com.taobao.tddl.jdbc.group.DataSourceWrapper;
import com.taobao.tddl.jdbc.group.TGroupDataSource;

/**
 * 获取一下DbType
 * 
 * @author jianghang 2014-6-26
 * @since 3.3.2.2
 */
public class DBTypeHelper {

    private static Method getDbType = null;

    public static String getDbType(DataSource dataSource) {
        String dbType = "oracle";
        if (dataSource == null) {
            return dbType;
        }

        else if (dataSource instanceof TGroupDataSource) {
            TGroupDataSource tGroupDataSource = (TGroupDataSource) dataSource;
            DataSource ds = tGroupDataSource.getDataSourceMap().values().iterator().next();
            if (ds instanceof TDruidDataSource || ds instanceof DataSourceWrapper) {
                return getDbType(ds);
            }
        } else if (dataSource instanceof TDataSource) {
            TDataSource tDataSource = (TDataSource) dataSource;
            DataSource ds = tDataSource.getRuntimeConfigHolder().get().dsMap.values().iterator().next();
            return getDbType(ds);
        } else if (dataSource instanceof DataSourceWrapper) {
            DataSourceWrapper ds = (DataSourceWrapper) dataSource;
            if (ds.getWrappedDataSource() instanceof TDruidDataSource) {
                return getDbType(ds.getWrappedDataSource());
            }
        } else if (dataSource instanceof TDruidDataSource) {
            TDruidDataSource tdruidDataSource = (TDruidDataSource) dataSource;
            Object dbTypeEnum = invoke(tdruidDataSource);
            if (dbTypeEnum == null) {
                return dbType;
            } else if (dbTypeEnum.getClass()
                .getName()
                .equals("com.taobao.tddl.jdbc.druid.config.object.DruidDbTypeEnum")) {
                // tddl 3.1版本 返回的是这个类型
                if ("ORACLE".equals(dbTypeEnum.toString())) {
                    return "oracle";
                } else if ("MYSQL".equals(dbTypeEnum.toString())) {
                    return "mysql";
                }
            } else if (dbTypeEnum.getClass().getName().equals("com.taobao.tddl.common.standard.atom.AtomDbTypeEnum")) {
                // tddl 3.1版本 返回的是这个类型
                if ("ORACLE".equals(dbTypeEnum.toString())) {
                    return "oracle";
                } else if ("MYSQL".equals(dbTypeEnum.toString())) {
                    return "mysql";
                }
            }
        }

        return dbType;
    }

    private static Object invoke(TDruidDataSource tdruidDataSource) {
        try {
            if (getDbType == null) {
                getDbType = tdruidDataSource.getClass().getDeclaredMethod("getDbType", new Class[] {});
                getDbType.setAccessible(true);
            }
            return getDbType.invoke(tdruidDataSource, new Object[] {});
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
