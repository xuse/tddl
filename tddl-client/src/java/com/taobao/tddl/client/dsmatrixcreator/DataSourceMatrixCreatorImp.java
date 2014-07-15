package com.taobao.tddl.client.dsmatrixcreator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.jdbc.TddlRuntime;
import com.taobao.tddl.common.ConfigServerHelper;
import com.taobao.tddl.common.DataSourceChangeListener;
import com.taobao.tddl.common.TDDLConstant;
import com.taobao.tddl.common.client.util.DataSourceType;
import com.taobao.tddl.common.config.ConfigDataHandler;
import com.taobao.tddl.common.config.ConfigDataHandlerFactory;
import com.taobao.tddl.common.config.ConfigDataListener;
import com.taobao.tddl.common.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.common.util.NamedThreadFactory;
import com.taobao.tddl.jdbc.group.TGroupDataSource;

/**
 * 用于根据一个或一组数据源Martrix key 来最终动态的创建数据源listener. 然后从这个listener可以动态的创建自己的数据源。
 * 因为这一层不知道group ds和atom ds之间的对应关系。所以最多只能支持2种模式。 1 . 给定一系列ds group matrix key.
 * 动态创建。 2 . 给定一系列atom ds的key. 动态创建。 这个是其中用于支持ds group matrix key 动态创建数据源的支持类。
 * 
 * @author shenxun
 */
public class DataSourceMatrixCreatorImp implements ConfigDataListener {
	//final static Pattern pattern = Pattern.compile("\\s*[|\t|\r|\n]");
	Log logger = LogFactory.getLog(DataSourceMatrixCreatorImp.class);
	@SuppressWarnings("unchecked")
	private volatile TddlRuntime matrixDSRuntime = new TddlRuntime(Collections.EMPTY_MAP);
	private volatile ConfigDataHandlerFactory cdhf;
	private String appName;
	private final String unitName;
	private DataSourceType dataSourceType;
	private ConfigDataHandler matrixHandler;
	private final List<DataSourceChangeListener> dsChangeListeners = new ArrayList<DataSourceChangeListener>(2);
	
	public DataSourceMatrixCreatorImp(DataSourceType dataSourceType,String unitName){
		this.dataSourceType =  dataSourceType;
		this.unitName = unitName;
	}
	
	public synchronized void setNewDSMatrixKey(String appName) {
		setAppName(appName);
		logger.warn("receive a ds keys,follow will add to map " + appName);
		String configKey = ConfigServerHelper.getDBGroupsConfig(this.appName);
		cdhf = ConfigDataHandlerCity.getFactory(this.appName);
		matrixHandler = cdhf.getConfigDataHandlerWithListener(configKey, this, unitName);
		String data;
		try {
			// 等10s
			data = matrixHandler.getData(TDDLConstant.DIAMOND_GET_DATA_TIMEOUT,ConfigDataHandler.FIRST_SERVER_STRATEGY);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (data == null) {
			throw new RuntimeException("can't find datasource map by appName :" + appName);
		}
		appendNewDataSourceMap(data);
		logger.warn("success");
	}

	private Set<String> getKey2Add(String[] tempArray, Set<String> oldHaveNewDont) {
		Set<String> newKeyToAdd = new HashSet<String>(Arrays.asList(tempArray));
		//因为是unmodifiedmap ，所以复制一份出来再修改
		Set<String> tempOldHaveNewDont = new HashSet<String>(oldHaveNewDont);
		for (String temp : tempArray) {
			if (temp == null || temp.length() == 0) {
				continue;
			}
			if (tempOldHaveNewDont.contains(temp)) {
				// 从旧的临时map里移出,剩下的为有问题的key
				tempOldHaveNewDont.remove(temp);
				// 从新的临时里移出，剩下的为需要新加入的key
				newKeyToAdd.remove(temp);
			}
		}
		if (!tempOldHaveNewDont.isEmpty()) {
			throw new IllegalArgumentException("only add is allowed" + tempOldHaveNewDont);
		}
		return newKeyToAdd;
	}

//	private Map<String, DataSource> getNewDataSourceMap(Set<String> new2add) {
//		Map<String, DataSource> newDsMap = new HashMap<String, DataSource>(new2add.size());
//
//		if (new2add.isEmpty()) {
//			return newDsMap;
//		}
//		for (String key : new2add) {
//			logger.warn("add a ds 2 map. key is " + key);
//			TGroupDataSource tgroupds = new TGroupDataSource();
//			tgroupds.setDbGroupKey(key);
//			tgroupds.setDataSourceType(dataSourceType);
//			tgroupds.setAppName(getAppName());
//			tgroupds.init();
//			newDsMap.put(key, tgroupds);
//		}
//		logger.warn("inited");
//		return newDsMap;
//	}
	
	private Map<String, DataSource> getNewDataSourceMap(Set<String> new2add) {
		Map<String, DataSource> newDsMap = new HashMap<String, DataSource>(
				new2add.size());

		if (new2add.isEmpty()) {
			return newDsMap;
		}

		final int t = Runtime.getRuntime().availableProcessors();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(t, t, 5,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("TDDL_INIT"),new ThreadPoolExecutor.CallerRunsPolicy());

		Map<String, Future<TGroupDataSource>> m = new HashMap<String, Future<TGroupDataSource>>();
		for (final String key : new2add) {
			Future<TGroupDataSource> f = executor
					.submit(new Callable<TGroupDataSource>() {
						@Override
						public TGroupDataSource call() throws Exception {
							logger.warn("add a ds 2 map. key is " + key);
							TGroupDataSource tgroupds = new TGroupDataSource();
							tgroupds.setDbGroupKey(key);
							tgroupds.setDataSourceType(dataSourceType);
							tgroupds.setAppName(getAppName());
							tgroupds.setUnitName(unitName);
							tgroupds.init();
							return tgroupds;
						}
					});
			m.put(key, f);
		}

		for (Map.Entry<String, Future<TGroupDataSource>> e : m.entrySet()) {
			try {
				newDsMap.put(e.getKey(), e.getValue().get());
			} catch (Exception e1) {
				throw new RuntimeException("init group ds error! group key is "
						+ e.getKey(), e1);
			}
		}

		executor.shutdown();
		logger.warn("inited");
		return newDsMap;
	}

	public void appendNewDataSourceMap(String data) {
		logger.warn("received data " + data);
		String[] keys = propertiesSpliter(data);
		if (keys == null) {
			logger.warn("keys is null ,do nothing");
			return;
		}
		Set<String> oldSet = matrixDSRuntime.dsMap.keySet();
		Set<String> new2AddSet = getKey2Add(keys, oldSet);
		Map<String, DataSource> newDSMap = getNewDataSourceMap(new2AddSet);
		Map<String, DataSource> oldDSMap = matrixDSRuntime.dsMap;
		newDSMap.putAll(oldDSMap);
		// runtime change
		this.matrixDSRuntime = new TddlRuntime(newDSMap);
		logger.warn("matrix refresh!");
	}

	public synchronized void onDataRecieved(String dataId,String data) {
		logger.warn("matrix ds data received !dataId:"+dataId+" data:"+data);
		appendNewDataSourceMap(data);
		for (DataSourceChangeListener dataSourceChangeListener : dsChangeListeners) {
			dataSourceChangeListener.onDataSourceChanged(matrixDSRuntime.dsMap);
		}
	}

	public void addPropertiesChangeListener(DataSourceChangeListener dataSourceChangeListener) {
		dsChangeListeners.add(dataSourceChangeListener);
	}

	public Map<String, DataSource> getDataSourceMap() {
		if (matrixDSRuntime == null) {
			throw new IllegalArgumentException("not inited");
		}
		return matrixDSRuntime.dsMap;
	}

	public synchronized void setDSMatrixeKey(String key) {
		String[] tempArray = propertiesSpliter(key);
		if (tempArray == null) {
			throw new IllegalArgumentException("input new dsMartrix is null");
		}
		if (matrixDSRuntime != null) {
			throw new IllegalArgumentException("already have matrixDS runtime");
		}
		logger.warn("init a new ds");
		Set<String> keySet = new HashSet<String>();
		Map<String, ConfigDataHandler> newMap = new HashMap<String, ConfigDataHandler>();
		for (String str : tempArray) {
			ConfigDataHandler cdh = cdhf.getConfigDataHandlerWithListener(str, this, unitName);
			newMap.put(str, cdh);
			String data = cdh.getData(TDDLConstant.DIAMOND_GET_DATA_TIMEOUT,ConfigDataHandler.FIRST_SERVER_STRATEGY);
			// 添加所有key
			keySet.addAll(Arrays.asList(propertiesSpliter(data)));
		}
		Map<String, DataSource> dsMap = getNewDataSourceMap(keySet);
		this.matrixDSRuntime = new TddlRuntime(dsMap);
		logger.warn("success");
	}

	public ConfigDataHandlerFactory getCdhf() {
		return cdhf;
	}

	public synchronized void setCdhf(ConfigDataHandlerFactory cdhf) {
		this.cdhf = cdhf;
	}

	public String getAppName() {
		return appName;
	}

	protected void setAppName(String appName) {
		if (this.appName != null) {
			if (!appName.equals(this.appName)) {
				throw new IllegalArgumentException("already have appName !" + appName);
			} else {
				logger.warn("set app name twice. " + appName);
			}
		}
		this.appName = appName;
	}

	public ConfigDataHandler getMatrixHandler() {
		return matrixHandler;
	}

	/**
	 * 切分peoperties 
	 * 
	 * @param target
	 * @return
	 */
	public static String[] propertiesSpliter(String target) {
		if (target == null) {
			return null;
		}
		String[] tokens = target.split(",");
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = tokens[i].trim();
		}
		return tokens;
	}

	public DataSourceType getDataSourceType() {
		return dataSourceType;
	}

	public void setDataSourceType(DataSourceType dataSourceType) {
		this.dataSourceType = dataSourceType;
	}
	
	public void destroyAll()
	{
		if(matrixHandler != null) {
			matrixHandler.closeUnderManager();
		}
		Map<String, DataSource> map = matrixDSRuntime.dsMap;
		for(DataSource ds : map.values())
		{
			if(ds instanceof TGroupDataSource)
			{
				try {
					((TGroupDataSource) ds).destroyDataSource();
				} catch (Exception e) {
					logger.error("we got exception when close .",e);
				}
			}
		}
	}
}
