package com.taobao.tddl.client.jdbc.sqlexecutor.parallel;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.alibaba.common.lang.io.ByteArrayInputStream;
import com.taobao.tddl.common.TDDLConstant;
import com.taobao.tddl.common.config.ConfigDataHandler;
import com.taobao.tddl.common.config.ConfigDataHandlerFactory;
import com.taobao.tddl.common.config.ConfigDataListener;
import com.taobao.tddl.common.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.common.util.NamedThreadFactory;

/**
 *并行执行控制类，一个dbIndex一个线程池，延迟初始化，
 *线程池饱和策略采用主线程执行（FIXME:从并行串行对比
 *测试看来没问题，但线上没经验，需要试验）。
 *
 *持久配置中心配置两个属性useparallelexecute和parallelthreadcount
 *<code>
 *   useParallelExecute=true
 *   parallelThreadCount=10
 *</code>
 *
 *dataId格式为<code>com.taobao.tddl.jdbc.client.sqlexecutor.{0}</code>
 *生成dataId可以调用<code>ParallelDiamondConfigManager.getSqlExecutorKey(appName)</code>   
 *
 *上述属性也可以通过tddl配置管理界面直接配置，不必手写
 *
 * @author junyu
 * 
 */
public class ParallelDiamondConfigManager implements ConfigDataListener {
	private static Log logger = LogFactory
			.getLog(ParallelDiamondConfigManager.class);
	public static final String USE_PARALLEL_EXECUTE = "useparallelexecute";
	public static final String PARALLEL_THREAD_COUNT = "parallelthreadcount";

	private static MessageFormat PARALLEL_EXECUTOR_FORMAT = new MessageFormat(
			"com.taobao.tddl.jdbc.client.sqlexecutor.{0}");

	private static boolean useParallel = false;
	private static int esThreadCount = 10;
	private static int queueSize = 2;

	private static Map<String, ThreadPoolExecutor> esMap = new ConcurrentHashMap<String, ThreadPoolExecutor>();

	private boolean inited = false;

	/**
	 * 需要在TDataSource实例化时启动
	 */
	public ParallelDiamondConfigManager(String appName, String unitName) {
		init(appName, unitName);
	}

	protected void init(String appName, String unitName) {
		if (inited) {
			return;
		}

		ConfigDataHandlerFactory configHandlerFactory = ConfigDataHandlerCity.getFactory(appName);
		ConfigDataHandler dataHandler = configHandlerFactory
				.getConfigDataHandlerWithListener(getSqlExecutorKey(appName),this,unitName);
		String data = null;

		try {
			data = dataHandler.getData(TDDLConstant.DIAMOND_GET_DATA_TIMEOUT,ConfigDataHandler.FIRST_SERVER_STRATEGY);
		} catch (Exception e) {
			logger.error("[PARALLEL_EXECUTE]try to get diamond config error.",
					e);
		}

		if (StringUtil.isBlank(data)) {
			logger.warn("no parallel execute info, set useParallel false");
			setUseParallelFalse();
			return;
		}

		logger.warn("[INIT]recieve parallel execute config,start to init.data:"
				+ data);
		configChange(data, true);
		inited = true;
		logger.warn("[INIT]init parallel execute info success!");
	}

	/**
	 * 对收到的配置进行解析初始化
	 * 
	 * @param data
	 * @param isInit
	 */
	protected synchronized void configChange(String data, boolean isInit) {
		Properties prop = parseConfigStr2Prop(data.toLowerCase());

		/**
		 * 安全起见，如果开启并行执行，必须配置每个池大小。
		 */
		if (StringUtil.isBlank((String) prop.get(USE_PARALLEL_EXECUTE))
				|| StringUtil.isBlank((String) prop.get(PARALLEL_THREAD_COUNT))) {
			logger.warn("the parallel config useparallelexecute or parallelthreadcount is blank,must be both configed");
			setUseParallelFalse();
			return;
		}

		boolean newUseParallel = Boolean.valueOf((String) prop
				.get(USE_PARALLEL_EXECUTE));

		if (newUseParallel != useParallel) {
			useParallel = newUseParallel;
		}

		int threadCount = Integer.valueOf((String) prop
				.get(PARALLEL_THREAD_COUNT));

		/**
		 * 如果新的池大小与老的池大小不一样, 并且不是初始化,那么动态修改池大小
		 * 
		 */
		if (esThreadCount != threadCount) {
			esThreadCount = threadCount;
			if (!isInit) {
				synchronized (esMap) {
					for (Map.Entry<String, ThreadPoolExecutor> entry : esMap
							.entrySet()) {
						entry.getValue().setCorePoolSize(esThreadCount);
						entry.getValue().setMaximumPoolSize(esThreadCount * 2);
					}
				}
			}
		}
	}

	/**
	 * 动态配置推送
	 */
	public void onDataRecieved(String dataId,String data) {
		if (null == data) {
			setUseParallelFalse();
			logger.warn("no parallel execute info, set useParallel false");
			return;
		}

		logger
				.warn("[RUNNING]recieve parallel execute config,dataId:"+dataId+" start to init data:"
						+ data);
		configChange(data, false);
		logger.warn("[RUNNING]reset parallel execute info success!");
	}

	public static String getSqlExecutorKey(String appName) {
		return PARALLEL_EXECUTOR_FORMAT.format(new Object[] { appName });
	}

	/**
	 * 将property字符串转换成Properties
	 * 
	 * @param data
	 * @return
	 */
	private Properties parseConfigStr2Prop(String data) {
		Properties prop = new Properties();
		if (StringUtil.isNotBlank(data)) {
			ByteArrayInputStream byteArrayInputStream = null;
			try {
				byteArrayInputStream = new ByteArrayInputStream(data.getBytes());
				prop.load(byteArrayInputStream);
			} catch (IOException e) {
				logger
						.error("[PARALLEL_EXECUTE]parse diamond config error!",
								e);
			} finally {
				byteArrayInputStream.close();
			}
		}

		return prop;
	}

	private void setUseParallelFalse() {
		useParallel = false;
	}

	/**
	 * 提交一个任务让线程池执行， 一般是一个库维持一个线程池， 防止其他库受到干扰。 如果线程池map里面不存在目标库 的线程池，那么新建一个。
	 * 
	 * 
	 * @param dbIndex
	 * @param command
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static Future submit(String dbIndex, Runnable command) {
		ThreadPoolExecutor es = null;
		/**
		 * 先试着拿下，如果有就不需要进锁
		 */
		if (null != esMap.get(dbIndex)) {
			es = esMap.get(dbIndex);
			return es.submit(command);
		}

		synchronized (esMap) {
			/**
			 * 这个时候esMap没有指定线程池的几率比较大 所以先判定为null
			 */
			if (null == esMap.get(dbIndex)) {
				logger.warn("init threadpool for " + dbIndex);
				logger.warn("dbIndex:" + dbIndex
						+ ",parallel threadPool corepool size:" + esThreadCount
						+ ",maxpool size:" + esThreadCount*2);
				es = new ThreadPoolExecutor(esThreadCount, esThreadCount * 2,
						2000, TimeUnit.MILLISECONDS,
						new LinkedBlockingQueue<Runnable>(queueSize),
						new NamedThreadFactory("TDDL-PARALLEL"),
						new ThreadPoolExecutor.CallerRunsPolicy());
				esMap.put(dbIndex, es);
			} else {
				/**
				 * 最后最小可能就是在第一次判定不为null后 并发地加入了一个指定key的线程池
				 */
				es = esMap.get(dbIndex);
			}
		}
		return es.submit(command);
	}

	public static boolean isUseParallel() {
		return useParallel;
	}
}
