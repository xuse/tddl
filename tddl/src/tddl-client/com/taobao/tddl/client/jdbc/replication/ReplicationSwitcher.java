package com.taobao.tddl.client.jdbc.replication;

/**
 * 行复制开关
 * 
 * @author linxuan
 */
public interface ReplicationSwitcher {
	public enum PropKey {
		level, //行复制级别的属性键
		insertSyncLogMode, //插入日志库的模式
		replicationThreadPoolSize, //行复制线程池大小
		insertSyncLogThreadPoolSize, //异步插入日志库的线程池大小
	}

	public enum Level {
		ALL_ON, //全部打开行复制， 同步插入日志库，应用端同步或异步实时行复制
		INSERT_LOG, //只同步插入日志库，不进行应用端实时行复制。只依赖补偿服务器进行同步
		ALL_OFF, //全部关闭行复制,不插日志不复制
	}

	public enum InsertSyncLogMode {
		normal, // 现在的方式：同步插入日志库，失败抛出特定异常。可靠的，保证不丢失更新
		logfileonly, //不插入日志库，只同步记录到本地log文件(或用store4j)(level=INSERT_LOG不能选这项）
		streaking, //裸奔。不插入日志库，也不写本地log文件。完全依赖实时行复制。数据一致性由额外的dump保证(level=INSERT_LOG不能选这项）
		/**
		 * 同步记录到本地log文件(或用store4j)。分为两种log：
		 * 1. 每个更新都记log，滚动记录，只要保证log文件包含异步线程池queue大小的更新日志即可。
		 *    用于在应用重启后，根据queue大小的设置，取最后一段更新log进行补偿恢复
		 * 2. 当线程池queue满时，或线程处理抛出异常时，记录日志到单独的文件中。用来人工恢复
		 */
		asynchronous,
	}

	Level level();

	InsertSyncLogMode insertSyncLogMode();

	void addReplicationConfigAware(ReplicationConfigAware replicationConfigAware);

	interface ReplicationConfigAware {
		void setReplicationThreadPoolSize(int threadPoolSize);

		int getReplicationThreadPoolSize();

		void setInsertSyncLogThreadPoolSize(int threadPoolSize);

		int getInsertSyncLogThreadPoolSize();
	}

	public class ReplicationConfigAwareAdaptor implements ReplicationConfigAware {
		private int insertSyncLogThreadPoolSize;
		private int replicationThreadPoolSize;

		public void setInsertSyncLogThreadPoolSize(int threadPoolSize) {
			this.insertSyncLogThreadPoolSize = threadPoolSize;
		}

		public void setReplicationThreadPoolSize(int threadPoolSize) {
			this.replicationThreadPoolSize = threadPoolSize;
		}

		public int getInsertSyncLogThreadPoolSize() {
			return insertSyncLogThreadPoolSize;
		}

		public int getReplicationThreadPoolSize() {
			return replicationThreadPoolSize;
		}
	}
}
