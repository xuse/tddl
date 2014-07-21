package com.taobao.tddl.client.jdbc.replication;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.client.jdbc.listener.Context;
import com.taobao.tddl.client.jdbc.listener.Handler;
import com.taobao.tddl.client.jdbc.listener.HookPoints;
import com.taobao.tddl.client.jdbc.listener.Adapter;

/*
 * @author guangxia
 * @since 1.0, 2010-4-13 下午05:25:54
 */
public class ReplicationAdapter implements Adapter {
	
	private final RowBasedReplicationListener rowBasedReplicationListener;
	
	public ReplicationAdapter(RowBasedReplicationListener rowBasedReplicationListener) {
		this.rowBasedReplicationListener = rowBasedReplicationListener;
	}

	public void init(TDataSource tDataSource, HookPoints hookPoints) {
		rowBasedReplicationListener.setReplicationConfig(tDataSource.getReplicationConfig());
		rowBasedReplicationListener.init(tDataSource);		
		
		hookPoints.appendBeforeExecuteFinally(new Handler() {
			@Override
			protected boolean run(Context context) throws SQLException {
				rowBasedReplicationListener.beforeSqlExecute(context);
				return true;
			}
		});	
		hookPoints.appendAfterExecuteFinally(new Handler() {
			@Override
			protected boolean run(Context context) throws SQLException {
				rowBasedReplicationListener.afterSqlExecute(context);
				return true;
			}
		});
//		hookPoints.appendAfterRollBackFinally(new Handler() {
//			@Override
//			protected boolean run(Context context) throws SQLException {
//				rowBasedReplicationListener.afterTxRollback(context);
//				return true;
//			}
//		});
	}

	public void setMaxTxTime(long maxTxTime) {
		rowBasedReplicationListener.setMaxTxTime(maxTxTime);
	}

	public void setReplicationSwitcher(ReplicationSwitcher replicationSwitcher) {
		rowBasedReplicationListener.replicationSwitcher = replicationSwitcher;
	}

	public void setReplicationCallbackHandler(ReplicationCallbackHandler replicationCallbackHandler) {
		rowBasedReplicationListener.setReplicationCallbackHandler(replicationCallbackHandler);
	}

	/*
	public void setSyncLogDataSourceConfigFile(String syncLogDataSourceConfigFile) {
		rowBasedReplicationListener.setSyncLogDataSourceConfigFile(syncLogDataSourceConfigFile);
	}

	public void setSynclogDatabaseId(String synclogDatabaseId) {
		rowBasedReplicationListener.setSynclogDatabaseId(synclogDatabaseId);
	}
	*/

	public void setSyncLogDataSource(DataSource syncLogDataSource) {
		rowBasedReplicationListener.setSyncLogDataSource(syncLogDataSource);
	}

	public RowBasedReplicationListener getRowBasedReplicationListener() {
		return rowBasedReplicationListener;
	}

}
