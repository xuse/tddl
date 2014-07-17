package com.taobao.tddl.client.jdbc.replication;

/*
 * @author guangxia
 * @since 1.0, 2010-4-13 обнГ05:46:30
 */
public class SyncReplicationAdapter extends ReplicationAdapter {

	public SyncReplicationAdapter() {
		super(new SyncRowBasedReplicationListener());
	}

}
