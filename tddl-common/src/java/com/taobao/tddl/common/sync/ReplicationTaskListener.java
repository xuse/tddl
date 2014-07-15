package com.taobao.tddl.common.sync;

public interface ReplicationTaskListener {
    void onTaskCompleted(RowBasedReplicationContext context, boolean success);
}
