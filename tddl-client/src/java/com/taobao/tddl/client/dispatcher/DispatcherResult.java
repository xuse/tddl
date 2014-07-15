package com.taobao.tddl.client.dispatcher;

import java.util.List;

import com.taobao.tddl.client.controller.ColumnMetaData;
import com.taobao.tddl.client.controller.OrderByMessages;

/**
 * ���ǵ����Ҫ֧������ʵ�֣�ǰ����������Ƚ��鷳����˶��������Ľӿ�
 * �ڲ�ʵ�ֲ���Ҫcare.
 * 
 * @author shenxun
 * 
 *TODO:û��group by element��¶���������ⲻ��\
 */
public interface DispatcherResult extends Result {
	/**
	 * �Ƿ������������
	 * 
	 * @return
	 */
	public boolean allowReverseOutput();

	/**
	 * ��ȡ�ֿ���������Ҫ�и��ƵĻ�
	 * ��Ҫע�����
	 * primaryKey�ͷֿ���ͷֱ���֮������ǲ����ظ��ġ�
	 * ���primaryKey�ͷֱ��г��ֵģ���ô�ֿ��оͲ������
	 * ����ֿ��к�primaryKey�г��ֵģ���ô�ֱ��Ͳ������
	 * @return
	 */
    public List<ColumnMetaData> getSplitDB();

	/**
	 * ��ȡ�ֱ����������Ҫ�и��ƵĻ�
	 * primaryKey�ͷֿ���ͷֱ���֮������ǲ����ظ��ġ�
	 * ���primaryKey�ͷֱ��г��ֵģ���ô�ֿ��оͲ������
	 * ����ֿ��к�primaryKey�г��ֵģ���ô�ֱ��Ͳ������
	 * @return
	 */
	public List<ColumnMetaData> getSplitTab();

	/**
	 * ��ȡ�����������Ҫ�и��ƵĻ�
	 * primaryKey�ͷֿ���ͷֱ���֮������ǲ����ظ��ġ�
	 * ���primaryKey�ͷֱ��г��ֵģ���ô�ֿ��оͲ������
	 * ����ֿ��к�primaryKey�г��ֵģ���ô�ֱ��Ͳ������
	 * @return
	 */
    public ColumnMetaData getPrimaryKey();

	/**
	 * �Ƿ���Ҫ�и���
	 * 
	 * @return
	 */
	public boolean needRowCopy();

	/**
	 * ��ȡorder by ��Ϣ
	 * @return
	 */
	public OrderByMessages getOrderByMessages();

	/**
	 * ��ȡskipֵ
	 * ��ΪTDDL�������ݿ⣬������������һ���ٶ���
	 * �����к���Ϊskip�������У������Ǹ���Զ��������ġ�
	 * 
	 * ���Ƕ����Ҳ����ˡ�
	 * @return
	 */
	public int getSkip();

	/**
	 * ��ȡmaxֵ��
	 * ��ΪTDDL�������ݿ⣬������������һ���ٶ���
	 * �����к���Ϊmax�������У������Ǹ���Զ��������ġ�
	 * 
	 * ���Ƕ����Ҳ����ˡ�
	 * @return
	 */
	public int getMax();

	/**
	 * �����������
	 * @param needReverseOutput
	 */
	public void needAllowReverseOutput(boolean needReverseOutput);

	/**
	 * ��ȡ���ݿ�ִ�мƻ�
	 * @return
	 */
	public EXECUTE_PLAN getDatabaseExecutePlan();

	/**
	 * ����
	 * @param executePlan
	 */
	public void setDatabaseExecutePlan(EXECUTE_PLAN executePlan);

	public EXECUTE_PLAN getTableExecutePlan();

	public void setTableExecutePlan(EXECUTE_PLAN executePlan);

    /**
     * ��¼��join���������
     * @author jiangping
     * @param virtualJoinTableNames
     */
    public void setVirtualJoinTableNames(List<String> virtualJoinTableNames);

    /**
     * ��ȡ��join���������
     * @author jiangping
     * @return
     */
    public List<String> getVirtualJoinTableNames();
    
	public List<String> getDistinctColumns();
}