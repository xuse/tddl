package com.taobao.tddl.common.monitor;

import java.util.Date;
/**
 * 
 * @author junyu
 *
 */
public interface TimeComputer {
	/**
	 * 得到距离最近的某个时间的间隔
	 * 
	 * @return 毫秒计
	 */
    public long getMostNearTimeInterval();
    
    /**
     * 得到距离的最近的某个时间
     * 
     * @return Date
     */
    public Date getMostNearTime();
}
