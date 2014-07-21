package com.taobao.tddl.sqlobjecttree;


public  class BindIndexHolder {
	private int selfAdd = 0;
	/**
	 * 返回当前值，并自增where内的计数，用于添加绑定变量时的标识
	 * 
	 * @return
	 */
	public int selfAddAndGet() {
		int ret = selfAdd;
		selfAdd++;
		return ret;
	}
	public BindIndexHolder() {
	}
	public BindIndexHolder(int index) {
		this.selfAdd = index;
	}
	public int getCurrentIndex(){
		return selfAdd;
	}
}
