package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.PageWrapper;


public  class RangePlaceHandler implements ReplaceHandler{
	private Number skip;
	
	private Number max;
	public RangePlaceHandler() {
	}
	public RangePlaceHandler(Number skip, Number max) {
		super();
		this.skip = skip;
		this.max = max;
	}

	public Number getSkip() {
		return skip;
	}
	public Number getMax() {
		return max;
	}
	/**
	 * 换value
	 * @param skip
	 * @param max
	 * @param max
	 * @param changeParam
	 * @return
	 */
	public String changeValue(PageWrapper pageWrapper ,Map<Integer, Object> changeParam){
		Integer index = pageWrapper.getIndex();
		boolean canBeChanged = pageWrapper.canBeChange();
		Number value = pageWrapper.getValue();
		if(index!=null){
			if(canBeChanged){
				pageWrapper.modifyParam(skip,max,changeParam);
			}
			return "?";
		}else if(value!= null){
			if(canBeChanged){
				String temp = pageWrapper.getSqlReturn(skip,max);
				if(temp == null){
					return value.toString();
				}else{
					return temp;
				}
			}else{
				return value.toString();
			}
		}else{
			throw new IllegalStateException("不应该出现没有值直接写在sql,但也没有index的情况");
		}
	}
}
