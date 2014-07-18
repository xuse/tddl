package com.taobao.tddl.interact.rule.enumerator;

import java.util.Set;

import com.taobao.tddl.interact.sqljep.Comparative;

public interface CloseIntervalFieldsEnumeratorHandler {
	 /**
	 * @param source
	 * @param retValue
	 * @param cumulativeTimes
	 * @param atomIncrValue
	 */
	void processAllPassableFields(Comparative source ,Set<Object> retValue,Integer cumulativeTimes,Comparable<?> atomIncrValue);
		/**
		 * 穷举出从from到to中的所有值，根据自增value
		 * 
		 * @param from
		 * @param to
		 */
	abstract void mergeFeildOfDefinitionInCloseInterval(
				Comparative from, Comparative to, Set<Object> retValue,Integer cumulativeTimes,Comparable<?> atomIncrValue);

	
}
