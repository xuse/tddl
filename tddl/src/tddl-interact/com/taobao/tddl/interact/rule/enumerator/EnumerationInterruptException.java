package com.taobao.tddl.interact.rule.enumerator;

import com.taobao.tddl.interact.sqljep.Comparative;

public class EnumerationInterruptException extends RuntimeException{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient  Comparative comparative;
	public EnumerationInterruptException(Comparative comparative){
		this.comparative = comparative;
	}
	public EnumerationInterruptException(){
	}
	public Comparative getComparative() {
		return comparative;
	}
	public void setComparative(Comparative comparative) {
		this.comparative = comparative;
	}
	
}
