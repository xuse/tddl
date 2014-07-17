package com.taobao.tddl.common.sequence;

public class IDParseFactory {
	
	private static IDParseFactory instance = new IDParseFactory();
	private IDParseFactory(){}
	public static IDParseFactory newInstance() {
		return instance;
	}
	
	public IDParse<?, ?, ?> createIDParse(Config config) {
		if(config.getType() == Config.DEFAULT) {
			return new IDParseImp(config);
		} else if(config.getType() == Config.Long2DATE) {
			return new IDParse4CTU3(config);
		} else {
			//You can't arrive here!
			return null;
		}
	}
	
}
