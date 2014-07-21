package com.taobao.tddl.common.config;

/**
 * 默认解析文本控制器
 * 
 * @author linxuan
 *
 */
public class DefaultTddlConfigParser<T> implements TddlConfigParser<T> {
	private static final String prefix_spring = "<!-- tddlconfig -->";
	private static final String prefix_properties = "#tddlconfig";

	private SpringXmlConfigParser<T> springParser = new SpringXmlConfigParser<T>();
	private PropertiesConfigParser<T> propertiesParser = new PropertiesConfigParser<T>();
	public T parseCongfig(String txt) {
		if (txt.startsWith(prefix_spring)){
			//return springParser.parseCongfig(txt.substring(prefix_spring.length()));
			return springParser.parseCongfig(txt); //不需要去除头了
		}else if(txt.startsWith(prefix_properties)){
			//return propertiesParser.parseCongfig(txt.substring(prefix_properties.length()));
			return propertiesParser.parseCongfig(txt); //不需要去除头了
		}else{
			//不加前缀默认为spring xml格式，便于直接将推送文本拷贝到文件中，用spring IDE检查
			return springParser.parseCongfig(txt);
		}
	}
}
