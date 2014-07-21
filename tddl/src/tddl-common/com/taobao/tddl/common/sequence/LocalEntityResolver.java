package com.taobao.tddl.common.sequence;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

/*
 * @author guangxia
 * @since 1.0, 2009-5-12 下午08:39:02
 */
public class LocalEntityResolver implements EntityResolver {

	public InputSource resolveEntity(String publicId, String systemId) {
		if("-//arch.taobao.com//tddl generators config DTD//ZH".equals(publicId)
				&& "http://arch.taobao.com/tddl/generators.dtd".equals(systemId)) {
			return new InputSource(getClass().getResourceAsStream("/generators.dtd"));
		} else {
			return new InputSource(systemId);
		}
	}

}
