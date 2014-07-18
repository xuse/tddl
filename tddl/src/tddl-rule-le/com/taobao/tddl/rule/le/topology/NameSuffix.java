package com.taobao.tddl.rule.le.topology;

import java.util.ArrayList;
import java.util.List;

/**
 * 描述一个（组）有规律的后缀。
 * 
 * <pre>
 *   NameSuffix = Pattern | NameRange
 *   
 *   NameRange = Min "-" Max
 * </pre>
 * 
 * @author changyuan.lh
 */
public abstract class NameSuffix {

    public abstract boolean contains(String name);

    protected abstract List<String> iterate(StringBuilder buf, List<String> list);

    public final List<String> list() {
	return iterate(new StringBuilder(), new ArrayList<String>());
    }

    protected abstract StringBuilder buildString(StringBuilder buf);

    public final String toString() {
	return buildString(new StringBuilder()).toString();
    }
}
