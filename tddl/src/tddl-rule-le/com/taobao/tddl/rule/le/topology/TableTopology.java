package com.taobao.tddl.rule.le.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * 逻辑表级别的拓扑结构描述。
 * 
 * <pre>
 *   TableTopology = Name "{" GROUP *( "," GROUP ) "}"
 *   
 *   GROUP = GroupKey ":" NamePattern
 * </pre>
 * 
 * @author changyuan.lh
 */
public final class TableTopology {

    final String name;

    final Map<String, NamePattern> map = new TreeMap<String, NamePattern>();

    public TableTopology(String name) {
	this.name = name.trim();
    }

    public String getName() {
	return name;
    }

    public void put(String group, NamePattern pattern) {
	map.put(group.trim(), pattern);
    }

    public NamePattern lookup(String group) {
	return map.get(group);
    }

    public Collection<NamePattern> patterns() {
	return map.values();
    }

    public List<String> list() {
	List<String> list = new ArrayList<String>();
	StringBuilder buf = new StringBuilder();
	for (NamePattern namePattern : map.values()) {
	    namePattern.iterate(buf, list);
	    buf.setLength(0);
	}
	return list;
    }

    public Map<String, NamePattern> map() {
	return map;
    }

    /**
     * TableTopology = Name "{" GROUP *( "," GROUP ) "}"
     * 
     * GROUP = GroupKey ":" NamePattern
     * 
     * NamePattern = Name | ( Prefix SuffixExpr )
     */
    protected int loadBraces(String input, int fromIndex) {
	final int len = input.length();
	int index = fromIndex;
	String group = null;
	for (int i = index; i < len; i++) {
	    char ch = input.charAt(i);
	    switch (ch) {
	    case ':':
		group = input.substring(index, i).trim();
		if (group.isEmpty()) {
		    throw new IllegalArgumentException("Empty group: "
			    + input.substring(index));
		}
		index = i + 1;
		break;
	    case '[': {
		if (group == null) {
		    throw new IllegalArgumentException("No group: "
			    + input.substring(index));
		}
		String prefix = input.substring(index, i).trim();
		NamePattern namePattern = new NamePattern(
			prefix.isEmpty() ? null : NamePattern.unescape(prefix),
			new ArrayList<NameSuffix>());
		index = namePattern.loadSuffix(input, i + 1);
		// TODO: 这里应该是逗号: input.charAt(index)
		map.put(group, namePattern);
		group = null;
		i = index - 1;
		break;
	    }
	    case ',':
		if (i > index) {
		    String name = input.substring(index, i).trim();
		    if (!name.isEmpty()) {
			if (group == null) {
			    throw new IllegalArgumentException("No group: "
				    + input.substring(index));
			}
			name = NamePattern.unescape(name);
			NamePattern namePattern = new NamePattern(name, null);
			map.put(group, namePattern);
			group = null;
			index = i + 1;
			continue;
		    }
		}
		if (group != null) {
		    throw new IllegalArgumentException("No name pattern: "
			    + input.substring(index));
		}
		index = i + 1;
		break;
	    case '}':
		if (i > index) {
		    String name = input.substring(index, i).trim();
		    if (!name.isEmpty()) {
			if (group == null) {
			    throw new IllegalArgumentException("No group: "
				    + input.substring(index));
			}
			name = NamePattern.unescape(name);
			NamePattern namePattern = new NamePattern(name, null);
			map.put(group, namePattern);
			group = null;
			return i + 1;
		    }
		}
		if (group != null) {
		    throw new IllegalArgumentException("No name pattern: "
			    + input.substring(index));
		}
		return i + 1;
	    }
	}

	if (len > index) {
	    String name = input.substring(index, len).trim();
	    if (!name.isEmpty()) {
		if (group == null) {
		    throw new IllegalArgumentException("No group: "
			    + input.substring(index));
		}
		name = NamePattern.unescape(name);
		NamePattern namePattern = new NamePattern(name, null);
		map.put(group, namePattern);
		group = null;
		return len;
	    }
	}
	if (group != null) {
	    throw new IllegalArgumentException("No name pattern: "
		    + input.substring(index));
	}
	return len;
    }

    /**
     * TableTopology = Name "{" GROUP *( "," GROUP ) "}"
     */
    public static TableTopology loadInput(String input) {
	final int braceIndex = input.indexOf('{', 0);
	if (braceIndex == -1) {
	    throw new IllegalArgumentException("No braces: " + input);
	}
	String name = input.substring(0, braceIndex).trim();
	if (name.isEmpty()) {
	    throw new IllegalArgumentException("No name: " + input);
	}
	TableTopology topology = new TableTopology(name);
	topology.loadBraces(input, braceIndex + 1);
	return topology;
    }

    final StringBuilder buildString(StringBuilder buf) {
	buf.append(name);
	buf.append(" {\n  ");
	boolean comma = false;
	for (Entry<String, NamePattern> entry : map.entrySet()) {
	    String group = entry.getKey();
	    if (comma)
		buf.append(",\n  ");
	    buf.append(group);
	    buf.append(": ");
	    NamePattern namePattern = entry.getValue();
	    namePattern.buildString(buf);
	    comma = true;
	}
	buf.append("\n}");
	return buf;
    }

    public String toString() {
	return buildString(new StringBuilder()).toString();
    }

    public static void main(String[] args) {
	TableTopology topology = TableTopology
		.loadInput("billin { \n BILLIN_GROUP_1: billin_[01-08], BILLIN_GROUP_2: billin_[09-16] }\n");
	System.out.println("Topology:\n" + topology);
	System.out.println("Topology:\n"
		+ TableTopology.loadInput(topology.toString()));
	System.out.println("list:\n  " + topology.list());
    }
}
