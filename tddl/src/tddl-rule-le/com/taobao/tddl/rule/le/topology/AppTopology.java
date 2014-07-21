package com.taobao.tddl.rule.le.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 应用级别的拓扑结构描述。
 * 
 * <pre>
 *   AppTopology = TableTopology *( "," TableTopology )
 *   
 *   TableTopology = Name "{" GROUP *( "," GROUP ) "}"
 *   
 *   GROUP = GroupKey ":" NamePattern
 * </pre>
 * 
 * @author changyuan.lh
 */
public final class AppTopology {

    final Map<String, TableTopology> map = new TreeMap<String, TableTopology>();

    public void put(TableTopology topology) {
	map.put(topology.getName(), topology);
    }

    public TableTopology lookup(String table) {
	return map.get(table.toLowerCase());
    }

    public Collection<TableTopology> tables() {
	return map.values();
    }

    public List<String> list() {
	List<String> list = new ArrayList<String>();
	for (TableTopology topo : map.values()) {
	    list.addAll(topo.list());
	}
	return list;
    }

    public Map<String, TableTopology> map() {
	return map;
    }

    /**
     * AppTopology = TableTopology *( "," TableTopology )
     * 
     * TableTopology = Name "{" GROUP *( "," GROUP ) "}"
     */
    protected int loadInput(String input, int fromIndex) {
	final int len = input.length();
	int index = fromIndex;
	for (int i = index; i < len; i++) {
	    char ch = input.charAt(i);
	    if (ch == '{') {
		String name = input.substring(index, i).trim();
		if (name.isEmpty()) {
		    throw new IllegalArgumentException("No name: " + input);
		}
		TableTopology topology = new TableTopology(name);
		index = topology.loadBraces(input, i + 1);
		map.put(name, topology);
		i = index - 1;
	    }
	}
	return index;
    }

    /**
     * AppTopology = TableTopology *( "," TableTopology )
     */
    public static AppTopology loadInput(String input) {
	AppTopology topology = new AppTopology();
	topology.loadInput(input, 0);
	return topology;
    }

    final StringBuilder buildString(StringBuilder buf) {
	for (TableTopology topology : map.values()) {
	    topology.buildString(buf);
	    buf.append('\n');
	}
	return buf;
    }

    public String toString() {
	return buildString(new StringBuilder()).toString();
    }

    public static void main(String[] args) {
	AppTopology topology = AppTopology
		.loadInput("billin { BILLIN_GROUP_1: billin_[00-07], BILLIN_GROUP_2: billin_[08-15] }"
			+ "crm { CRM_GROUP_01: crm_[1-4], CRM_GROUP_02: crm_[5-8], "
			+ "CRM_GROUP_03: crm_[9-12], CRM_GROUP_04: crm_[13-16] }");
	System.out.println("Topology:\n" + topology);
	System.out.println("Topology:\n"
		+ AppTopology.loadInput(topology.toString()));
	System.out.println("list:\n  " + topology.list());
    }
}
