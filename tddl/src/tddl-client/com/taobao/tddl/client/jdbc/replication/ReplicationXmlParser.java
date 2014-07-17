//package com.taobao.tddl.client.jdbc.replication;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.dom4j.Document;
//import org.dom4j.DocumentException;
//import org.dom4j.Element;
//import org.dom4j.io.SAXReader;
//
//import com.taobao.tddl.common.sync.BizTDDLContext;
//import com.taobao.tddl.common.sync.SlaveInfo;
//import com.taobao.tddl.common.sync.SyncConstants;
//
//public class ReplicationXmlParser {
//	private String replicationUrl;
//
//	public ReplicationXmlParser() {}
//
//	public ReplicationXmlParser(String replicationUrl) {
//		this.replicationUrl = replicationUrl;
//	}
//
//	@SuppressWarnings("unchecked")
//	public Map<String,BizTDDLContext> parse() {
//		Map<String,BizTDDLContext> replicationMap = new HashMap<String, BizTDDLContext>();
//		
//		
//		
////		xmlElements.setSlaveInfos(replicationMap);
//		
//		SAXReader reader = new SAXReader();
//
//		Document document;
//		try {
//			document = reader.read(getClass().getResource(replicationUrl));
//		} catch (DocumentException e) {
//			throw new RuntimeException("read " + replicationUrl + " error", e);
//		}
//
//		Element replication = document.getRootElement();
//		List<Element> masters = replication.elements("master");
//		for (Element master : masters) {
//			String masterColumns = master.elementTextTrim("master-columns");
//			BizTDDLContext xmlElement = new BizTDDLContext();
//			xmlElement.setMasterColumnsIfAbsentAndGet(masterColumns);
//			List<Element> slaveElements = master.element("slaves").elements("slave");
//			SlaveInfo[] slaves = new SlaveInfo[slaveElements.size()];
//			int i = 0;
//			for (Element slaveElement : slaveElements) {
//				slaves[i] = new SlaveInfo();
//				slaves[i].setName(slaveElement.attributeValue("name").trim().toLowerCase());
//				slaves[i].setType(slaveElement.attributeValue("type").trim().toLowerCase());
//				if (!SyncConstants.DATABASE_TYPE_MYSQL.equals(slaves[i].getType())
//						&& !SyncConstants.DATABASE_TYPE_ORACLE.equals(slaves[i].getType())) {
//					throw new RuntimeException("Unsupported slave database type, type = " + slaves[i].getType());
//				}
//				slaves[i].setDataSourceName(slaveElement.elementTextTrim("data-source-name"));
//				if (slaveElement.element("database-shard-column") != null) {
//					slaves[i].setDatabaseShardColumn(slaveElement.elementTextTrim("database-shard-column").toLowerCase());
//				}
//				if (slaveElement.element("table-shard-column") != null) {
//					slaves[i].setTableShardColumn(slaveElement.elementTextTrim("table-shard-column").toLowerCase());
//				}
//
//				if (slaveElement.element("columns") != null) {
//					List<Element> columnElements = slaveElement.element("columns").elements("column");
//					String[] columns = new String[columnElements.size()];
//					int j = 0;
//					for (Element columnElement : columnElements) {
//						columns[j++] = columnElement.getTextTrim().toLowerCase();
//					}
//
//					slaves[i].setColumns(columns);
//				}
//
//				++i;
//			}
//			
//			xmlElement.setSlaveInfos(slaves);
//			
//			replicationMap.put(master.attributeValue("name").trim().toLowerCase(), xmlElement);
//		}
//
//		return replicationMap;
//	}
//
//	public void setReplicationUrl(String replicationUrl) {
//		this.replicationUrl = replicationUrl;
//	}
//}
