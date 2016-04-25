package com.xunlei.data.hbase.data;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

/**
 * 从HBase里面获取的一行数据，统一都做成Map类型，一行一个RowData
 * 
 * key的名字是字段名称，value就是值
 * 
 * 注意：另外还有"字段名称_timestamp"
 * 
 * 
 * @author q
 *
 */
public class RowData {

	private Map<String, String> rowMap;
	
	public RowData() {
		this.rowMap = new HashMap<String, String>();
	}
	
	public void addKeyValue(String columnName, String columnValue) {
		this.rowMap.put(columnName, columnValue);
	}
	
	public Map<String, String> getRowData() {
		return this.rowMap;
	}
	
	public String toJson() {
		return JSON.toJSONString(this.rowMap);
	}
	
	public static String toJsonString(RowData rowData) {
		return JSON.toJSONString(rowData.getRowData());
	}
}
