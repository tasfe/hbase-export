package com.xunlei.data.hbase.metadata;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * 整个HBase里面所有表的元数据
 * 
 * 一个HBaseMetadata包含多个TableMetadata
 * 
 * @author q
 *
 */
public class HBaseMetadata {

	@JSONField(name = "hbase_name")
	private String hbaseName;
	
	@JSONField(name = "tables")
	private List<TableMetadata> tableMetadataList;
	
	public HBaseMetadata() {
		this.tableMetadataList = new ArrayList<TableMetadata>();
	}

	public String getHbaseName() {
		return hbaseName;
	}

	public void setHbaseName(String hbaseName) {
		this.hbaseName = hbaseName;
	}

	public List<TableMetadata> getTableMetadataList() {
		return tableMetadataList;
	}

	public void setTableMetadataList(List<TableMetadata> tableMetadataList) {
		this.tableMetadataList = tableMetadataList;
	}
	
	public void addTableMetadata(TableMetadata tableMetadata) {
		this.tableMetadataList.add(tableMetadata);
	}
	
}
