package com.xunlei.data.hbase.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * 一个ColumnFamily一个对象
 * 
 * 一个ColumnFamily下包含多个Column
 * 
 * @author q
 *
 */
public class ColumnFamilyMetadata {

	@JSONField(name = "column_family_name")
	private String columnFamilyName;
	@JSONField(serialize = false)
	private byte[] columnFamilyNameBytes;
	@JSONField(name = "columns")
	private List<ColumnMetadata> columnMetadataList; 
	
	public ColumnFamilyMetadata() {
		
	}
	
	public ColumnFamilyMetadata(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
		this.columnFamilyNameBytes = Bytes.toBytes(this.columnFamilyName);
		this.columnMetadataList = new ArrayList<ColumnMetadata>();
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
		this.columnFamilyNameBytes = Bytes.toBytes(this.columnFamilyName);
	}

	public byte[] getColumnFamilyNameBytes() {
		return columnFamilyNameBytes;
	}

	public List<ColumnMetadata> getColumnMetadataList() {
		return columnMetadataList;
	}
	
	public void setColumnMetadataList(List<ColumnMetadata> columnMetadataList) {
		this.columnMetadataList = columnMetadataList;
	}

	public void addColumnMetadata(ColumnMetadata columnMetadata) {
		this.columnMetadataList.add(columnMetadata);
	}
	
}
