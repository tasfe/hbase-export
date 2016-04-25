package com.xunlei.data.hbase.metadata;

import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * 一列一个ColumnMetadata对象
 * 
 * @author q
 *
 */
public class ColumnMetadata {

	@JSONField(name = "column_name")
	private String columnName;
	@JSONField(serialize = false)
	private byte[] columnNameBytes;
	@JSONField(name = "column_type")
	private DataType columnType;

	public ColumnMetadata() {
		
	}
	
	public ColumnMetadata(String columnName, DataType columnType) {
		this.columnName = columnName;
		this.columnNameBytes = Bytes.toBytes(this.columnName);
		this.columnType = columnType;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
		this.columnNameBytes = Bytes.toBytes(this.columnName);
	}

	public byte[] getColumnNameBytes() {
		return columnNameBytes;
	}

	public DataType getColumnType() {
		return columnType;
	}

	public void setColumnType(DataType columnType) {
		this.columnType = columnType;
	}

}
