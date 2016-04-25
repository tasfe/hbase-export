package com.xunlei.data.hbase.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * 一个表一个TableMetadata
 * 
 * @author q
 *
 */
public class TableMetadata {
	//table
	@JSONField(name = "table_name")
	private String tableName;
	@JSONField(serialize = false)
	private TableName tableNameHBase;
	
	//rowkey
	@JSONField(name = "rowkey_type")
	private DataType rowKeyType;
	@JSONField(name = "rowkey_assemble_type")
	private RowKeyAssembleType rowKeyAssembleType;
	//column-family
	@JSONField(name = "column_familys")
	private List<ColumnFamilyMetadata> columnFamilyMetadataList;

	public TableMetadata(){
		
	}
	
	public TableMetadata(String tableName) {
		this(tableName, DataType.STRING, RowKeyAssembleType.NO_ASSEMBLE);
	}

	public TableMetadata(String tableName, String rowKeyType, String rowKeyAssembleType) {
		this(tableName, DataType.parse(rowKeyType), RowKeyAssembleType.parse(rowKeyAssembleType));
	}
	
	public TableMetadata(String tableName, DataType rowKeyType, RowKeyAssembleType rowKeyAssembleType) {
		this.tableName = tableName;
		this.tableNameHBase = TableName.valueOf(this.tableName);
		this.rowKeyType = rowKeyType;
		this.rowKeyAssembleType = rowKeyAssembleType;
		this.columnFamilyMetadataList = new ArrayList<ColumnFamilyMetadata>();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
		this.tableNameHBase = TableName.valueOf(this.tableName);
	}

	public TableName getTableNameHBase() {
		return tableNameHBase;
	}

	public DataType getRowKeyType() {
		return rowKeyType;
	}

	public void setRowKeyType(DataType rowKeyType) {
		this.rowKeyType = rowKeyType;
	}

	public RowKeyAssembleType getRowKeyAssembleType() {
		return rowKeyAssembleType;
	}

	public void setRowKeyAssembleType(RowKeyAssembleType rowKeyAssembleType) {
		this.rowKeyAssembleType = rowKeyAssembleType;
	}

	public List<ColumnFamilyMetadata> getColumnFamilyMetadataList() {
		return columnFamilyMetadataList;
	}

	public void setColumnFamilyMetadataList(List<ColumnFamilyMetadata> columnFamilyMetadataList) {
		this.columnFamilyMetadataList = columnFamilyMetadataList;
	}

	public void addColumnFamilyMetadata(ColumnFamilyMetadata columnFamilyMetadata) {
		this.columnFamilyMetadataList.add(columnFamilyMetadata);
	}
	
}
