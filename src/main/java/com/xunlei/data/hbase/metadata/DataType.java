package com.xunlei.data.hbase.metadata;

/**
 * 字段类型
 * 
 * 获取数据的时候，从HBase底层都是byte[]；
 * 
 * 需要根据元数据里面的LONG/STRING等，做相应的转换；
 * 
 * 比如：如果元数据是LONG，则Bytes.toLong(byte[])，获得Long对象，在转成字符串统一写道RowData里面
 * 
 * @author q
 *
 */
public enum DataType {

	INT("int"), 
	LONG("long"),
	SHORT("short"),
	STRING("string"),
	BYTE("byte");

	private String typeName;

	private DataType(String typeName) {
		this.name();
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public static DataType parse(String typeName) {
		if ("int".equals(typeName)) {
			return DataType.INT;
		} else if ("long".equals(typeName)) {
			return DataType.LONG;
		} else if ("string".equals(typeName)) {
			return DataType.STRING;
		} else if ("short".equals(typeName)){
			return DataType.SHORT;
		} else {
			return DataType.BYTE;
		}
	}
}
