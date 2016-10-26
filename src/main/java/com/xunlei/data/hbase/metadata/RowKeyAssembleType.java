package com.xunlei.data.hbase.metadata;

/**
 * rowkey的几种类型：
 * 
 * long/String对象，直接存HBase：NO_ASSEMBLE
 * 
 * 多个String对象通过下划线拼接，存HBase：NEED_SPLIT
 * 
 * 有的是#开头，有的是*开头，String类型，存HBase：DIFFERENT_START
 * 
 * 两个long类型，转换为byte[8]，两个byte[8]拼接，则是byte[16]，然后byte[]类型之间存HBase: LENGTH_BYTE_TO_LONG
 * 
 * @author q
 *
 */
public enum RowKeyAssembleType {

	NO_ASSEMBLE("no_assemble"), 
	NEED_SPLIT("need_split"), 
	DIFFERENT_START("different_start"), 
	LENGTH_BYTE_TO_LONG("length_byte_to_long"), 
	ARRAY_REVERSE("arry_reverse");

	private String typeName;

	private RowKeyAssembleType(String typeName) {
		this.name();
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public static RowKeyAssembleType parse(String typeName) {
		if ("no_assemble".equals(typeName)) {
			return RowKeyAssembleType.NO_ASSEMBLE;
		} else if ("need_split".equals(typeName)) {
			return RowKeyAssembleType.NEED_SPLIT;
		} else if ("different_start".equals(typeName)) {
			return RowKeyAssembleType.DIFFERENT_START;
		} else if ("arry_reverse".equals(typeName)) {
			return RowKeyAssembleType.ARRAY_REVERSE;
		} else {
			return RowKeyAssembleType.LENGTH_BYTE_TO_LONG;
		}
	}

}
