package com.xunlei.data.hbase.metadata;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 从HBase获取的值都是byte[]，然后根据元数据中的类型解析，最后统一是String类型写入json进文件
 *
 * @author q
 */
public class ColumnTypeTransform {
	
	private static final String NULL_STR = "\\N";

    /**
     * 普通的column，根据columnType判断类型，解析数据
     *
     * @param columnType
     * @param originalValue
     * @return
     */
    public static String transform(DataType columnType, byte[] originalValue) {
        if (columnType == null) {
            return null;
        }

        if (originalValue == null || originalValue.length == 0) {
        	return NULL_STR;
        }
        
        switch (columnType) {
            case LONG:
                return String.valueOf(Bytes.toLong(originalValue));
            case STRING:
                return String.valueOf(Bytes.toString(originalValue));
            case INT:
                return String.valueOf(Bytes.toInt(originalValue));
            case SHORT:
                return String.valueOf(Bytes.toShort(originalValue));
            default:
                return NULL_STR;
        }
    }

    /**
     * rowkey，先判断RowKeyAssembleType对象，然后通过columnType来解析
     *
     * @param columnType
     * @param assembleType
     * @param originalValue
     * @return
     */
    public static String transformRowKey(DataType columnType, RowKeyAssembleType assembleType, byte[] originalValue) {
        if (columnType == null || assembleType == null) {
            return null;
        }
        String value = null;
        switch (assembleType) {
            case NO_ASSEMBLE:
                value = ColumnTypeTransform.transform(columnType, originalValue);
                break;
            case NEED_SPLIT:
                value = ColumnTypeTransform.transform(columnType, originalValue);
                break;
            case DIFFERENT_START:
                value = ColumnTypeTransform.transform(columnType, originalValue);
                break;
            case LENGTH_BYTE_TO_LONG:
                if (originalValue[0]=='t') {
                    // 不正常情况
                    // 不正常情况一般组合是string+long,如果不足8位，则按照string处理
                    if(originalValue.length<8){
                        value = String.valueOf(originalValue);//或者直接处理成\\N
                    }else {
                        byte[] strArray = new byte[originalValue.length - 8];
                        byte[] longArray = new byte[8];
                        // 翻转，先把long弄出来，剩下的给string，然后两个数组再翻转，再组合相应类型
                        ArrayUtils.reverse(originalValue);
                        System.arraycopy(originalValue, 0, longArray, 0, 8);
                        System.arraycopy(originalValue, 8, strArray, 0, strArray.length);
                        ArrayUtils.reverse(longArray);
                        ArrayUtils.reverse(strArray);
                        value = ColumnTypeTransform.transform(DataType.STRING, strArray) + ColumnTypeTransform.transform(DataType.LONG, longArray);
                    }
                } else {
                    if(originalValue.length != Bytes.SIZEOF_LONG * 2) {
                        // 不正常情况
                        int v = Bytes.toInt(originalValue);
                        value = String.valueOf(v);
                    }else {
                        // 正常情况，两个long都换成byte[]
                        byte[] b1 = new byte[]{originalValue[7], originalValue[6], originalValue[5], originalValue[4], originalValue[3], originalValue[2], originalValue[1], originalValue[0]};
                        byte[] b2 = new byte[]{originalValue[8], originalValue[9], originalValue[10], originalValue[11], originalValue[12], originalValue[13], originalValue[14], originalValue[15]};
                        value = ColumnTypeTransform.transform(DataType.LONG, b1) + "_" + ColumnTypeTransform.transform(DataType.LONG, b2);
                    }
                }
                break;
            default:
                value = "";
        }
        return value;
    }

}
