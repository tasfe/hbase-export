package com.xunlei.data.hbase.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.xunlei.data.hbase.metadata.ColumnFamilyMetadata;
import com.xunlei.data.hbase.metadata.ColumnMetadata;
import com.xunlei.data.hbase.metadata.DataType;
import com.xunlei.data.hbase.metadata.HBaseMetadata;
import com.xunlei.data.hbase.metadata.RowKeyAssembleType;
import com.xunlei.data.hbase.metadata.TableMetadata;

/**
 * MetadataPropFile类读取配置文件获取json字符串
 * 
 * HBaseMetadataBuilder根据此json字符串生成HBaseMetadata对象
 * 
 * @author q
 *
 */
public class HBaseMetadataBuilder {

	private static final Logger logger = LoggerFactory.getLogger(HBaseMetadataBuilder.class);
	
	/**
	 * 通过MetadataPropFile类创建HBaseMetadata
	 * 
	 * @return
	 */
	public static HBaseMetadata createHBaseMetadata() {
		HBaseMetadata hbaseMetadata = null;
		try {
			hbaseMetadata = JSON.parseObject(MetadataPropFile.builder().getJson(), HBaseMetadata.class);
		} catch (Exception e) {
			logger.error("read prop-file and get json data, paser to object error", e);
		}
		return hbaseMetadata;
	}

	public static void main(String[] args) {
		HBaseMetadata hbase = new HBaseMetadata();
		hbase.setHbaseName("test");
		
		TableMetadata tableMetadata = new TableMetadata("table1");
		tableMetadata.setRowKeyType(DataType.STRING);
		tableMetadata.setRowKeyAssembleType(RowKeyAssembleType.NO_ASSEMBLE);
		ColumnFamilyMetadata columeMetadata = new ColumnFamilyMetadata("bs");
		columeMetadata.addColumnMetadata(new ColumnMetadata("gcid", DataType.STRING));
		columeMetadata.addColumnMetadata(new ColumnMetadata("size", DataType.LONG));
		tableMetadata.addColumnFamilyMetadata(columeMetadata);
		
		hbase.addTableMetadata(tableMetadata);

		System.out.println(JSON.toJSONString(hbase));

		System.out.println(DataType.LONG.name());

		// String s =
		// "{\"column_familys\":[{\"column_family_name\":\"bs\",\"columns\":[{\"column_name\":\"gcid\",\"column_type\":\"STRING\"},{\"column_name\":\"size\",\"column_type\":\"LONG\"}]}],\"data_type\":\"STRING\",\"rowkey_assemble_type\":\"NO_ASSEMBLE\",\"table_name\":\"table1\"}";
		
		JSON.toJSONString(hbase);
		HBaseMetadata meta = JSON.parseObject(MetadataPropFile.builder().getJson(), HBaseMetadata.class);
		System.out.println(meta.getTableMetadataList().get(0).getRowKeyAssembleType());
		System.out.println(meta.getTableMetadataList().get(0).getRowKeyType());
		System.out.println(meta.getTableMetadataList().get(0).getColumnFamilyMetadataList().get(0).getColumnMetadataList().get(0).getColumnName());
		System.out.println(meta.getTableMetadataList().get(0).getColumnFamilyMetadataList().get(0).getColumnMetadataList().get(0).getColumnType());
	}
}
