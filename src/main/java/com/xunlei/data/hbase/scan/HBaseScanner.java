package com.xunlei.data.hbase.scan;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xunlei.data.hbase.data.RowData;
import com.xunlei.data.hbase.export.ExportFile;
import com.xunlei.data.hbase.export.MetadataPropFile;
import com.xunlei.data.hbase.metadata.ColumnFamilyMetadata;
import com.xunlei.data.hbase.metadata.ColumnMetadata;
import com.xunlei.data.hbase.metadata.ColumnTypeTransform;
import com.xunlei.data.hbase.metadata.TableMetadata;

/**
 * 
 * 核心类 读取配置文件，与HBase创建链接，获得ResultScanner对象； 
 * 
 * 遍历ResultScanner对象（相当于一个游标），获取每行的Result对象； 
 * 
 * 从Result对象里面根据元数据去或者rowkey和各个column字段的值； 
 * 
 * 将字段名字和值都put到RowData对象，一行一个RowData对象；
 * 
 * 获取每一个字段的值的时候，把此字段在HBase里面存储的timestamp值也获取了，key就是字段名称加下划线加timestamp；
 * 
 * RowData存在List对象里面，满100000行后，会刷新到文件；
 * 
 * 文件由ExportFile对象管理，文件名称为表加下环线加日期；
 * 
 * @author q
 *
 */
public class HBaseScanner {

	private static final Logger logger = LoggerFactory.getLogger(HBaseScanner.class);

	private static final String HBASE_CONN_PROPERTIES_FILE = "/hbase-export.properties";

	private static final int FLUSH_TO_FILE_SIZE = 100000;
	private static final String TIME_KEY_STR = "_timestamp";

	private static Configuration HBASE_CONF;

	private static Properties PROP;

	static {
		/*
		 * 加载配置文件，获取各个属性值
		 */
		PROP = new Properties();
		try {
			PROP.load(MetadataPropFile.class.getClass().getResourceAsStream(HBASE_CONN_PROPERTIES_FILE));
		} catch (IOException e) {
			logger.error("load prop-file: " + HBASE_CONN_PROPERTIES_FILE + " error", e);
		}
	}

	static {
		/*
		 * 设置HBase的CONF
		 */
		HBASE_CONF = HBaseConfiguration.create();
		HBASE_CONF.set("hbase.zookeeper.quorum", PROP.getProperty("hbase.zookeeper.quorum")/* 192.168.226.154 */);
		HBASE_CONF.set("hbase.zookeeper.property.clientPort", PROP.getProperty("hbase.zookeeper.property.clientPort")/* 2181 */);
		HBASE_CONF.set("zookeeper.znode.parent", PROP.getProperty("zookeeper.znode.parent")/* hbase */);
	}

	private TableMetadata tableMetadata;
	private List<RowData> rowDataBuffList;

	private Connection conn;
	private Table table;
	private ResultScanner resultScanner;

	private String filePath;
	private ExportFile exportFile;

	public HBaseScanner(TableMetadata tableMetadata) {
		this.tableMetadata = tableMetadata;
		this.rowDataBuffList = new ArrayList<RowData>();
		this.filePath = this.createFilePath();
	}

	public void scanResultScanner() {
		this.rowDataBuffList.clear();

		/*
		 * 创建链接，获取表的ResultScanner对象
		 */
		this.conn = this.createConnection();
		this.table = this.createTable(this.conn);
		this.resultScanner = this.createResultScanner(this.table);

		if (this.resultScanner == null) {
			return;
		}

		int line = 0;
		int allLine = 0;
		// 创建文件
		this.exportFile = new ExportFile(this.filePath);
		try {
			this.exportFile.createFile();
			for (Result result : this.resultScanner) {
				// 一个result是hbase里面的一行，并生成RowData对象
				RowData rowData = this.getDataFromResultScanner(result);
				this.rowDataBuffList.add(rowData);
				allLine++;
				if (++line >= FLUSH_TO_FILE_SIZE) {
					// flush to disk
					this.exportFile.append(this.rowDataBuffList);
					logger.info(String.format("scan table[%s] line[%d], save to disk", this.tableMetadata.getTableName(), allLine));
					line = 0;
					this.rowDataBuffList.clear();

				}
			}

			// flush to disk
			this.exportFile.append(this.rowDataBuffList);
			logger.info(String.format("scan table[%s] line[%d], save to disk end", this.tableMetadata.getTableName(), allLine));
			line = 0;
			allLine = 0;
			this.rowDataBuffList.clear();
		} catch (Exception e) {
			logger.error("scanner and flush to disk error", e);
		} finally {
			this.exportFile.closeFile();
			try {
				if (this.resultScanner != null) {
					this.resultScanner.close();
				}
				if (this.table != null) {
					this.table.close();
				}
				if (this.conn != null) {
					this.conn.close();
				}
			} catch (IOException e) {
				logger.error("clost connection/table error", e);
			}
		}

	}

	/**
	 * 
	 * 第一步：获取rowkey的值，put("rowkey", "rowkey的值")
	 * 
	 * 第二步：获取其他column的值，put("字段名称", "字段的值")
	 * 
	 * 第三步：获取此column的timestamp值，put("字段名称_timestamp", "timestamp值")
	 * 
	 * @param result
	 * @return
	 */
	private RowData getDataFromResultScanner(Result result) {
		RowData rowData = new RowData();

		// rowkey
		rowData.addKeyValue("rowkey", ColumnTypeTransform.transformRowKey(this.tableMetadata.getRowKeyType(), this.tableMetadata.getRowKeyAssembleType(), result.getRow()));

		// column-family
		for (ColumnFamilyMetadata cfMetadata : this.tableMetadata.getColumnFamilyMetadataList()) {
			// column
			for (ColumnMetadata cloumnMetadata : cfMetadata.getColumnMetadataList()) {
				// column-data
				rowData.addKeyValue(cloumnMetadata.getColumnName(), ColumnTypeTransform.transform(cloumnMetadata.getColumnType(), result.getValue(cfMetadata.getColumnFamilyNameBytes(), cloumnMetadata.getColumnNameBytes())));
				// column-timestamp
				this.addColumnTimestamp(result, rowData, cfMetadata, cloumnMetadata);
			}
		}

		return rowData;
	}

	/**
	 * 获取column的timestamp
	 * 
	 * @param result
	 * @param rowData
	 * @param cfMetadata
	 * @param cloumnMetadata
	 */
	private void addColumnTimestamp(Result result, RowData rowData, ColumnFamilyMetadata cfMetadata, ColumnMetadata cloumnMetadata) {
		Cell cell = result.getColumnLatestCell(cfMetadata.getColumnFamilyNameBytes(), cloumnMetadata.getColumnNameBytes());
		if (cell == null) {
			return;
		}
		rowData.addKeyValue(cloumnMetadata.getColumnName() + TIME_KEY_STR, String.valueOf(cell.getTimestamp()));
	}

	/**
	 * 创建ResultScanner对象
	 * 
	 * @param table
	 * @return
	 */
	private ResultScanner createResultScanner(Table table) {
		if (table == null) {
			return null;
		}

		List<ColumnFamilyMetadata> cfMetadataList = this.tableMetadata.getColumnFamilyMetadataList();

		Scan scan = new Scan();
		// 增加所有的column-family
		for (ColumnFamilyMetadata cfMetadata : cfMetadataList) {
			scan.addFamily(cfMetadata.getColumnFamilyNameBytes());
		}
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
		} catch (IOException e) {
			logger.error("create result-scanner on " + table.getName() + " error", e);
		}
		return resultScanner;
	}

	private Table createTable(Connection conn) {
		if (conn == null) {
			return null;
		}

		Table table = null;
		try {
			table = conn.getTable(this.tableMetadata.getTableNameHBase());
		} catch (IOException e) {
			logger.error("create table with hbase error", e);
		}
		return table;
	}

	private Connection createConnection() {
		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(HBASE_CONF);
		} catch (IOException e) {
			logger.error("create connection with hbase error", e);
		}
		return conn;
	}

	/**
	 * 根据配置文件里面配置的hbase.export.file.path属性；
	 * 
	 * 获取文件写入的目录 再根据元数据中表的名称加日期，生成文件路径；
	 * 
	 * 注意：hbase.export.file.path属性是目录，末尾需要加上/
	 * 
	 * @return
	 */
	private String createFilePath() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		// 日期是需要减一的
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String dateStr = format.format(calendar.getTime());

		return PROP.getProperty("hbase.export.file.path"/* /data/apps/logs/ */) + this.tableMetadata.getTableName() + "_" + dateStr;
	}

}
