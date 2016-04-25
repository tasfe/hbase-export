package com.xunlei.data.hbase;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xunlei.data.hbase.export.HBaseMetadataBuilder;
import com.xunlei.data.hbase.metadata.HBaseMetadata;
import com.xunlei.data.hbase.metadata.TableMetadata;
import com.xunlei.data.hbase.scan.HBaseScanner;

/**
 * Boot
 * 
 * @author q
 *
 */
public class ExportBoot {

	private static final Logger logger = LoggerFactory.getLogger(ExportBoot.class);
	
	private HBaseMetadata hbaseMetadata;
	
	public ExportBoot() {
		this.initMetadata();
	}
	
	public void initMetadata() {
		logger.info("start to get metadata");
		this.hbaseMetadata = HBaseMetadataBuilder.createHBaseMetadata();
		logger.info("get metadata successfully");
	}
	
	public void exportTables() {
		logger.info("start to export tables");
		List<TableMetadata> tableMetadataList = this.hbaseMetadata.getTableMetadataList();
		for (TableMetadata tableMetadata : tableMetadataList) {
			logger.info("start to export table: " + tableMetadata.getTableName());
			HBaseScanner scanner = new HBaseScanner(tableMetadata);
			scanner.scanResultScanner();
			logger.info("export table: " + tableMetadata.getTableName() + " end");
		}
		logger.info("export tables end");
	}
	
	
	public static void main(String[] args) {
		logger.info("*********************************************");
		logger.info("************  hbase export start ************");
		logger.info("*********************************************");
		ExportBoot boot = new ExportBoot();
		boot.exportTables();
		logger.info("*********************************************");
		logger.info("************  hbase export end **************");
		logger.info("*********************************************");
	}
	
}
