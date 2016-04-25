package com.xunlei.data.hbase.export;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xunlei.data.hbase.data.RowData;

/**
 * 
 * ExportFile提供创建一个文件，并往文件里面一行一行写入的功能
 * 
 * 使用方法：创建对象，创建文件，一行一行写入文件，关闭文件
 * 
 * @author q
 *
 */
public class ExportFile {

	private static final Logger logger = LoggerFactory.getLogger(ExportFile.class);

	private String path;
	private FileWriter fileWriter;
	private BufferedWriter buffWriter;

	public ExportFile(String path) {
		this.path = path;
	}

	public void createFile() throws IOException {
		try {
			this.fileWriter = new FileWriter(this.path);
			this.buffWriter = new BufferedWriter(this.fileWriter);
		} catch (IOException e) {
			logger.error("create file: " + this.path + " error", e);
			throw e;
		}
	}

	/**
	 * 一次过来是一个List，遍历，写入文件
	 * 
	 * @param rowData
	 */
	public void append(RowData rowData) {
		try {
			this.buffWriter.write(RowData.toJsonString(rowData));
		} catch (IOException e) {
			logger.error("append to file error", e);
		}
	}
	
	/**
	 * 一个RowData写成一行
	 * 
	 * @param rowDataList
	 */
	public void append(List<RowData> rowDataList) {
		try {
			for (RowData rowData : rowDataList) {
				this.buffWriter.write(RowData.toJsonString(rowData) + System.lineSeparator());
			}
		} catch (IOException e) {
			logger.error("append to file error", e);
		}
	}

	public void closeFile() {
		try {
			if (this.buffWriter != null) {
				this.buffWriter.close();
			}
			if (this.fileWriter != null) {
				this.fileWriter.close();
			}
		} catch (Exception e) {
			logger.error("close file error", e);
		}
	}

}
