package com.xunlei.data.hbase.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读取配置文件metadata.properties(里面是一个大json)，将文件内容以String提供出来
 * 
 * @author q
 *
 */
public class MetadataPropFile {

	private static final Logger logger = LoggerFactory.getLogger(MetadataPropFile.class);

	public static final String METADATA_PROP_FILE_PATH = "/metadata.properties";

	private StringBuffer jsonBuff;

	public static MetadataPropFile builder() {
		return new MetadataPropFile();
	}

	private MetadataPropFile() {
		this.jsonBuff = new StringBuffer();
	}

	public String getJson() {
		InputStreamReader inputStreamReader = new InputStreamReader(MetadataPropFile.class.getClass().getResourceAsStream(METADATA_PROP_FILE_PATH));
		BufferedReader buffReader = null;
		try {
			buffReader = new BufferedReader(inputStreamReader);
			String tempString = null;
			while ((tempString = buffReader.readLine()) != null) {
				this.jsonBuff.append(tempString);
			}
		} catch (Exception e) {
			logger.error("read file: " + METADATA_PROP_FILE_PATH + " error", e);
		} finally {
			if (buffReader != null) {
				try {
					buffReader.close();
				} catch (IOException e) {
					logger.error("close buffered-reader file: " + METADATA_PROP_FILE_PATH + " error", e);
				}
			}
		}
		return this.jsonBuff.toString();
	}

}
