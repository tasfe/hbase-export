package com.xunlei.data.hbase.scan;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;

import com.xunlei.data.hbase.metadata.CaptureType;
import com.xunlei.data.hbase.metadata.TableMetadata;
import com.xunlei.data.hbase.util.DateUtil;

public class ScanCaptureTypeSetter {

	public static Scan scanIncrement(Scan scan, TableMetadata tableMetadata) throws IOException {
		//
		CaptureType captureType = tableMetadata.getCaptureType();
		
		if (captureType == null) {
			return scan;
		}

		switch (captureType) {
		case SNAPSHOT:
			break;
		case INCREMENT:
			scan.setTimeRange(DateUtil.getPrevDateStartTimestamp(), DateUtil.getCurrDateStartTimestamp());
			break;
		default:
			break;
		}

		return scan;
	}

}
