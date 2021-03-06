package com.hortonworks.ZeroDowntimeDeployment.Spouts;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class AccessLogScheme implements Scheme{

	private static final long serialVersionUID = 1L;

	@Override
	public List<Object> deserialize(byte[] bytes) {
		
		try {
			
			if(bytes == null) {
				return new Values(FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD);
			}
			
			String log = new String(bytes, "UTF-8");
			
			if(log.trim().equals("")){
				return new Values(FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD, FieldNames.BADRECORD);
			}
			
			String[] parts = log.split("\\|");
			//String datetime = parts[0];
			String host = parts[1];
			String module = parts[2];
			String version = parts[3];
			String method = parts[4];
			//String endpoint = parts[5];
			String responseCode = parts[6];
			String latency = parts[7];
			//String content = parts[8];
			
			return new Values(host, module, version, method, responseCode, latency);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(
				FieldNames.HOST,
				FieldNames.MODULE,
				FieldNames.VERSION,
				FieldNames.METHOD,
				FieldNames.RESPONSECODE,
				FieldNames.LATENCY
		);
	}

	
}
