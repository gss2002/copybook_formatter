/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.copybook.mapred;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.Numeric.Convert;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CopybookFileRecordReader extends RecordReader<NullWritable, Text> {
	private static final Log LOG = LogFactory.getLog(CopybookFileRecordReader.class.getName());
	private FileSplit fileSplit;
	private Configuration conf;
	private Text value = new Text();
	private boolean processed = false;
	Path file = null;
	boolean firstRec = true;
	AbstractLineReader reader;
	CobolIoProvider ioProvider;
	int lineNum = 0;
	boolean includeIsArray = false;
	boolean excludeIsArray = false;
	boolean useIncludeRecord;
	boolean useExcludeRecord;
	String includeRecords;
	String excludeRecords;
	String[] includeRecordArray;
	String[] excludeRecordArray;

	Map<Integer, Map<List<String>, List<String>>> includeMap;
	Map<Integer, Map<List<String>, List<String>>> excludeMap;

	int excludeMapSize;
	int includeMapSize;

	boolean useRecord;
	boolean useRecLength;
	String recordLength;

	boolean mrDebug;
	boolean mrTrace;
	boolean mrTraceAll;

	String copybookLayout;
	int copyBookFileType;
	int splitOption;
	int copybookSysType;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.file = fileSplit.getPath();

		this.copybookSysType = conf.getInt("copybook.numericType", Convert.FMT_MAINFRAME);
		this.splitOption = conf.getInt("copybook.splitOption", CopybookLoader.SPLIT_NONE);
		this.copyBookFileType = conf.getInt("copybook.fileType", Constants.IO_VB);
		this.copybookLayout = conf.get("copybook.layout");
		this.recordLength = conf.get("copybook.recordLength");
		this.useIncludeRecord = conf.getBoolean("copybook.include.useRecord", true);
		this.useExcludeRecord = conf.getBoolean("copybook.exclude.useRecord", false);
		this.includeRecords = conf.get("copybook.include.records");
		this.excludeRecords = conf.get("copybook.exclude.records");

		this.useRecLength = conf.getBoolean("copybook.useRecordLength", true);

		this.mrDebug = conf.getBoolean("copybook.debug", false);
		this.mrTrace = conf.getBoolean("copybook.trace", false);
		this.mrTraceAll = conf.getBoolean("copybook.traceall", false);

		LOG.info("Copybook SysType:" + copybookSysType);
		LOG.info("Copybook Split:" + splitOption);
		LOG.info("Copybook FileType:" + copyBookFileType);
		LOG.info("Copybook RecordLayout File:" + copybookLayout);
		LOG.info("Copybook UseRecordLength: " + useRecLength);
		if (useRecLength) {
			LOG.info("Copybook Record Length: " + recordLength);
		}
		LOG.info("Copybook UseIncludeRecord: " + useIncludeRecord);
		if (useIncludeRecord) {
			LOG.info("Copybook includeRecord " + includeRecords);
		}

		LOG.info("Copybook UseExcludeRecord: " + useExcludeRecord);
		if (useExcludeRecord) {
			LOG.info("Copybook excludeRecord " + excludeRecords);
		}

		String[] loggers = { "org.apache.hadoop.copybook.mapred.CopybookFileRecordReader" };

		if (mrDebug) {
			for (String ln : loggers) {
				LOG.info("Enabling Debug");
				org.apache.log4j.Logger.getLogger(ln).setLevel(org.apache.log4j.Level.DEBUG);
			}
		}

		if (mrTrace || mrTraceAll) {
			for (String ln : loggers) {
				LOG.info("Enabling Trace");
				org.apache.log4j.Logger.getLogger(ln).setLevel(org.apache.log4j.Level.TRACE);
			}
		}

		this.ioProvider = CobolIoProvider.getInstance();

		try {
			this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType, this.splitOption,
					this.copybookLayout, this.file, this.conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (useIncludeRecord || useExcludeRecord) {
			if (useIncludeRecord) {
				includeMap = new LinkedHashMap<Integer, Map<List<String>, List<String>>>();
				if (includeRecords.contains(":")) {
					includeIsArray = true;
					includeRecordArray = includeRecords.split(":");
					LOG.info("includeRecordArray::Length " + includeRecordArray.length);
					if (includeIsArray) {
						LOG.debug("includeIsArray=true");
						if (includeRecordArray.length > 1) {
							LOG.debug("includeRecordArray > 1");
							for (int in = 0; in < includeRecordArray.length; in++) {
								LinkedHashMap<List<String>, List<String>> recordMap = new LinkedHashMap<List<String>, List<String>>();
								List<String> recInTypeList = new ArrayList<String>();
								List<String> recInValueList = new ArrayList<String>();
								if (includeRecordArray[in].contains(",")) {
									LOG.debug("includeRecordArray is MultiKeyValue");
									String[] includeRecArray = includeRecordArray[in].split(",");
									LOG.debug("includeRecArray=" + includeRecArray);
									for (int in2 = 0; in2 < includeRecArray.length; in2++) {
										String[] includeRecTypeValueArray = includeRecArray[in2].split("=");
										LOG.debug("includeKey=" + includeRecTypeValueArray[0] + "::includeValue="
												+ includeRecTypeValueArray[1]);
										recInTypeList.add(includeRecTypeValueArray[0]);
										recInValueList.add(includeRecTypeValueArray[1]);
									}
								} else {
									LOG.debug("includeRecordArray is NOT MultiKeyValue");
									String[] includeRecTypeValueArray = includeRecordArray[in].split("=");
									LOG.debug("includeKey=" + includeRecTypeValueArray[0] + "::includeValue="
											+ includeRecTypeValueArray[1]);
									recInTypeList.add(includeRecTypeValueArray[0]);
									recInValueList.add(includeRecTypeValueArray[1]);
								}
								LOG.debug("Adding Entry to includeMap: " + recInTypeList + "::" + recInValueList);
								recordMap.put(recInTypeList, recInValueList);
								includeMap.put(in, recordMap);
							}
						}
					}
				} else {
					List<String> recInTypeList = new ArrayList<String>();
					List<String> recInValueList = new ArrayList<String>();
					LinkedHashMap<List<String>, List<String>> recordMap = new LinkedHashMap<List<String>, List<String>>();
					String[] includeRecArray = includeRecords.split(",");
					for (int in = 0; in < includeRecArray.length; in++) {
						String[] includeRecTypeValueArray = includeRecArray[in].split("=");
						LOG.debug("includeKey=" + includeRecTypeValueArray[0] + "::includeValue="
								+ includeRecTypeValueArray[1]);
						recInTypeList.add(includeRecTypeValueArray[0]);
						recInValueList.add(includeRecTypeValueArray[1]);
					}
					recordMap.put(recInTypeList, recInValueList);
					includeMap.put(0, recordMap);
				}
				includeMapSize = includeMap.size();
				LOG.debug("IncludeMapSize: " + includeMapSize);
			}
			if (useExcludeRecord) {
				excludeMap = new LinkedHashMap<Integer, Map<List<String>, List<String>>>();
				if (excludeRecords.contains(":")) {
					excludeIsArray = true;
					excludeRecordArray = excludeRecords.split(":");
					LOG.info("excludeRecordArray::Length " + excludeRecordArray.length);
					if (excludeIsArray) {
						LOG.debug("excludeIsArray=true");
						if (excludeRecordArray.length > 1) {
							LOG.debug("excludeRecordArray > 1");
							for (int ex = 0; ex < excludeRecordArray.length; ex++) {
								LinkedHashMap<List<String>, List<String>> recordMap = new LinkedHashMap<List<String>, List<String>>();
								List<String> recExTypeList = new ArrayList<String>();
								List<String> recExValueList = new ArrayList<String>();
								if (excludeRecordArray[ex].contains(",")) {
									LOG.debug("excludeRecordArray is MultiKeyValue");
									String[] excludeRecArray = excludeRecordArray[ex].split(",");
									LOG.debug("excludeRecArray=" + excludeRecArray);
									for (int ex2 = 0; ex2 < excludeRecArray.length; ex2++) {
										String[] excludeRecTypeValueArray = excludeRecArray[ex2].split("=");
										LOG.debug("excludeKey=" + excludeRecTypeValueArray[0] + "::excludeValue="
												+ excludeRecTypeValueArray[1]);
										recExTypeList.add(excludeRecTypeValueArray[0]);
										recExValueList.add(excludeRecTypeValueArray[1]);
									}
								} else {
									LOG.debug("excludeRecordArray is NOT MultiKeyValue");
									String[] excludeRecTypeValueArray = excludeRecordArray[ex].split("=");
									LOG.debug("includeKey=" + excludeRecTypeValueArray[0] + "::excludeValue="
											+ excludeRecTypeValueArray[1]);
									recExTypeList.add(excludeRecTypeValueArray[0]);
									recExValueList.add(excludeRecTypeValueArray[1]);
								}
								LOG.debug("Adding Entry to excludeMap: " + recExTypeList + "::" + recExValueList);
								recordMap.put(recExTypeList, recExValueList);
								excludeMap.put(ex, recordMap);
							}
						}
					}
				} else {
					List<String> recExTypeList = new ArrayList<String>();
					List<String> recExValueList = new ArrayList<String>();
					LinkedHashMap<List<String>, List<String>> recordMap = new LinkedHashMap<List<String>, List<String>>();
					String[] excludeRecArray = excludeRecords.split(",");
					for (int ex = 0; ex < excludeRecArray.length; ex++) {
						String[] excludeRecTypeValueArray = excludeRecArray[ex].split("=");
						LOG.debug("excludeKey=" + excludeRecTypeValueArray[0] + "::excludeValue="
								+ excludeRecTypeValueArray[1]);
						recExTypeList.add(excludeRecTypeValueArray[0]);
						recExValueList.add(excludeRecTypeValueArray[1]);
					}
					recordMap.put(recExTypeList, recExValueList);
					excludeMap.put(0, recordMap);
				}
				excludeMapSize = excludeMap.size();
				LOG.debug("ExcludeMapSize: " + excludeMapSize);

			}
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		AbstractLine copyRecord;
		LayoutDetail copySchema = this.reader.getLayout();
		int recordId = 0; // Assuming only one record type In the file
		String cRecord = null;
		StringBuffer includeRecordValue = new StringBuffer();
		StringBuffer includeRecordType = new StringBuffer();
		StringBuffer excludeRecordType = new StringBuffer();
		StringBuffer excludeRecordValue = new StringBuffer();
		String includeValueIn = "";
		String excludeValueIn = "";

		StringBuffer sb = new StringBuffer();

		while ((copyRecord = this.reader.read()) != null) {
			lineNum += 1;

			excludeRecordValue.setLength(0);
			excludeRecordType.setLength(0);
			includeRecordType.setLength(0);
			includeRecordValue.setLength(0);
			boolean recTypeValue = false;

			if (mrDebug) {
				LOG.debug("Record Line::ByteLength: " + lineNum + " :: " + copyRecord.getData().length);
			}
			Integer copyRecLength = copyRecord.getData().length;
			if (useRecLength) {
				LOG.info("Record Line::ByteLength: " + lineNum + " :: " + copyRecLength);
			}

			if (useIncludeRecord) {
				this.useRecord = true;
				includeLoop: for (Integer includeMapKey : includeMap.keySet()) {
					Map<List<String>, List<String>> includeMapList = includeMap.get(includeMapKey);
					includeRecordType.setLength(0);
					includeRecordValue.setLength(0);
					for (List<String> includeMapListKey : includeMapList.keySet()) {
						List<String> includeTypeList = includeMapListKey;
						List<String> includeValueList = includeMapList.get(includeMapListKey);
						for (int i = 0; i < includeTypeList.size(); i++) {
							includeRecordValue
									.append(copyRecord.getFieldValue(includeTypeList.get(i)).asString().trim());
						}
						includeValueIn = String.join("", includeValueList);
						if (includeRecordValue.toString().equalsIgnoreCase(includeValueIn)) {
							String includeTypeIn = String.join("", includeTypeList);
							LOG.debug("Including Line::" + lineNum+ " - RecordType="+includeTypeIn+"::RecordValue=" + includeValueIn);							recTypeValue = true;
							break includeLoop;
						}
					}
				}
			}
			if (useExcludeRecord) {
				this.useRecord = true;
				excludeLoop: for (Integer excludeMapKey : excludeMap.keySet()) {
					Map<List<String>, List<String>> excludeMapList = excludeMap.get(excludeMapKey);
					excludeRecordType.setLength(0);
					excludeRecordValue.setLength(0);
					for (List<String> excludeMapListKey : excludeMapList.keySet()) {
						List<String> excludeTypeList = excludeMapListKey;
						List<String> excludeValueList = excludeMapList.get(excludeMapListKey);
						for (int i = 0; i < excludeTypeList.size(); i++) {
							excludeRecordValue
									.append(copyRecord.getFieldValue(excludeTypeList.get(i)).asString().trim());
						}
						excludeValueIn = String.join("", excludeValueList);
						if (excludeRecordValue.toString().equalsIgnoreCase(excludeValueIn)) {
							String excludeTypeIn = String.join("", excludeTypeList);
							LOG.debug("Excluding Line::" + lineNum+ " - RecordType="+excludeTypeIn+"::RecordValue=" + excludeValueIn);
							recTypeValue = false;
							break excludeLoop;
						}
					}
				}
			}

			boolean recLength = false;

			if (recordLength != null && useRecLength) {
				if (Integer.valueOf(recordLength) == copyRecLength) {
					if (mrDebug || mrTrace || mrTraceAll) {
						LOG.debug("RecordLength String::Integer" + recordLength + " :: " + copyRecLength);
					}
					recLength = true;
				}
			}

			if ((recTypeValue || useRecord == false || recLength == true)) {
				int recCount = copySchema.getRecord(recordId).getFieldCount();
				sb.setLength(0);
				copySchema.getRecord(recordId);
				if (mrDebug || mrTrace || mrTraceAll) {
					LOG.debug("CopySchema FieldCount: " + recCount);
					if (LOG.isTraceEnabled()) {
						LOG.trace("CopyRecord - " + recordId + ": " + copySchema.getRecord(recordId).toString());
						if (mrTraceAll) {
							byte[] recByteArray = copyRecord.getData();
							if (copybookSysType == Convert.FMT_MAINFRAME) {
								Charset charset = Charset.forName("cp037");
								byte[] recByteArrayLE = new String(recByteArray, charset).getBytes();
								LOG.trace("Z/OS EBCIDIC RecordLine:  " + new String(recByteArray));
								LOG.trace("Z/OS EBCIDIC HexString: " + Hex.encodeHexString(recByteArray));
								LOG.trace(
										"Converted EBCIDIC to ASCII RecordLine:  " + new String(recByteArray, charset));
								LOG.trace("Converted EBCIDIC to ASCII HexString:  "
										+ Hex.encodeHexString(recByteArrayLE));
							} else {
								LOG.trace("HexArray RecordLine:  " + new String(recByteArray));
							}
						}
					}
				}
				for (int i = 0; i < copySchema.getRecord(recordId).getFieldCount(); i++) {
					FieldDetail field = copySchema.getRecord(recordId).getField(i);
					// Clean the record before passing to stringbuffer appender
					try {
						cRecord = removeBadChars(copyRecord.getFieldValue(field).asString()).trim();
						if (cRecord == null || cRecord.isEmpty()) {
							sb.append("NULL");
						} else {
							sb.append(cRecord);
						}

					} catch (StringIndexOutOfBoundsException e) {
						String fontName = copySchema.getRecord(recordId).getFontName();
						String typeName = copyRecord.getFieldValue(field).getTypeName();
						String fieldName = field.getName();
						sb.append("BAD_FIELD_RECORD");
						LOG.error("Bad Record: Line=" + lineNum + " FieldName=" + fieldName + " FieldType=" + typeName
								+ " FontName=" + fontName);

					}

					if (recCount == i) {
						//No Work Performed Here
					} else {
						sb.append("\t");
					}
				}
				value.set(sb.toString());
				return true;
			}
		}
		reader.close();
		return false;

	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

	public static String removeBadChars(String strToBeTrimmed) {
		String strout = StringUtils.replace(strToBeTrimmed, "\n\r", " ");
		strout = StringUtils.replace(strout, "\r\n", " ");
		strout = StringUtils.replace(strout, "\n", " ");
		strout = StringUtils.replace(strout, "\r", " ");
		strout = StringUtils.replace(strout, "\t", " ");
		strout = StringUtils.replace(strout, "\b", " ");
		if (!(StringUtils.isAsciiPrintable(strout))) {
			strout = Normalizer.normalize(strout, Normalizer.Form.NFD);
			strout = strout.replaceAll("[^\\x00-\\x7F]", "");
		}
		return strout;
	}

}
