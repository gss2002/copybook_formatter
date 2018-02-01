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

package org.apache.hadoop.copybook.mapred.input;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.Line;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.JRecord.Numeric.ICopybookDialects;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.copybook.mapred.utils.GetCopyLayout;
import org.apache.hadoop.copybook.mapred.utils.MergeCopybookImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CopybookMultiFileRecordReader extends RecordReader<Text, Text> {
	// remove start
	boolean includeIsArray = false;
	boolean excludeIsArray = false;

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
	String copybookLayout;
	// remove end

	AbstractLineReader reader;
	CobolIoProvider ioProvider;

	private static final Log LOG = LogFactory.getLog(CopybookMultiFileRecordReader.class.getName());
	private FileSplit fileSplit;
	private Configuration conf;
	private Text value = new Text();
	private Text key = new Text();

	private boolean processed = false;
	Path file = null;
	boolean firstRec = true;
	int lineNum = 0;

	String copybookJson;
	String headerLayout;
	Map<String, Map<String, Map<List<String>, List<String>>>> includeRecordsMap;
	Map<String, Map<String, Map<List<String>, List<String>>>> excludeRecordsMap;
	Map<String, String> copyRecordsMap;
	Map<String, String> initialCopyRecordsMap;

	Map<String, String> tempRecordsMap;
	Map<String, LayoutDetail> copyRecordLayoutMap;
	boolean useIncludeRecord = false;
	boolean useExcludeRecord = false;
	boolean useHeaderLayout = false;
	String font;
	boolean mrDebug = false;
	boolean mrTrace = false;
	boolean mrTraceAll = false;
	int copyBookFileType;
	int splitOption;
	int copybookSysType;
	String copyRecordType;
	boolean useHivePartitionDay;
	boolean useHivePartitionMonth;
	boolean useHivePartitionYear;
	boolean useCopybookPartition = false;
	String copybookPartitionPath = "";



	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.file = fileSplit.getPath();

		this.copybookSysType = conf.getInt("copybook.numericType", Convert.FMT_MAINFRAME);
		this.splitOption = conf.getInt("copybook.splitOption", CopybookLoader.SPLIT_NONE);
		this.copyBookFileType = conf.getInt("copybook.fileType", Constants.IO_VB);
		this.copybookJson = conf.get("copybook.json");
		this.useCopybookPartition = conf.getBoolean("copybook.json.usePartition", false);
		if (useCopybookPartition) {
			this.copybookPartitionPath = conf.get("copybook.json.partition");
		}
		this.mrDebug = conf.getBoolean("copybook.debug", false);
		this.mrTrace = conf.getBoolean("copybook.trace", false);
		this.mrTraceAll = conf.getBoolean("copybook.traceall", false);

		LOG.info("Copybook SysType:" + copybookSysType);
		LOG.info("Copybook Split:" + splitOption);
		LOG.info("Copybook FileType:" + copyBookFileType);
		LOG.info("Copybook JSON: " + copybookJson);
		LOG.info("Copybook usePartition: "+useCopybookPartition);
		LOG.info("Copybook Partition: "+copybookPartitionPath);


		String[] loggers = { CopybookMultiFileRecordReader.class.getCanonicalName() };

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
		if (copybookSysType == ICopybookDialects.FMT_MAINFRAME) {
			font = "cp037";
		}

		GetCopyLayout gcl = new GetCopyLayout(copybookJson);

		this.includeRecordsMap = gcl.getIncludeRecords(gcl.getJSONObject());
		this.excludeRecordsMap = gcl.getExcludeRecords(gcl.getJSONObject());
		this.initialCopyRecordsMap = gcl.getRecord2CopyLayout(gcl.getJSONObject());
		this.headerLayout = gcl.getHeaderLayout(gcl.getJSONObject());

		if (this.headerLayout != null) {
			if (!(this.headerLayout.isEmpty())) {
				this.useHeaderLayout = true;
			}
		}
		if (this.initialCopyRecordsMap != null) {
			LOG.debug("initialCopyRecordsMap size: " + this.initialCopyRecordsMap.size());
		} else {
			LOG.debug("initialCopyRecordsMap size: null");

		}
		if (this.includeRecordsMap != null) {
			LOG.debug("includeRecordsMap size: " + this.includeRecordsMap.size());
		} else {
			LOG.debug("includeRecordsMap size: null");

		}
		if (this.excludeRecordsMap != null) {
			LOG.debug("excludeRecordsMap size: " + this.excludeRecordsMap.size());
		} else {
			LOG.debug("excludeRecordsMap size: null");

		}
		LOG.debug("useHeaderLayout: " + useHeaderLayout);

		if (this.includeRecordsMap != null) {
			this.useIncludeRecord = true;
		}
		LOG.debug("useIncludeRecord: " + this.useIncludeRecord);

		if (this.excludeRecordsMap != null) {
			this.useExcludeRecord = true;
		}
		LOG.debug("useExcludeRecord: " + this.useExcludeRecord);

		if (this.useHeaderLayout) {
			this.tempRecordsMap = new LinkedHashMap<String, String>();
			for (String copyRecordKey : this.initialCopyRecordsMap.keySet()) {
				MergeCopybookImpl mci = new MergeCopybookImpl();
				List<File> copybookArrayList = new ArrayList<File>();
				String[] copyRecordKeyArray = this.initialCopyRecordsMap.get(copyRecordKey).split("/");
				String mergedCopyRecordKey = copyRecordKeyArray[copyRecordKeyArray.length - 1];
				String mergeCopyLayout = "./merged-" + mergedCopyRecordKey;
				copybookArrayList.add(new File(this.headerLayout));
				copybookArrayList.add(new File(this.initialCopyRecordsMap.get(copyRecordKey)));

				File[] fileList = copybookArrayList.toArray(new File[copybookArrayList.size()]);

				try {
					mci.joinFiles(new File(mergeCopyLayout), fileList);
					this.tempRecordsMap.put(copyRecordKey, mergeCopyLayout);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			this.copyRecordsMap = this.tempRecordsMap;
		} else {
			this.copyRecordsMap = this.initialCopyRecordsMap;
		}

		this.ioProvider = CobolIoProvider.getInstance();
		this.copyRecordLayoutMap = new LinkedHashMap<String, LayoutDetail>();

		for (String copyRecordKey : this.copyRecordsMap.keySet()) {
			CobolCopybookLoader copybookLoader = new CobolCopybookLoader();
			LayoutDetail copybookLayoutDetail = null;
			LOG.debug("copyRecordsMap.get(copyRecordKey): " + this.copyRecordsMap.get(copyRecordKey));
			try {
				copybookLayoutDetail = copybookLoader
						.loadCopyBook(this.copyRecordsMap.get(copyRecordKey), this.splitOption, 0, this.font,
								CommonBits.getDefaultCobolTextFormat(), this.copybookSysType, 0, null)
						.setFileStructure(this.copyBookFileType).asLayoutDetail();
				LOG.debug("copybookLayoutDetailName: " + copybookLayoutDetail.getLayoutName());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (copybookLayoutDetail == null) {
				LOG.debug("copybookLayoutDetail is null");
			}
			this.copyRecordLayoutMap.put(copyRecordKey, copybookLayoutDetail);

		}
		if (this.useHeaderLayout) {
			for (String key : this.copyRecordsMap.keySet()) {
				this.headerLayout = this.copyRecordsMap.get(key);
				break;
			}
			try {
				this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType,
						this.splitOption, this.headerLayout, this.file, this.conf);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			for (String key : this.copyRecordsMap.keySet()) {
				this.copybookLayout = this.copyRecordsMap.get(key);
				break;
			}
			try {
				this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType,
						this.splitOption, this.copybookLayout, this.file, this.conf);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		AbstractLine copyRecord;
		LayoutDetail copySchema = null;
		int recordId = 0; // Assuming only one record type In the file
		String cRecord = null;
		StringBuffer includeRecordValue = new StringBuffer();
		StringBuffer includeRecordType = new StringBuffer();
		StringBuffer excludeRecordType = new StringBuffer();
		StringBuffer excludeRecordValue = new StringBuffer();
		String includeValueIn = "";
		String excludeValueIn = "";
		LayoutDetail copySchemaJunk = this.reader.getLayout();

		StringBuffer sb = new StringBuffer();

		while ((copyRecord = this.reader.read()) != null) {
			lineNum += 1;

			excludeRecordValue.setLength(0);
			excludeRecordType.setLength(0);
			includeRecordType.setLength(0);
			includeRecordValue.setLength(0);
			boolean recTypeValue = false;

			Integer copyRecLength = copyRecord.getData().length;
			if (useRecLength) {
				LOG.info("Record Line::ByteLength: " + lineNum + " :: " + copyRecLength);
			}

			if (useIncludeRecord) {
				this.useRecord = true;
				includeLoop: for (String includeRecordMapKey : this.includeRecordsMap.keySet()) {
					Map<String, Map<List<String>, List<String>>> includeRecordMap = this.includeRecordsMap
							.get(includeRecordMapKey);
					for (String includeMapRecordKey : includeRecordMap.keySet()) {

						Map<List<String>, List<String>> includeMapList = includeRecordMap.get(includeMapRecordKey);
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
								LOG.debug("Including Line::" + lineNum + " - RecordType=" + includeTypeIn
										+ "::RecordValue=" + includeValueIn);
								recTypeValue = true;
								copySchema = this.copyRecordLayoutMap.get(includeRecordMapKey);
								LOG.trace("CopySchema: " + copySchema.getLayoutName());
								LOG.trace("includeMapRecordKey/copyRecordType: " + includeMapRecordKey);
								copyRecordType = includeMapRecordKey;
								copySchema.getRecord(recordId);
								copyRecord.setLayout(copySchema);
								copySchema = copyRecord.getLayout();
								LOG.debug("CopySchemaLayoutName After Apply: " + copySchema.getLayoutName());
								break includeLoop;
							}
						}
					}
				}
			}

			if (useExcludeRecord) {
				this.useRecord = true;
				excludeLoop: for (String excludeRecordMapKey : this.excludeRecordsMap.keySet()) {
					Map<String, Map<List<String>, List<String>>> excludeRecordMap = this.excludeRecordsMap
							.get(excludeRecordMapKey);
					for (String excludeMapRecordKey : excludeRecordMap.keySet()) {

						Map<List<String>, List<String>> excludeMapList = excludeRecordMap.get(excludeMapRecordKey);
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
							if (excludeRecordValue.toString().equalsIgnoreCase(includeValueIn)) {
								String excludeTypeIn = String.join("", excludeTypeList);
								LOG.debug("Excluding Line::" + lineNum + " - RecordType=" + excludeTypeIn
										+ "::RecordValue=" + excludeValueIn);
								recTypeValue = false;
								break excludeLoop;
							}
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
				sb.setLength(0);
				int recCount = copySchema.getRecord(recordId).getFieldCount();
				copySchema.getRecord(recordId);
				if (mrDebug || mrTrace || mrTraceAll) {
					LOG.debug("CopySchema FieldCount: " + recCount);
					if (LOG.isTraceEnabled()) {
						LOG.trace("CopyRecord - " + recordId + ": " + copySchema.getRecord(recordId).toString());
						if (mrTraceAll) {
							byte[] recByteArray = copyRecord.getData();
							if (copybookSysType == Convert.FMT_MAINFRAME) {
								Charset charset = Charset.forName("cp037");
								LOG.trace("Z/OS EBCIDIC HexString: " + Hex.encodeHexString(recByteArray));
							} else {
								LOG.trace("HexArray RecordLine:  " + new String(recByteArray));
							}
						}
					}
				}
				LOG.trace("Moving on to Next Phase extracting records");
				for (int i = 0; i < copySchema.getRecord(recordId).getFieldCount(); i++) {
					FieldDetail field = copySchema.getRecord(recordId).getField(i);
					// Clean the record before passing to stringbuffer appender
					try {
						cRecord = removeBadChars(copyRecord.getFieldValue(field).asString()).trim();
						if (mrTraceAll) {
							LOG.trace("cRecord Field=" + field.getName() + " :: cRecordValue=" + cRecord);
						}
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
						// No Work Performed Here
					} else {
						sb.append("\t");
					}
				}
				LOG.trace("Output to Mapper: " + sb.toString());
				LOG.trace("Moving on to Next Phase send records to mapper");
				if (useCopybookPartition) {
					key.set(copyRecordType+"/"+copybookPartitionPath);
				} else {
					key.set(copyRecordType);
				}
				value.set(sb.toString());
				return true;
			}
		}
		LOG.trace("Moving on to Next Phase ending mapper");

		reader.close();
		return false;

	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
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
