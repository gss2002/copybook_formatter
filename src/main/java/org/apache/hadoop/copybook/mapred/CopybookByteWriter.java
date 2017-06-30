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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.JRecord.ByteIO.VbByteWriter;
import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.IFieldValue;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.Line;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.JRecord.Numeric.ICopybookDialects;

public class CopybookByteWriter extends RecordWriter<Text, NullWritable> {
	private static final Log LOG = LogFactory.getLog(CopybookByteWriter.class.getName());
	Path path = null;
	boolean firstRec = true;
	AbstractLineReader reader;
	CobolIoProvider ioProvider;
	int lineNum = 0;

	boolean mrDebug;
	boolean mrTrace;
	boolean mrTraceAll;
	boolean sortHeaderSplit;
	boolean sortSplitOffset;

	boolean sortSplitSkip;
	boolean sortSkipEntireRecord;

	String copybookLayout;
	String sortHeaderLayout;
	String sortHeaderLength;
	String sortSplitLayout;
	String sortSplitSkipValue;
	String sortSplitLengthName;
	String sortSkipEntireRecordName = "";
	String sortSkipEntireRecordValue = "";
	String sortSplitOffsetLength = "";

	int copyBookFileType;
	int splitOption;
	int copybookSysType;
	private Configuration conf;
	private FileSystem fs;
	private DataOutputStream output;
	CopybookLoader sortHeaderInt;
	CopybookLoader sortSplitInt;

	LayoutDetail sortHeaderLayoutDetail;
	LayoutDetail sortSplitLayoutDetail;

	public CopybookByteWriter(Path path, Configuration confIn, TaskAttemptContext taskAttemptContext) {
		this.conf = confIn;
		this.path = path;

		this.copybookSysType = conf.getInt("copybook.numericType", Convert.FMT_MAINFRAME);
		this.splitOption = conf.getInt("copybook.splitOption", CopybookLoader.SPLIT_NONE);
		this.copyBookFileType = conf.getInt("copybook.fileType", Constants.IO_VB);
		this.copybookLayout = conf.get("copybook.layout");
		this.sortHeaderLayout = conf.get("copybook.sort.header");
		this.sortHeaderLength = conf.get("copybook.sort.header.length");
		this.sortSplitLayout = conf.get("copybook.sort.split");
		this.sortHeaderSplit = conf.getBoolean("copybook.sort", false);
		this.sortSplitSkip = conf.getBoolean("copybook.sort.skip", false);
		this.sortSplitSkipValue = conf.get("copybook.sort.split.skip.value");
		this.sortSplitLengthName = conf.get("copybook.sort.split.record.length.name");
		this.sortSkipEntireRecord = conf.getBoolean("copybook.sort.skip.entire.record", false);
		this.sortSkipEntireRecordName = conf.get("copybook.sort.skip.entire.record.name");
		this.sortSkipEntireRecordValue = conf.get("copybook.sort.skip.entire.record.value");
		this.sortSplitOffsetLength = conf.get("copybook.sort.split.record.offset.length");
		this.sortSplitOffset = conf.getBoolean("copybook.sort.split.record.offset", false);

		this.mrDebug = conf.getBoolean("copybook.debug", false);
		this.mrTrace = conf.getBoolean("copybook.trace", false);
		this.mrTraceAll = conf.getBoolean("copybook.traceall", false);

		LOG.info("Copybook SysType:" + copybookSysType);
		LOG.info("Copybook Split:" + splitOption);
		LOG.info("Copybook FileType:" + copyBookFileType);
		LOG.info("Copybook RecordLayout File:" + copybookLayout);

		String[] loggers = { "org.apache.hadoop.copybook.mapred.CopybookByteWriter" };

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

	}

	@Override
	public void write(Text key, NullWritable value) throws IOException {

		String file = key.toString();
		if (sortHeaderSplit) {
			this.copybookLayout = this.sortHeaderLayout;
		}
		try {
			this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType, this.splitOption,
					this.copybookLayout, new Path(file), this.conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (sortHeaderSplit) {
			sortHeaderInt = new CobolCopybookLoader();
			String font = "";
			if (this.copybookSysType == ICopybookDialects.FMT_MAINFRAME) {
				font = "cp037";
			}
			try {
				this.sortHeaderLayoutDetail = sortHeaderInt
						.loadCopyBook(this.sortHeaderLayout, this.splitOption, 0, font,
								CommonBits.getDefaultCobolTextFormat(), this.copybookSysType, 0, null)
						.setFileStructure(this.copyBookFileType).asLayoutDetail();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sortSplitInt = new CobolCopybookLoader();
			try {
				this.sortSplitLayoutDetail = sortSplitInt
						.loadCopyBook(this.sortSplitLayout, this.splitOption, 0, font,
								CommonBits.getDefaultCobolTextFormat(), this.copybookSysType, 0, null)
						.setFileStructure(this.copyBookFileType).asLayoutDetail();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		AbstractLine copyRecord;
		int recordId = 0; // Assuming only one record type In the file
		LayoutDetail copySchema = this.reader.getLayout();
		VbByteWriter writer = new VbByteWriter();
		Path tempPath = new Path(file);
		String parentPath = path.getParent().toString();
		String fileString = parentPath + "/" + tempPath.getName().replaceAll("\'", "");
		Path fileOut = new Path(fileString);
		this.fs = fileOut.getFileSystem(conf);
		this.output = fs.create(fileOut, false);
		writer.open(output);
		while ((copyRecord = this.reader.read()) != null) {
			lineNum += 1;
			if (mrDebug) {
				LOG.debug("Record Line::ByteLength: " + lineNum + " :: " + copyRecord.getData().length);
			}
			if (mrTraceAll) {
				LOG.trace("Ebcidc Record Message: "+Hex.encodeHexString(copyRecord.getData()));
			}
			Integer startLength = 0;
			Integer nextRecLength = 0;
			Integer copyRecLength = copyRecord.getData().length;
			String skipRecordHex = "";
			FieldDetail testField = null;
			if (sortSkipEntireRecord) {

				skipRecordHex = copyRecord.getFieldValue(sortSkipEntireRecordName).asHex();
			}
			if (!(copyRecLength <= Integer.parseInt(sortHeaderLength))) {
				if (sortSkipEntireRecord && sortSkipEntireRecordValue.equalsIgnoreCase(skipRecordHex)) {
					LOG.warn("Skipping/Discarding Record LineNum: " + lineNum);
				} else {
					byte[] headerBytes = copyRecord.getData(1, Integer.parseInt(sortHeaderLength));
					LOG.debug("MaxSplitLength" + sortSplitLayoutDetail.getMaximumRecordLength());
					AbstractLine tempRecord = new Line(sortSplitLayoutDetail);
					tempRecord.setLayout(sortSplitLayoutDetail);
					LOG.info("SplitRecordLayout: " + tempRecord.getLayout().getLayoutName());
					startLength = Integer.parseInt(sortHeaderLength) + 1;
					internalRecLoop: do {
						byte[] segmentBytes = copyRecord.getData(startLength,
								sortSplitLayoutDetail.getMaximumRecordLength());
						tempRecord.setData(segmentBytes);
						int nextDecimal = 0;
						nextRecLength = Integer.parseInt(tempRecord.getFieldValue(sortSplitLengthName).asString());
						if (mrTraceAll) {
							LOG.trace("Ebcidc Segment Id/Length Message: "+Hex.encodeHexString(tempRecord.getData()));
						}
						if ((nextRecLength < 0) || (nextRecLength > copyRecLength)) {
							LOG.error("!!Record Error!! Breaking Out of Loop: " + lineNum);
							LOG.error("!!Reord Error!! NextRecLength " + nextRecLength + " < 0  or "
									+ nextRecLength + "(NextRecLength) >" + copyRecLength+"(copyRecLength)");
							LOG.error("!!Record Error!! Segment Length/ID Hex: "+Hex.encodeHexString(tempRecord.getData()));
							LOG.error("!!Record Error!! CopyRecord Hex: "+Hex.encodeHexString(copyRecord.getData()));
							break internalRecLoop;
						}
						if (nextRecLength == 0) {
							if (sortSplitSkip) {
								if (copybookSysType == Convert.FMT_MAINFRAME) {
									LOG.trace("Z/OS EBCIDIC HexString: " + Hex.encodeHexString(segmentBytes)
											.substring(0, sortSplitLayoutDetail.getMaximumRecordLength()));
									String checkHex = Hex.encodeHexString(segmentBytes).substring(0,
											sortSplitLayoutDetail.getMaximumRecordLength() / 2);
									if (checkHex.equalsIgnoreCase(sortSplitSkipValue)) {
										String checkHexLength = Hex.encodeHexString(segmentBytes).substring(
												sortSplitLayoutDetail.getMaximumRecordLength() / 2,
												sortSplitLayoutDetail.getMaximumRecordLength());
										LOG.trace("HEX VALUE: " + checkHexLength);
										nextDecimal = Integer.parseInt("00" + checkHexLength, 16);
										LOG.trace("Decimal: " + nextDecimal);
									} else {
										LOG.error("!!Record Error!! Breaking Out of Loop: " + lineNum);
										LOG.error("!!Record Error!! Segment Length/ID Hex: "+Hex.encodeHexString(tempRecord.getData()));
										LOG.error("!!Record Error!! CopyRecord Hex: "+Hex.encodeHexString(copyRecord.getData()));	
										break internalRecLoop;
									}
									nextRecLength = nextDecimal;
								}
							} else {
								LOG.error("!!Record Error!! Breaking Out of Loop: " + lineNum);
								LOG.error("!!Record Error!! Segment Length/ID Hex: "+Hex.encodeHexString(tempRecord.getData()));
								LOG.error("!!Record Error!! CopyRecord Hex: "+Hex.encodeHexString(copyRecord.getData()));
								break internalRecLoop;
							}
						}
						if (sortSplitOffset) {
							LOG.debug("Offset Used");
							LOG.debug("Next RecordLength before Offset: "+nextRecLength);
							nextRecLength=nextRecLength + Integer.parseInt(sortSplitOffsetLength);
							LOG.debug("NextRecLength after Offset: " + nextRecLength);

						} else {
							LOG.debug("NextRecLength: " + nextRecLength);
						}
						byte[] dataBytes = copyRecord.getData(startLength, nextRecLength - 1);
						byte[] concatBytes = ArrayUtils.addAll(headerBytes, dataBytes);
						startLength = nextRecLength + startLength;
						LOG.debug("Next Record StartLength: " + startLength);
						LOG.debug("Total Record Length::" + copyRecLength + ", FileName::" + file);
						writer.write(concatBytes);
						output.flush();
					} while (copyRecLength > startLength);
				}
			}
		}
		writer.close();
	}

	@Override
	public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		if (this.output != null) {
			this.output.close();
		}
	}
}
