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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.Numeric.Convert;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.copybook.mapred.input.CopybookByteInputFormat;
import org.apache.hadoop.copybook.mapred.input.CopybookInputFormat;
import org.apache.hadoop.copybook.mapred.input.CopybookMultiInputFormat;
import org.apache.hadoop.copybook.mapred.output.CopybookByteOutputFormat;
import org.apache.hadoop.copybook.mapred.output.CopybookMultiOutputFormat;
import org.apache.hadoop.copybook.mapred.utils.CopybookParser;
import org.apache.hadoop.copybook.mapred.utils.GetCopyLayout;
import org.apache.hadoop.copybook.mapred.utils.MergeCopybookImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class CopybookDriverImpl {
	private static final Log LOG = LogFactory.getLog(CopybookDriverImpl.class.getName());
	static Options options = new Options();
	static CommandLine cmd;
	
	static Configuration conf;
	static Job job;
	static String jobname;
	static FileSystem fs;
	static Path fsTempPath;
	static String tempPath;

	static String font = null;
	static int numericType = 0;
	static int splitOption = 0;
	static int copybookFileType = 0;
	static String inputPath = "";
	static String tempPathCopy;
	static String outputPath = "";
	static String appname = "";

	static String recLength = "";
	static String copybookLayout = "";
	static String copybookHdfsLayout = null;
	static String sortHeaderHdfsLayout = null;
	static String sortSplitHdfsLayout = null;
	static String copybookType = "";
	static String copybookSplitOpt = "NOSPLIT";
	static String copybookJsonLayout = "";

	static boolean useRecLength = false;
	static boolean debug = false;
	static boolean trace = false;
	static boolean traceall = false;
	static boolean isCopybookLayout = false;
	static boolean isCopybookJsonLayout = false;
	static boolean useHeaderLayout;
	static boolean isMergeCopyLayout = false;

	static String hivePartition = null;
	static String hiveTableName = null;
	static String hivePath = "./";
	static String hiveOutputPartition;

	static boolean useHiveTableName = false;
	static boolean generateHiveOnly = false;
	static boolean noGenHive = false;
	static boolean useHivePartition = false;
	static boolean useHivePartitionDate = false;

	static boolean convert = false;
	static String convertType = "";

	static boolean sort = false;
	static String sortHeaderLayout = "";
	static String sortSplitLayout = "";
	static String sortHeaderLength = "";
	static String sortSplitSkipValue = "";
	static String sortSplitLengthName = "";
	static String sortSplitOffsetLength = "";
	static String sortSkipEntireRecordName = "";
	static String sortSkipEntireRecordValue = "";
	static boolean useSortHeaderSplit = false;
	static boolean useSortRecordSkip = false;
	static boolean useSortSplitOffset = false;
	static boolean useSortRecordName = false;
	static boolean useSortSkipEntireRecord = false;

	static File file = null;
	static long currentTimeCopy;
	static long currentNanoTimeCopy;
	static String procTime;

	static boolean legacyUseIncludeRecord = true;
	static boolean legacyUseExcludeRecord = false;
	static String legacyIncludeRecord = "";
	static String legacyExcludeRecord = "";
	
	private static CopybookLoader copybookInt = new CobolCopybookLoader();


	/**
	 * @param args
	 * 
	 * 
	 */

	public static void main(String[] args) throws Exception {
		setTimeStamp();
		conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		setOptions(); 
		
		CommandLineParser parser = new CopybookParser();
		cmd = parser.parse(options, otherArgs);

		parseOptions();

		// Start Job Setup

		if (isCopybookLayout && copybookLayout.contains(",")) {
			legacyMergeCopybookHeaders();
		}
		
		setupCopybookFunctions();

		if (convert && isCopybookLayout) {
			addCopyBooksToClsLdr(copybookLayout);
		}
		if (convert && isCopybookJsonLayout) {
			addCopyBooksToClsLdr(copybookJsonLayout);
		}
		if (sort) {
			addCopyBooksToClsLdr(sortHeaderLayout);
		}

		try {
			if (convert && (!(sort)) && isCopybookLayout) {
				generateHiveTable(copybookLayout);
			}

			if (convert && isCopybookJsonLayout) {
				mergeGenHiveJsonCopybook();
			}

			if (!(generateHiveOnly)) {
				jobSetup();

				if (convert && isCopybookLayout) {
					setupConvertCopybookLayout();
				}
				if (convert && isCopybookJsonLayout) {
					setupConvertCopybookJsonLayout();
				}
				if (sort) {
					setupSortCopybook();
				}

				FileInputFormat.addInputPaths(job, inputPath);
				FileOutputFormat.setOutputPath(job, new Path(outputPath));
				job.waitForCompletion(true);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fs != null || fsTempPath != null) {
				fs.deleteOnExit(fsTempPath);
			}
			if (isMergeCopyLayout) {
				File copyLayoutFile = new File(copybookLayout);
				copyLayoutFile.delete();
			}
		}
	}

	private static void missingParams() {
		String header = "Copybook Formatter";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}

	private static void setTimeStamp() {
		currentTimeCopy = System.currentTimeMillis();
		currentNanoTimeCopy = System.nanoTime();
		procTime = currentTimeCopy + "_" + currentNanoTimeCopy;
	}

	private static String getTimeStamp() {
		return procTime;
	}

	@SuppressWarnings("deprecation")
	private static void addCopyBooksToClsLdr(String file) throws Exception {

		try {
			File fileToAdd = new File(file);
			URL u = new File(fileToAdd.getParent()).toURL();
			ClassLoader sysLoader = ClassLoader.getSystemClassLoader();
			if (sysLoader instanceof URLClassLoader) {
				sysLoader = (URLClassLoader) sysLoader;
				Class<URLClassLoader> sysLoaderClass = URLClassLoader.class;

				// use reflection to invoke the private addURL method
				Method method = sysLoaderClass.getDeclaredMethod("addURL", new Class[] { URL.class });
				method.setAccessible(true);
				method.invoke(sysLoader, new Object[] { u });
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void setupCopybookFunctions() {
		if (copybookType.equalsIgnoreCase("MFVB")) {
			numericType = Convert.FMT_MAINFRAME;
			copybookFileType = Constants.IO_VB;
		}

		if (copybookType.equalsIgnoreCase("MFFB")) {
			numericType = Convert.FMT_MAINFRAME;
			copybookFileType = Constants.IO_FIXED_LENGTH;
		}

		if (copybookType.equalsIgnoreCase("MFDVB")) {
			numericType = Convert.FMT_MAINFRAME_COMMA_DECIMAL;
			copybookFileType = Constants.IO_VB;
		}

		if (copybookSplitOpt.equalsIgnoreCase("REDEFINE")) {
			splitOption = CopybookLoader.SPLIT_REDEFINE;
		}

		if (copybookSplitOpt.equalsIgnoreCase("NOSPLIT")) {
			splitOption = CopybookLoader.SPLIT_NONE;
		}

		if (copybookSplitOpt.equalsIgnoreCase("LEVEL1")) {
			splitOption = CopybookLoader.SPLIT_01_LEVEL;
		}

		if (numericType == Convert.FMT_MAINFRAME) {
			font = "cp037";
		}
	}

	private static void mergeGenHiveJsonCopybook() {

		GetCopyLayout jsonGcl = new GetCopyLayout(copybookJsonLayout);
		String jsonAbsPath = new File(copybookJsonLayout).getAbsolutePath();
		String jsonParentPath = new File(jsonAbsPath).getParent();
		File jsonFilePath = new File(jsonParentPath);
		String mergedCopybookPath = "/tmp/copybook_formatter_" + getTimeStamp();
		String jsonHivePartition = jsonGcl.getHivePartitionData(jsonGcl.getJSONObject());
		System.out.println("jsonHivePartition: "+jsonHivePartition);
		String headerLayout = jsonGcl.getHeaderLayout(jsonGcl.getJSONObject());
		String dateFormat = jsonGcl.getPartitionDateFormat(jsonGcl.getJSONObject());
		useHivePartitionDate = false;
		useHivePartition=false;
		if (jsonHivePartition != null && !(dateFormat.equalsIgnoreCase("NOTSET"))) {
			System.out.println("jsonHivePartition: Not Null");
			if (!(jsonHivePartition.equalsIgnoreCase(""))) {
				System.out.println("jsonHivePartition: Not Empty");
				useHivePartition = true;
				hivePartition = jsonHivePartition;
			}
		}
		if (dateFormat != null) {
			System.out.println("dateFormat: Not Null");
			if (!(dateFormat.equalsIgnoreCase("NOTSET"))) {
				useHivePartitionDate = true;
				useHivePartition = true;
				generatePartitionDateStructure(dateFormat);			
			}
		}

		if (headerLayout != null) {
			if (!(headerLayout.isEmpty())) {
				useHeaderLayout = true;
			}
		}

		Map<String, String> initialCopyRecordsMap = jsonGcl.getRecord2CopyLayout(jsonGcl.getJSONObject());

		if (useHeaderLayout) {
			File headerFilePath = null;
			if (headerLayout.startsWith("file://") || headerLayout.startsWith("/") || headerLayout.startsWith("./")) {
				String headerLayoutLocalPath = new File(headerLayout.replace("file://", "")).getAbsolutePath();
				String headerLayoutParentPath = new File(headerLayoutLocalPath).getParent();
				headerFilePath = new File(headerLayoutParentPath);
			} else if (headerLayout.startsWith("hdfs://")) {

			} else {
				String headerLayoutLocalPath = new File(headerLayout).getAbsolutePath();
				String headerLayoutParentPath = new File(headerLayoutLocalPath).getParent();
				headerFilePath = new File(headerLayoutParentPath);
			}

			if (jsonFilePath.canWrite()) {
				mergedCopybookPath = jsonFilePath.getAbsolutePath();
			} else if (headerFilePath.canWrite()) {
				mergedCopybookPath = headerFilePath.getAbsolutePath();
			} else {
				File mergedCopybookFileDir = new File(mergedCopybookPath);
				if (!(mergedCopybookFileDir.exists()) && !(mergedCopybookFileDir.isDirectory())) {
					mergedCopybookFileDir.mkdirs();
				}
			}
			for (String copyRecordKey : initialCopyRecordsMap.keySet()) {
				MergeCopybookImpl mci = new MergeCopybookImpl();
				List<File> copybookArrayList = new ArrayList<File>();
				String[] copyRecordKeyArray = initialCopyRecordsMap.get(copyRecordKey).split("/");
				String mergedCopyRecordKey = copyRecordKeyArray[copyRecordKeyArray.length - 1];
				String mergeCopyLayout = mergedCopybookPath + "/merged-" + mergedCopyRecordKey;
				copybookArrayList.add(new File(headerLayout));
				copybookArrayList.add(new File(initialCopyRecordsMap.get(copyRecordKey)));

				File[] fileList = copybookArrayList.toArray(new File[copybookArrayList.size()]);

				try {
					mci.joinFiles(new File(mergeCopyLayout), fileList);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					generateHiveTable(mergeCopyLayout, copyRecordKey, mergedCopybookPath, useHivePartition);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		} else {
			String jsonOutputPath = "/tmp/copybook_formatter_" + getTimeStamp();
			if (jsonFilePath.canWrite()) {
				jsonOutputPath = jsonFilePath.getAbsolutePath();
			} else {
				File jsonOutputPathFile = new File(jsonOutputPath);
				if (!(jsonOutputPathFile.exists()) && !(jsonOutputPathFile.isDirectory())) {
					jsonOutputPathFile.mkdirs();
				}
			}

			for (String copyRecordKey : initialCopyRecordsMap.keySet()) {
				try {
					generateHiveTable(initialCopyRecordsMap.get(copyRecordKey), copyRecordKey, jsonOutputPath,
							useHivePartition);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	private static void jobSetup() throws IOException {
		// Configuration actionConf = new Configuration(false);
		if (System.getProperty("oozie.action.conf.xml") != null) {
			conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
		}
		conf.setBoolean("copybook.debug", debug);
		conf.setBoolean("copybook.trace", trace);
		conf.setBoolean("copybook.traceall", traceall);
		conf.setInt("copybook.splitOption", splitOption);
		conf.setInt("copybook.numericType", numericType);
		conf.setInt("copybook.copybookFileType", copybookFileType);

		// propagate delegation related props from launcher job to
		// MR
		// job
		if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
			System.out
					.println("HADOOP_TOKEN_FILE_LOCATION is NOT NULL: " + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		}

		URL cb2xmlUrl = CopybookDriverImpl.class.getClassLoader().getResource("cb2xml.properties");
		String cb2xmlPath = null;
		System.out.println("cb2xmlUrl: " + cb2xmlUrl.getPath());
		if (cb2xmlUrl != null) {
			cb2xmlPath = cb2xmlUrl.getFile();
		}

		// job = new Job(conf, "CopybookDriver-" + jobname);
		fs = FileSystem.get(conf);
		long currentTime = System.currentTimeMillis();
		tempPath = "/tmp/copybook_formatter_" + UserGroupInformation.getCurrentUser().getShortUserName() + "-"
				+ currentTime;
		fsTempPath = new Path(tempPath);
		Path cb2xmlLocalPath = new Path("file://" + cb2xmlPath);
		fs.mkdirs(fsTempPath);
		fs.copyFromLocalFile(false, true, cb2xmlLocalPath, fsTempPath);
	}

	private static void setupSortCopybook() throws IOException {
		String[] sortHeaderLayoutSplit = sortHeaderLayout.split("/");
		int sortHeaderLayoutSplitCount = sortHeaderLayoutSplit.length;
		sortHeaderHdfsLayout = sortHeaderLayoutSplit[sortHeaderLayoutSplitCount - 1];

		String[] sortSplitLayoutSplit = sortSplitLayout.split("/");
		int sortSplitLayoutSplitCount = sortSplitLayoutSplit.length;
		sortSplitHdfsLayout = sortSplitLayoutSplit[sortSplitLayoutSplitCount - 1];

		conf.setBoolean("copybook.sort", useSortHeaderSplit);
		conf.set("copybook.sort.header", "./" + sortHeaderHdfsLayout);
		conf.set("copybook.sort.header.length", sortHeaderLength);
		conf.set("copybook.sort.split", "./" + sortSplitHdfsLayout);
		conf.set("copybook.sort.split.record.length.name", sortSplitLengthName);
		conf.set("copybook.sort.split.record.offset.length", sortSplitOffsetLength);
		conf.set("copybook.sort.split.skip.value", sortSplitSkipValue);
		conf.setBoolean("copybook.sort.skip", useSortRecordSkip);
		conf.setBoolean("copybook.sort.split.record.offset", useSortSplitOffset);
		conf.setBoolean("copybook.sort.skip.entire.record", useSortSkipEntireRecord);
		conf.set("copybook.sort.skip.entire.record.name", sortSkipEntireRecordName);
		conf.set("copybook.sort.skip.entire.record.value", sortSkipEntireRecordValue);

		jobname = appname + "_sort";

		String sortHeaderLayoutPath = new File(sortHeaderLayout).getAbsolutePath();
		Path sortHeaderLocalLayoutPath = new Path("file://" + sortHeaderLayoutPath);
		fs.copyFromLocalFile(false, true, sortHeaderLocalLayoutPath, fsTempPath);
		String sortSplitLayoutPath = new File(sortSplitLayout).getAbsolutePath();
		Path sortSplitLocalLayoutPath = new Path("file://" + sortSplitLayoutPath);
		fs.copyFromLocalFile(false, true, sortSplitLocalLayoutPath, fsTempPath);

		job = Job.getInstance(conf, "CopybookDriver-" + jobname);
		job.setJarByClass(CopybookDriverImpl.class);
		job.addCacheFile(new Path("/apps/copybook_formatter/JRecordV2.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/copybook_formatter/JRecordV2.jar"));
		job.addCacheFile(new Path("/apps/copybook_formatter/json.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/copybook_formatter/json.jar"));
		job.addCacheFile(new Path("hdfs://" + tempPath + "/cb2xml.properties").toUri());
		job.addCacheFile(new Path("hdfs://" + tempPath + "/" + sortHeaderHdfsLayout).toUri());
		job.addCacheFile(new Path("hdfs://" + tempPath + "/" + sortSplitHdfsLayout).toUri());
		job.setInputFormatClass(CopybookByteInputFormat.class);
		job.setOutputFormatClass(CopybookByteOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(CopybookByteCoreMapper.class);
		job.setNumReduceTasks(0);
	}

	private static void setupConvertCopybookJsonLayout() throws IOException {
		GetCopyLayout jsonGcl = new GetCopyLayout(copybookJsonLayout);
		System.out.println("copybookJsonLayout: " + copybookJsonLayout);
		String jsonAbsPath = new File(copybookJsonLayout).getAbsolutePath();
		System.out.println("jsonAbsPath: " + jsonAbsPath);
		String jsonParentPath = new File(jsonAbsPath).getParent();
		System.out.println("jsonParentPath: " + jsonParentPath);
		File jsonFilePath = new File(jsonParentPath);
		System.out.println("jsonFilePath: " + jsonFilePath);

		if (copybookJsonLayout.contains("/")) {
			String[] copybookJsonLayoutArray = copybookJsonLayout.split("/");
			copybookJsonLayout = copybookJsonLayoutArray[copybookJsonLayoutArray.length - 1];
			System.out.println("copybookJsonLayoutTrailer: " + copybookJsonLayout);
		}
		String copybookJsonHdfsLayout = "/tmp/copybook_formatter_" + getTimeStamp() + "/hdfs" + copybookJsonLayout;
		System.out.println("copybookJsonHdfsLayoutBeforeIf: " + copybookJsonHdfsLayout);

		if (jsonFilePath.canWrite()) {
			copybookJsonHdfsLayout = jsonParentPath + "/hdfs-" + copybookJsonLayout;
			System.out.println("copybookJsonHdfsLayoutCanWrite: " + copybookJsonHdfsLayout);
		} else {
			File copybookJsonHdfsFileDir = new File(copybookJsonHdfsLayout);
			if (!(copybookJsonHdfsFileDir.getParentFile().exists())
					&& !(copybookJsonHdfsFileDir.getParentFile().isDirectory())) {
				copybookJsonHdfsFileDir.getParentFile().mkdirs();
			}
		}

		String[] copybookLayoutSplit = copybookJsonHdfsLayout.split("/");
		String copybookHdfsJsonLayout = copybookLayoutSplit[copybookLayoutSplit.length - 1];
		System.out.println("copybookHdfsJsonLayout: " + copybookHdfsJsonLayout);
		conf.set("copybook.json", "./" + copybookHdfsJsonLayout);
		if (useHivePartition) {
			conf.set("copybook.json.partition", hiveOutputPartition);
			conf.setBoolean("copybook.json.usePartition", useHivePartition);
		}
		conf.set("copybook.recordLength", recLength);
		conf.setBoolean("copybook.useRecordLength", useRecLength);

		System.out.println("copybookJsonHdfsLayout: " + copybookJsonHdfsLayout);

		if (useHiveTableName) {
			jobname = appname + "_" + hiveTableName;
		} else {
			jobname = appname;
		}
		job = Job.getInstance(conf, "CopybookDriver-" + jobname);

		// Process files inside of json file and load into hdfs
		String headerLayout = jsonGcl.getHeaderLayout(jsonGcl.getJSONObject());
		if (headerLayout.startsWith("file://") || headerLayout.startsWith("/") || headerLayout.startsWith("./")) {
			String localInFile = headerLayout;
			System.out.println("localInFile: " + localInFile);
			String headerLayoutPath = new File(localInFile.replace("file://", "")).getAbsolutePath();
			System.out.println("headerLayoutPath: " + headerLayoutPath);
			Path headerLocalLayoutPath = new Path("file://" + headerLayoutPath);
			System.out.println("headerLocalLayoutPath: " + headerLocalLayoutPath);
			System.out.println("fsTempPath: " + fsTempPath);

			fs.copyFromLocalFile(false, true, headerLocalLayoutPath, fsTempPath);
			System.out.println("tempPath: " + tempPath);
			String headerHdfsLayout = null;
			if (localInFile.contains("/")) {
				String[] localInFileArray = localInFile.split("/");
				headerHdfsLayout = localInFileArray[localInFileArray.length - 1];
			} else {
				headerHdfsLayout = localInFile;
			}
			System.out.println("headerHdfsLayout: " + headerHdfsLayout);
			job.addCacheFile(new Path("hdfs://" + tempPath + "/" + headerHdfsLayout).toUri());
		}
		if (headerLayout.startsWith("hdfs://")) {
			job.addCacheFile(new Path(headerLayout).toUri());
		}

		Map<String, String> copyLayouts = jsonGcl.getRecord2CopyLayout(jsonGcl.getJSONObject());
		System.out.println("copyLayouts size: " + copyLayouts.size());
		if (copyLayouts != null) {
			for (String copyLayoutRecKey : copyLayouts.keySet()) {
				if (copyLayouts.get(copyLayoutRecKey).startsWith("file://")
						|| copyLayouts.get(copyLayoutRecKey).startsWith("/")
						|| copyLayouts.get(copyLayoutRecKey).startsWith("./")) {
					String localInFile = copyLayouts.get(copyLayoutRecKey);
					System.out.println("localInFile: " + localInFile);
					String copybookJsonLayoutPath = new File(localInFile.replace("file://", "")).getAbsolutePath();
					System.out.println("copybookJsonLayoutPath: " + copybookJsonLayoutPath);
					Path copybookLocalJsonLayoutPath = new Path("file://" + copybookJsonLayoutPath);
					System.out.println("copybookLocalJsonLayoutPath: " + copybookLocalJsonLayoutPath);
					System.out.println("fsTempPath: " + fsTempPath);

					fs.copyFromLocalFile(false, true, copybookLocalJsonLayoutPath, fsTempPath);
					System.out.println("tempPath: " + tempPath);
					if (localInFile.contains("/")) {
						String[] localInFileArray = localInFile.split("/");
						copybookHdfsLayout = localInFileArray[localInFileArray.length - 1];
					} else {
						copybookHdfsLayout = localInFile;
					}
					System.out.println("copybookHdfsLayout: " + copybookHdfsLayout);
					job.addCacheFile(new Path("hdfs://" + tempPath + "/" + copybookHdfsLayout).toUri());
				}
				if (copyLayouts.get(copyLayoutRecKey).startsWith("hdfs://")) {
					job.addCacheFile(new Path(copyLayouts.get(copyLayoutRecKey)).toUri());
				}
			}
		}
		jsonGcl.enableJobJson(jsonGcl.getJSONObject(), copybookJsonHdfsLayout);

		Path copybookLocalJsonLayoutPath = new Path("file://" + copybookJsonHdfsLayout);

		fs.copyFromLocalFile(false, true, copybookLocalJsonLayoutPath, fsTempPath);

		job.setJarByClass(CopybookDriverImpl.class);
		job.addCacheFile(new Path("/apps/copybook_formatter/JRecordV2.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/copybook_formatter/JRecordV2.jar"));
		job.addCacheFile(new Path("/apps/copybook_formatter/json.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/copybook_formatter/json.jar"));
		job.addCacheFile(new Path("hdfs://" + tempPath + "/cb2xml.properties").toUri());

		job.addCacheFile(new Path("hdfs://" + tempPath + "/" + copybookHdfsJsonLayout).toUri());
		job.setInputFormatClass(CopybookMultiInputFormat.class);
		job.setMapperClass(CopybookFormatMultiMapper.class);
		if (useHivePartition) {
			job.setOutputFormatClass(CopybookMultiOutputFormat.class);
		} else {
			job.setOutputFormatClass(TextOutputFormat.class);
		}
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
	}

	private static void generatePartitionDateStructure(String dateFormat) {
		Date now = Calendar.getInstance().getTime();
		SimpleDateFormat yearfrmt = new SimpleDateFormat("yyyy");
		SimpleDateFormat monthfrmt = new SimpleDateFormat("MM");
		SimpleDateFormat dayfrmt = new SimpleDateFormat("dd");

		String year = yearfrmt.format(now);
		String month = monthfrmt.format(now);
		String day = dayfrmt.format(now);
		if (dateFormat.equalsIgnoreCase("currentYear")) {
			hiveOutputPartition = year;
			hivePartition = "LOAD_YEAR=" + year;
		}
		if (dateFormat.equalsIgnoreCase("currentMonth")) {
			hiveOutputPartition = year + "/" + month;
			hivePartition = "LOAD_YEAR=" + year + ",LOAD_MONTH=" + month;
		}
		if (dateFormat.equalsIgnoreCase("currentDate")) {
			hiveOutputPartition = year + "/" + month + "/" + day;
			hivePartition = "LOAD_YEAR=" + year + ",LOAD_MONTH=" + month + ",LOAD_DAY=" + day;

		}
	}

	private static void generateHiveTable(String copybookLayout, String recordName, String hiveOutputPath,
			boolean genHivePartition) throws Exception {
		String hivePartsInfo = null;
		String hivePartsLocation = null;
		String hiveTablePartition = null;

		if (genHivePartition) {
			StringBuffer sbouthive = new StringBuffer();
			StringBuffer sbouthiveloc = new StringBuffer();
			StringBuffer tablePartition = new StringBuffer();
			String[] hivePartitionsSplit = hivePartition.split(",");
			int hivePartsLength = hivePartitionsSplit.length;
			int hivePartsCount = 0;
			for (String hiveparts : hivePartitionsSplit) {
				hivePartsCount++;
				// split hive parts
				String[] hivePartsSplit = hiveparts.split("=");
				String hivePartsDefClean = hivePartsSplit[0] + "='" + hivePartsSplit[1] + "'";
				sbouthive.append(hivePartsDefClean);
				sbouthiveloc.append(hivePartsSplit[1]);
				tablePartition.append(hivePartsSplit[0] + " STRING");
				if (hivePartsLength != hivePartsCount) {
					sbouthive.append(", ");
					sbouthiveloc.append("/");
					tablePartition.append(", ");
				}
			}
			hivePartsInfo = sbouthive.toString();
			hivePartsLocation = sbouthiveloc.toString();
			hiveTablePartition = tablePartition.toString();
		}

		LayoutDetail copyBook = ToLayoutDetail.getInstance()
				.getLayout(copybookInt.loadCopyBook(copybookLayout, splitOption, 0, font, numericType, 0, null));
		copyBook.getRecord(0).getFieldCount();
		StringBuffer sbout = new StringBuffer();
		sbout.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + appname + "_" + recordName + " (");

		boolean firstIn = true;
		int filterCount = 0;

		for (int i = 0; i < copyBook.getRecord(0).getFieldCount(); i++) {
			FieldDetail field = copyBook.getRecord(0).getField(i);
			String outputClean = field.getName().trim().replaceAll(",", "_").replaceAll(" ", "_").replaceAll("[()]", "")
					.replaceAll("-", "_");
			if (firstIn != true) {
				sbout.append(",");
				sbout.append(" ");
			}
			if (outputClean.contains("FILLER")) {
				filterCount = filterCount + 1;
				Integer filterCountStr = filterCount;
				outputClean = outputClean + "_" + filterCountStr.toString();
			}
			sbout.append(outputClean);
			// .replaceAll("[\r\n\t]", " ");
			sbout.append(" ");
			sbout.append("STRING");
			firstIn = false;
		}
		sbout.append(") ");

		String tableOutputPath = outputPath+"/"+recordName;
		if (genHivePartition) {
			sbout.append("PARTITIONED BY (" + hiveTablePartition
					+ ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' lines terminated by '\\n' STORED AS TEXTFILE LOCATION ");
			sbout.append("\'hdfs://" + tableOutputPath.replaceAll(hivePartsLocation, "") + "\';");
			sbout.append("\n");
			sbout.append("ALTER TABLE " + appname + "_" + recordName + " ADD IF NOT EXISTS PARTITION (" + hivePartsInfo
					+ ") LOCATION '" + hivePartsLocation + "';");
			file = new File(hiveOutputPath + "/" + appname + "_" + recordName + ".hive");
		}

		if (!(genHivePartition)) {
			sbout.append(
					"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' lines terminated by '\\n' STORED AS TEXTFILE LOCATION ");
			sbout.append("\'hdfs://" + tableOutputPath + "\';");
		}

		if (!(noGenHive)) {
			file = new File(hiveOutputPath + "/" + appname + "_" + recordName.replaceAll("\\.", "") + ".hive");
			FileWriter writer = new FileWriter(file, false);
			PrintWriter output = new PrintWriter(writer);
			output.print(sbout);
			output.close();
			writer.close();
		}
	}

	
	private static void setOptions() {
		options = new Options();
		options.addOption("sort", false, "Sorts Copy Records from multiple per line to one per line");
		options.addOption("convert", true,
				"--convert (tsv,orc,parquet) requires (--input, --output)/(--gen_hive_only, --output), --copybook, --copybook_filetype");
		options.addOption("gen_hive_only", false, "Hive script generation only");
		options.addOption("no_gen_hive", false, "No Hive Script Generation");
		options.addOption("input", true, "HDFS InputPath");
		options.addOption("output", true, "HDFS OutputPath");
		options.addOption("copybook_json", true,
				"Copybook Json requires -appname, -copybook_split, -copybook_filetype");
		options.addOption("appname", true, "Business Application Name");
		options.addOption("copybook_layout", true, "Copybook FileName / ");
		options.addOption("copybook_split", true, "Copybook Split Option");
		options.addOption("copybook_filetype", true,
				"--copybook_filetype MFVB somtimes requires --include_record, --exclude_record");
		options.addOption("include_record", true, "--include_record, used for MFVB Variable Block files");
		options.addOption("exclude_record", true, "--exclude_record, used for MFVB Variable Block files");
		options.addOption("record_length", true, "--recordtype_length, used for MFVB Variable Block files");
		options.addOption("sort_header_layout", true, "Copybook Sort Header layout FileName");
		options.addOption("sort_header_length", true, "Copybook Sort Header length in bytes");
		options.addOption("sort_split_length_name", true, "Copybook Sort Split Length Record Name");
		options.addOption("sort_split_length_offset", true, "Copybook Sort Split Length Offset Record Name");
		options.addOption("sort_split_skip_value", true, "Copybook Sort Split Skip Value Ebcidic Hex");
		options.addOption("sort_split_layout", true, "Copybook Sort Split layout FileName");
		options.addOption("sort_skip_record_name", true, "Copybook Sort SkipRecord Name");
		options.addOption("sort_skip_record_value", true, "Copybook Sort SkipRecord Value");
		options.addOption("hive_tablename", true, "--tablename, provides hive name of table if no recordtype set");
		options.addOption("hive_partition", true, "Hive Partition Name/Value Pairs");
		options.addOption("no_hive_partition", false, "Generate Hive Script w/ Partitions");
		options.addOption("hive_script_outdir", true, "Hive Script output directory");

		options.addOption("debug", false, "Debug Logging output to Mapreduce");
		options.addOption("trace", false, "Trace Logging output to Mapreduce");
		options.addOption("traceall", false, "TraceAll Logging output to Mapreduce");
		options.addOption("help", false, "Display help");
	}
	
	private static void parseOptions() {
		if (cmd.hasOption("convert")) {
			convert = true;
			convertType = cmd.getOptionValue("convert");
		}
		if (cmd.hasOption("sort")) {
			sort = true;
		}
		if (cmd.hasOption("gen_hive_only")) {
			generateHiveOnly = true;
		}
		if (cmd.hasOption("help")) {
			missingParams();
			System.exit(0);
		}

		if (cmd.hasOption("debug")) {
			debug = true;
		}
		if (cmd.hasOption("trace")) {
			debug = true;
			trace = true;
		}
		if (cmd.hasOption("traceall")) {
			debug = true;
			trace = true;
			traceall = true;
		}

		if (convert && !(sort)) {
			if (cmd.hasOption("output") && cmd.hasOption("appname") && cmd.hasOption("copybook_layout")
					&& cmd.hasOption("copybook_filetype")) {
				if (!(generateHiveOnly)) {
					inputPath = cmd.getOptionValue("input");
				} else {
					if (!(cmd.hasOption("input"))) {
						System.out.println("Missing Options: input");
					}
				}
				outputPath = cmd.getOptionValue("output");
				appname = cmd.getOptionValue("appname");
				if (cmd.hasOption("copybook_layout")) {
					isCopybookLayout = true;
					copybookLayout = cmd.getOptionValue("copybook_layout");
				}
				copybookType = cmd.getOptionValue("copybook_filetype");
				if (cmd.hasOption("copybook_split")) {
					copybookSplitOpt = cmd.getOptionValue("copybook_split");
				}
				if (copybookType.equalsIgnoreCase("MFVB") && convert) {
					if (cmd.hasOption("exclude_record") || cmd.hasOption("include_record")) {
						if (cmd.hasOption("exclude_record")) {
							legacyUseExcludeRecord = true;
							legacyExcludeRecord = cmd.getOptionValue("exclude_record");
						}
						if (cmd.hasOption("include_record")) {
							legacyUseIncludeRecord = true;
							legacyIncludeRecord = cmd.getOptionValue("include_record");
						}
					} else if (cmd.hasOption("record_length") && convert) {
						legacyUseIncludeRecord = false;
						useRecLength = true;
						recLength = cmd.getOptionValue("record_length");
						hiveTableName = cmd.getOptionValue("tablename");
					} else {
						System.out.println(
								"Missing Options: include_record, exclude_record or record_length required for MFVB Types");
						missingParams();
						System.exit(0);
					}
				}
			} else if (cmd.hasOption("output") && cmd.hasOption("appname") && cmd.hasOption("copybook_json")
					&& cmd.hasOption("copybook_filetype")) {
				if (!(generateHiveOnly)) {
					inputPath = cmd.getOptionValue("input");
				} else {
					if (!(cmd.hasOption("input"))) {
						System.out.println("Missing Options: input");
					}
				}
				outputPath = cmd.getOptionValue("output");
				appname = cmd.getOptionValue("appname");
				if (cmd.hasOption("copybook_json")) {
					isCopybookJsonLayout = true;
					copybookJsonLayout = cmd.getOptionValue("copybook_json");
				}
				copybookType = cmd.getOptionValue("copybook_filetype");
				if (cmd.hasOption("copybook_split")) {
					copybookSplitOpt = cmd.getOptionValue("copybook_split");
				}

			} else {
				System.out.println("Missing Options: output, appname, copybook_layout, copybook_filetype");
				missingParams();
				System.exit(0);
			}
			if (copybookType.equalsIgnoreCase("MFVB") && convert) {
				hiveTableName = cmd.getOptionValue("tablename");
			}

			if (cmd.hasOption("hive_tablename") && convert) {
				useHiveTableName = true;
				hiveTableName = cmd.getOptionValue("hive_tablename");
			}
			if (cmd.hasOption("hive_script_outdir") && convert) {
				hivePath = cmd.getOptionValue("hive_script_outdir");
			}
			if (cmd.hasOption("no_hive_partition") && convert) {
				useHivePartition = false;
			}
			if (cmd.hasOption("hive_partition") && convert) {
				hivePartition = cmd.getOptionValue("hive_partition");
				useHivePartition = true;
			}
			if (cmd.hasOption("no_gen_hive") && convert) {
				noGenHive = true;
			}
		} else if (sort && !(convert)) {
			if (cmd.hasOption("input") && cmd.hasOption("output") && cmd.hasOption("appname")
					&& cmd.hasOption("copybook_filetype")) {
				inputPath = cmd.getOptionValue("input");
				outputPath = cmd.getOptionValue("output");
				appname = cmd.getOptionValue("appname");
				if (cmd.hasOption("sort_header_layout") && cmd.hasOption("sort_split_layout")
						&& cmd.hasOption("sort_header_length") && cmd.hasOption("sort_split_length_name")) {
					useSortHeaderSplit = true;
					sortHeaderLayout = cmd.getOptionValue("sort_header_layout");
					sortSplitLayout = cmd.getOptionValue("sort_split_layout");
					sortHeaderLength = cmd.getOptionValue("sort_header_length");
					if (cmd.hasOption("sort_split_length_offset")) {
						sortSplitOffsetLength = cmd.getOptionValue("sort_split_length_offset");
						useSortSplitOffset = true;
					}

					sortSplitLengthName = cmd.getOptionValue("sort_split_length_name");
					if (cmd.hasOption("sort_split_skip_value")) {
						useSortRecordSkip = true;
						sortSplitSkipValue = cmd.getOptionValue("sort_split_skip_value");
					}
					if (cmd.hasOption("sort_skip_record_name") && cmd.hasOption("sort_skip_record_value")) {
						useSortSkipEntireRecord = true;
						sortSkipEntireRecordName = cmd.getOptionValue("sort_skip_record_name");
						sortSkipEntireRecordValue = cmd.getOptionValue("sort_skip_record_value");
					}

				} else {
					System.out.println(
							"Missing Options: sort_header_layout, sort_split_layout, sort_header_length, sort_split_length_name");
					missingParams();
					System.exit(0);
				}
				copybookType = cmd.getOptionValue("copybook_filetype");
				if (cmd.hasOption("copybook_split")) {
					copybookSplitOpt = cmd.getOptionValue("copybook_split");
				}
			} else {
				System.out.println("Missing Options: input, output, appname, copybook_filetype");
				missingParams();
				System.exit(0);
			}
		} else {
			missingParams();
			System.exit(0);
		}
	}
	
	// Start Legacy Code

	private static void legacyMergeCopybookHeaders() {
		isMergeCopyLayout = true;
		// long currentTimeCopy = System.currentTimeMillis();
		// long currentNanoTimeCopy = System.nanoTime();
		String[] copybookLayoutFiles = copybookLayout.split(",");
		File getParentPath = new File(copybookLayoutFiles[0]);
		new File(getParentPath.getParent());
		tempPathCopy = getParentPath.getParent() + "/" + copybookLayoutFiles[copybookLayoutFiles.length - 1] + ".txt";

		List<File> copybookArrayList = new ArrayList<File>();
		for (String s : copybookLayoutFiles) {
			System.out.println(s);
			copybookArrayList.add(new File(s));
		}

		File[] fileList = copybookArrayList.toArray(new File[copybookArrayList.size()]);

		copybookLayout = tempPathCopy;
		try {
			legacyJoinFiles(new File(copybookLayout), fileList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void setupConvertCopybookLayout() throws IOException {
		String[] copybookLayoutSplit = copybookLayout.split("/");
		int copybookLayoutSplitCount = copybookLayoutSplit.length;
		copybookHdfsLayout = copybookLayoutSplit[copybookLayoutSplitCount - 1];
		conf.set("copybook.layout", "./" + copybookHdfsLayout);
		conf.setBoolean("copybook.include.useRecord", legacyUseIncludeRecord);
		conf.setBoolean("copybook.exclude.useRecord", legacyUseExcludeRecord);
		conf.set("copybook.include.records", legacyIncludeRecord);
		conf.set("copybook.exclude.records", legacyExcludeRecord);
		conf.set("copybook.recordLength", recLength);
		conf.setBoolean("copybook.useRecordLength", useRecLength);
		if (useHiveTableName) {
			jobname = appname + "_" + hiveTableName;
		} else {
			jobname = appname + "_"
					+ legacyIncludeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "");
		}
		String copybookLayoutPath = new File(copybookLayout).getAbsolutePath();
		Path copybookLocalLayoutPath = new Path("file://" + copybookLayoutPath);
		fs.copyFromLocalFile(false, true, copybookLocalLayoutPath, fsTempPath);
		job = Job.getInstance(conf, "CopybookDriver-" + jobname);

		job.setJarByClass(CopybookDriverImpl.class);
		job.addCacheFile(new Path("/apps/copybook_formatter/JRecordV2.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/copybook_formatter/JRecordV2.jar"));
		job.addCacheFile(new Path("/apps/copybook_formatter/json.jar").toUri());
		job.addArchiveToClassPath(new Path("/apps/copybook_formatter/json.jar"));
		job.addCacheFile(new Path("hdfs://" + tempPath + "/cb2xml.properties").toUri());

		job.addCacheFile(new Path("hdfs://" + tempPath + "/" + copybookHdfsLayout).toUri());

		job.setInputFormatClass(CopybookInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CopybookFormatMapper.class);
		job.setNumReduceTasks(0);
	}

	private static void generateHiveTable(String copybookLayout) throws Exception {
		String hivePartsInfo = null;
		String hivePartsLocation = null;
		String hiveTablePartition = null;

		if (useHivePartition) {
			StringBuffer sbouthive = new StringBuffer();
			StringBuffer sbouthiveloc = new StringBuffer();
			StringBuffer tablePartition = new StringBuffer();
			String[] hivePartitionsSplit = hivePartition.split(",");
			int hivePartsLength = hivePartitionsSplit.length;
			int hivePartsCount = 0;
			for (String hiveparts : hivePartitionsSplit) {
				hivePartsCount++;
				// split hive parts
				String[] hivePartsSplit = hiveparts.split("=");
				String hivePartsDefClean = hivePartsSplit[0] + "='" + hivePartsSplit[1] + "'";
				sbouthive.append(hivePartsDefClean);
				sbouthiveloc.append(hivePartsSplit[1]);
				tablePartition.append(hivePartsSplit[0] + " STRING");
				if (hivePartsLength != hivePartsCount) {
					sbouthive.append(", ");
					sbouthiveloc.append("/");
					tablePartition.append(", ");
				}
			}
			hivePartsInfo = sbouthive.toString();
			hivePartsLocation = sbouthiveloc.toString();
			hiveTablePartition = tablePartition.toString();
		}

		LayoutDetail copyBook = ToLayoutDetail.getInstance()
				.getLayout(copybookInt.loadCopyBook(copybookLayout, splitOption, 0, font, numericType, 0, null));
		copyBook.getRecord(0).getFieldCount();
		StringBuffer sbout = new StringBuffer();
		if (useHiveTableName) {
			sbout.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + appname + "_" + hiveTableName + " (");
		} else {
			sbout.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + appname + "_"
					+ legacyIncludeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "") + " (");
		}

		boolean firstIn = true;
		int filterCount = 0;

		for (int i = 0; i < copyBook.getRecord(0).getFieldCount(); i++) {
			FieldDetail field = copyBook.getRecord(0).getField(i);
			String outputClean = field.getName().trim().replaceAll(",", "_").replaceAll(" ", "_").replaceAll("[()]", "")
					.replaceAll("-", "_");
			if (firstIn != true) {
				sbout.append(",");
				sbout.append(" ");
			}
			if (outputClean.contains("FILLER")) {
				filterCount = filterCount + 1;
				Integer filterCountStr = filterCount;
				outputClean = outputClean + "_" + filterCountStr.toString();
			}
			sbout.append(outputClean);
			// .replaceAll("[\r\n\t]", " ");
			sbout.append(" ");
			sbout.append("STRING");
			firstIn = false;
		}
		sbout.append(") ");

		if (useHivePartition) {
			sbout.append("PARTITIONED BY (" + hiveTablePartition
					+ ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' lines terminated by '\\n' STORED AS TEXTFILE LOCATION ");
			sbout.append("\'hdfs://" + outputPath.replaceAll(hivePartsLocation, "") + "\';");
			sbout.append("\n");
			if (useHiveTableName) {
				sbout.append("ALTER TABLE " + appname + "_" + hiveTableName + " ADD IF NOT EXISTS PARTITION ("
						+ hivePartsInfo + ") LOCATION '" + hivePartsLocation + "';");
				file = new File(hivePath + "/" + appname + "_" + hiveTableName + ".hive");
			} else {
				sbout.append("ALTER TABLE " + appname + "_"
						+ legacyIncludeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "")
						+ " ADD IF NOT EXISTS PARTITION (" + hivePartsInfo + ") LOCATION '" + hivePartsLocation + "';");
				file = new File(hivePath + "/" + appname + "_"
						+ legacyIncludeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "") + "_"
						+ hivePartsLocation.replaceAll("/", "_") + ".hive");
			}
		}

		if (!(useHivePartition)) {
			sbout.append(
					"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' lines terminated by '\\n' STORED AS TEXTFILE LOCATION ");
			sbout.append("\'hdfs://" + outputPath + "\';");
		}

		if (!(noGenHive)) {
			if (useHiveTableName) {
				file = new File(hivePath + "/" + appname + "_" + hiveTableName.replaceAll("\\.", "") + ".hive");

			} else {
				file = new File(hivePath + "/" + appname + "_"
						+ legacyIncludeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "")
						+ ".hive");
			}
			FileWriter writer = new FileWriter(file, false);
			PrintWriter output = new PrintWriter(writer);
			output.print(sbout);
			output.close();
			writer.close();
		}
	}

	public static void legacyJoinFiles(File destination, File[] sources) throws IOException {
		for (File source : sources) {
			System.out.println("Merging CopyBooks: " + source + " to " + destination);
			legacyAppendFile(destination, source);
		}
	}

	private static void legacyAppendFile(File output, File source) throws IOException {
		RandomAccessFile inRaf = new RandomAccessFile(source.getPath(), "r");
		RandomAccessFile outRaf = new RandomAccessFile(output.getPath(), "rw");
		long outRafSize = outRaf.length();
		outRaf.seek(outRafSize);
		byte[] dataFile = new byte[(int) source.length()];
		inRaf.readFully(dataFile);
		outRaf.write(dataFile);
		outRaf.close();
		inRaf.close();
	}
}
