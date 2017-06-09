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
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class CopybookDriver {
	private static final Log LOG = LogFactory.getLog(CopybookDriver.class.getName());
	static Options options = new Options();

	/**
	 * @param args
	 * 
	 * 
	 */

	private static CopybookLoader copybookInt = new CobolCopybookLoader();

	private static void addCopyBooks(String file) throws Exception {

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

	public static void main(String[] args) throws Exception {
		String font = null;
		int numericType = 0;
		int splitOption = 0;
		int copybookFileType = 0;
		String inputPath = "";
		String outputPath = "";
		String appname = "";
		String includeRecord = "";
		String excludeRecord = "";

		String recLength = "";
		String copybookLayout = "";
		String copybookHdfsLayout = null;
		String copybookType = "";
		String copybookSplitOpt = "NOSPLIT";
		String hivePath = "./";
		boolean hivePartition = false;
		boolean includeUseRecord = true;
		boolean excludeUseRecord = false;
		boolean useRecLength = false;
		boolean debug = false;
		boolean trace = false;
		boolean traceall = false;
		String hivePartsInfo = null;
		String hivePartsLocation = null;
		String hiveTablePartition = null;
		String hivePartitionsIn = null;
		String hiveTableName = null;
		boolean generateHiveOnly = false;
		boolean convert = false;
		String convertType = "";
		
		boolean noGenHive = false;
		File file = null;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		options = new Options();
		options.addOption("convert", true,
				"--convert (tsv,orc,parquet) requires (--input, --output)/(--gen_hive_only, --output), --copybook, --copybook_filetype");
		options.addOption("gen_hive_only", false, "Hive script generation only");
		options.addOption("no_gen_hive", false, "No Hive Script Generation");
		options.addOption("input", true, "HDFS InputPath");
		options.addOption("output", true, "HDFS OutputPath");
		options.addOption("appname", true, "Business Application Name");
		options.addOption("copybook_layout", true, "Copybook FileName");
		options.addOption("copybook_split", true, "Copybook Split Option");
		options.addOption("copybook_filetype", true,
				"--copybook_filetype MFVB somtimes requires --include_record, --exclude_record");
		options.addOption("include_record", true, "--include_record, used for MFVB Variable Block files");
		options.addOption("exclude_record", true, "--exclude_record, used for MFVB Variable Block files");
		options.addOption("record_length", true, "--recordtype_length, used for MFVB Variable Block files");
		options.addOption("hive_tablename", true, "--tablename, provides hive name of table if no recordtype set");
		options.addOption("hive_partition", true, "Hive Partition Name/Value Pairs");
		options.addOption("no_hive_partition", false, "Generate Hive Script w/ Partitions");
		options.addOption("hive_script_outdir", true, "Hive Script output directory");
		options.addOption("debug", false, "Debug Logging output to Mapreduce");
		options.addOption("trace", false, "Trace Logging output to Mapreduce");
		options.addOption("traceall", false, "TraceAll Logging output to Mapreduce");
		options.addOption("help", false, "Display help");
		CommandLineParser parser = new CopybookParser();
		CommandLine cmd = parser.parse(options, otherArgs);

		if (cmd.hasOption("convert")) {
			convert = true;
			convertType = cmd.getOptionValue("convert");
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
		
		if(convert) {
			if (cmd.hasOption("output") && cmd.hasOption("appname")
					&& cmd.hasOption("copybook_layout") && cmd.hasOption("copybook_filetype")) {
				if (!(generateHiveOnly)) {
					inputPath = cmd.getOptionValue("input");
				} else {
					if (!(cmd.hasOption("input"))) {
						System.out.println("Missing Options: input");
					}
				}
				outputPath = cmd.getOptionValue("output");
				appname = cmd.getOptionValue("appname");
				copybookLayout = cmd.getOptionValue("copybook_layout");
				copybookType = cmd.getOptionValue("copybook_filetype");
				if (cmd.hasOption("copybook_split")) {
					copybookSplitOpt = cmd.getOptionValue("copybook_split");
				}
			} else {
				System.out.println("Missing Options: output, appname, copybook_layout, copybook_filetype");
				missingParams();
				System.exit(0);
			}
			if (copybookType.equalsIgnoreCase("MFVB")) {
				if (cmd.hasOption("exclude_record") || cmd.hasOption("include_record")) {
					if (cmd.hasOption("exclude_record")) {
						excludeUseRecord = true;
						excludeRecord = cmd.getOptionValue("exclude_record");
					}
					if (cmd.hasOption("include_record")) {
						includeUseRecord = true;
						includeRecord = cmd.getOptionValue("include_record");
					}
				} else if (cmd.hasOption("record_length")) {
					includeUseRecord = false;
					useRecLength = true;
					recLength = cmd.getOptionValue("record_length");
					hiveTableName = cmd.getOptionValue("tablename");
				} else {
					System.out.println("Missing Options: include_record, exclude_record or record_length required for MFVB Types");
					missingParams();
					System.exit(0);				
				}
			}
			
			if (cmd.hasOption("hive_tablename")) {
				hiveTableName = cmd.getOptionValue("hive_tablename");
			}
			if (cmd.hasOption("hive_script_outdir")) {
				hivePath = cmd.getOptionValue("hive_script_outdir");
			}
			if (cmd.hasOption("no_hive_partition")) {
				hivePartition = false;
			} 
			if (cmd.hasOption("hive_partition")) {
				hivePartitionsIn = cmd.getOptionValue("hive_partition");
				hivePartition = true;
			}
			if (cmd.hasOption("no_gen_hive")) {
				noGenHive = true;
			}
		} else {
			missingParams();
			System.exit(0);	
		}
	
		if (hivePartition) {
			StringBuffer sbouthive = new StringBuffer();
			StringBuffer sbouthiveloc = new StringBuffer();
			StringBuffer tablePartition = new StringBuffer();
			String[] hivePartitionsSplit = hivePartitionsIn.split(",");
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

		System.out.println("Input:" + inputPath + ", Output: " + outputPath + ", AppName: " + appname
				+ ", CopyBookLayOutFile: " + copybookLayout + ", copybookSplitOpt: " + copybookSplitOpt
				+ ", copybookType: " + copybookType + ", useIncludeRecord: " + includeUseRecord + ", IncludeRecord: "
				+ includeRecord + ", useExcludeRecord: " + excludeUseRecord + ", ExcludeRecord: " + excludeRecord
				+ ", hivePath: " + hivePath + ", hiveTableName: " + hiveTableName + ", GenerateHiveOnly:(false) "
				+ generateHiveOnly + ", hivePartitionsIn: " + hivePartitionsIn + ", hivePartition:(false) "
				+ hivePartition);

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

		String[] copybookLayoutSplit = copybookLayout.split("/");
		int copybookLayoutSplitCount = copybookLayoutSplit.length;
		copybookHdfsLayout = copybookLayoutSplit[copybookLayoutSplitCount - 1];

		addCopyBooks(copybookLayout);

		FileSystem fs = null;
		Path fsTempPath = null;
		try {
			LayoutDetail copyBook = ToLayoutDetail.getInstance()
					.getLayout(copybookInt.loadCopyBook(copybookLayout, splitOption, 0, font, numericType, 0, null));
			copyBook.getRecord(0).getFieldCount();
			StringBuffer sbout = new StringBuffer();
			if (includeUseRecord) {
				sbout.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + appname + "_"
						+ includeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "") + " (");
			} else {
				sbout.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + appname + "_" + hiveTableName + " (");
			}

			boolean firstIn = true;
			int filterCount = 0;

			for (int i = 0; i < copyBook.getRecord(0).getFieldCount(); i++) {
				FieldDetail field = copyBook.getRecord(0).getField(i);
				String outputClean = field.getName().trim().replaceAll(",", "_").replaceAll(" ", "_")
						.replaceAll("[()]", "").replaceAll("-", "_");
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

			if (hivePartition) {
				sbout.append("PARTITIONED BY (" + hiveTablePartition
						+ ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' lines terminated by '\\n' STORED AS TEXTFILE LOCATION ");
				sbout.append("\'hdfs://" + outputPath.replaceAll(hivePartsLocation, "") + "\';");
				sbout.append("\n");
				if (includeUseRecord) {
					sbout.append("ALTER TABLE " + appname + "_"
							+ includeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "")
							+ " ADD IF NOT EXISTS PARTITION (" + hivePartsInfo + ") LOCATION '" + hivePartsLocation
							+ "';");
					file = new File(hivePath + "/" + appname + "_"
							+ includeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "") + "_"
							+ hivePartsLocation.replaceAll("/", "_") + ".hive");
				} else {
					sbout.append("ALTER TABLE " + appname + "_" + hiveTableName + " ADD IF NOT EXISTS PARTITION ("
							+ hivePartsInfo + ") LOCATION '" + hivePartsLocation + "';");
					file = new File(hivePath + "/" + appname + "_" + hiveTableName + ".hive");
				}
			}

			if (!(hivePartition)) {
				sbout.append(
						"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' lines terminated by '\\n' STORED AS TEXTFILE LOCATION ");
				sbout.append("\'hdfs://" + outputPath + "\';");
			}

			if (!(noGenHive)) {
				if (includeUseRecord) {
					file = new File(hivePath + "/" + appname + "_"
							+ includeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "")
							+ ".hive");
				} else {
					file = new File(hivePath + "/" + appname + "_" + hiveTableName.replaceAll("\\.", "") + ".hive");
				}
				FileWriter writer = new FileWriter(file, false);
				PrintWriter output = new PrintWriter(writer);
				output.print(sbout);
				output.close();
				writer.close();
			}

			if (!(generateHiveOnly)) {
				// Configuration actionConf = new Configuration(false);
				if (System.getProperty("oozie.action.conf.xml") != null) {
					conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
				}
				conf.set("copybook.layout", "./" + copybookHdfsLayout);
				conf.setInt("copybook.numericType", numericType);
				conf.setBoolean("copybook.include.useRecord", includeUseRecord);
				conf.setBoolean("copybook.exclude.useRecord", excludeUseRecord);
				conf.set("copybook.include.records", includeRecord);
				conf.set("copybook.exclude.records", excludeRecord);
				conf.set("copybook.recordLength", recLength);
				conf.setBoolean("copybook.useRecordLength", useRecLength);
				conf.setInt("copybook.splitOption", splitOption);
				conf.setBoolean("copybook.debug", debug);
				conf.setBoolean("copybook.trace", trace);
				conf.setBoolean("copybook.traceall", traceall);

				conf.setInt("copybook.copybookFileType", copybookFileType);
				// propagate delegation related props from launcher job to MR
				// job
				if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
					System.out.println(
							"HADOOP_TOKEN_FILE_LOCATION is NOT NULL: " + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
					conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
				}

				URL cb2xmlUrl = CopybookDriver.class.getClassLoader().getResource("cb2xml.properties");
				String cb2xmlPath = null;
				System.out.println("cb2xmlUrl: " + cb2xmlUrl.getPath());
				if (cb2xmlUrl != null) {
					cb2xmlPath = cb2xmlUrl.getFile();
				}

				fs = FileSystem.get(conf);
				long currentTime = System.currentTimeMillis();
				String tempPath = "/tmp/" + UserGroupInformation.getCurrentUser().getShortUserName() + "-"
						+ currentTime;
				fsTempPath = new Path(tempPath);
				String copybookLayoutPath = new File(copybookLayout).getAbsolutePath();

				Path copybookLocalLayoutPath = new Path("file://" + copybookLayoutPath);
				Path cb2xmlLocalPath = new Path("file://" + cb2xmlPath);
				fs.mkdirs(fsTempPath);
				fs.copyFromLocalFile(false, true, copybookLocalLayoutPath, fsTempPath);
				fs.copyFromLocalFile(false, true, cb2xmlLocalPath, fsTempPath);

				String jobname = null;
				if (includeUseRecord) {
					jobname = appname + "_"
							+ includeRecord.replace(".", "").replace(",", "").replace(":", "").replace("=", "");
				} else {
					jobname = appname + "_" + hiveTableName;
				}

				@SuppressWarnings("deprecation")
				Job job = new Job(conf, "CopybookDriver-" + jobname);
				job.addCacheFile(new Path("/apps/copybook_formatter/JRecordV2.jar").toUri());
				job.addArchiveToClassPath(new Path("/apps/copybook_formatter/JRecordV2.jar"));
				job.addCacheFile(new Path("hdfs://" + tempPath + "/cb2xml.properties").toUri());
				job.addCacheFile(new Path("hdfs://" + tempPath + "/" + copybookHdfsLayout).toUri());
				job.setJarByClass(CopybookDriver.class);
				job.setInputFormatClass(CopybookInputFormat.class);
				FileInputFormat.addInputPaths(job, inputPath);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(Text.class);
				job.setMapperClass(CopybookFormatMapper.class);
				job.setNumReduceTasks(0);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				FileOutputFormat.setOutputPath(job, new Path(outputPath));
				job.waitForCompletion(true);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fs != null || fsTempPath != null) {
				fs.deleteOnExit(fsTempPath);
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
}
