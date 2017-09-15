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
package org.apache.hadoop.copybook.mapred.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;

public class MergeCopybookLayoutTest {

	static long currentTimeCopy;
	static long currentNanoTimeCopy;
	static String procTime;

	private static void setTimeStamp() {
		currentTimeCopy = System.currentTimeMillis();
		currentNanoTimeCopy = System.nanoTime();
		procTime = currentTimeCopy + "_" + currentNanoTimeCopy;
	}

	private static String getTimeStamp() {
		return procTime;
	}

	public static void main(String[] args) {
		List<File> fileArrayList = new ArrayList<File>();
		for (String s : args) {
			System.out.println(s);
			fileArrayList.add(new File(s));
		}
		// TODO Auto-generated method stub
		File[] fileList = fileArrayList.toArray(new File[fileArrayList.size()]);

		String filename = "/tmp/copybook_formatter/job_id_test";
		File fileDir = new File(filename);
		if (!(fileDir.getParentFile().exists()) && !(fileDir.getParentFile().isDirectory())) {
			fileDir.getParentFile().mkdirs();
		}
		try {
			joinFiles(new File(filename), fileList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void joinFiles(File destination, File[] sources) throws IOException {
		for (File source : sources) {
			System.out.println("Merging: " + source);
			appendFile(destination, source);
		}
	}

	private static void appendFile(File output, File source) throws IOException {
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
