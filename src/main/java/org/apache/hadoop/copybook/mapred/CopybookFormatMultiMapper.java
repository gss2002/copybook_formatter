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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class CopybookFormatMultiMapper extends AutoProgressMultiMapper<Text, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> out;

	
	public CopybookFormatMultiMapper() {
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		out = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String outputPath = key.toString()+"/part";
		out.write(NullWritable.get(), value, outputPath);
	}
	
    // 
    // You must override the cleanup method and close the multi-output object
    // or things do not work correctly.
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }
}
