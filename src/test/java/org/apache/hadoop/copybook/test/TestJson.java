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

package org.apache.hadoop.copybook.test;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.copybook.mapred.utils.GetCopyLayout;

public class TestJson {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		StringBuffer includeRecordValue = new StringBuffer();
		StringBuffer includeRecordType = new StringBuffer();
		StringBuffer excludeRecordType = new StringBuffer();
		StringBuffer excludeRecordValue = new StringBuffer();
		String includeValueIn = "";
		String excludeValueIn = "";
		System.out.println("GSS");
		GetCopyLayout gcl = new GetCopyLayout("src/test/resources/test.json");
		Map<String, Map<String, Map<List<String>, List<String>>>> includeMap = gcl
				.getIncludeRecords(gcl.getJSONObject());
		Map<String, Map<String, Map<List<String>, List<String>>>> excludeMap = gcl
				.getExcludeRecords(gcl.getJSONObject());
		Map<String, String> rec2copylayout = gcl.getRecord2CopyLayout(gcl.getJSONObject());
		includeLoop: for (String includeMapKey : includeMap.keySet()) {
			System.out.println("IncludeMap:  " + includeMapKey);
			String includeMapCopylayout = rec2copylayout.get(includeMapKey);
			Map<String, Map<List<String>, List<String>>> includeRecordMap = includeMap.get(includeMapKey);
			for (String includeMapRecordKey : includeRecordMap.keySet()) {
				Map<List<String>, List<String>> includeMapList = includeRecordMap.get(includeMapRecordKey);
				includeRecordType.setLength(0);
				includeRecordValue.setLength(0);
				for (List<String> includeMapListKey : includeMapList.keySet()) {
					System.out.println("IncludeMapListKey: " + includeMapListKey);
					List<String> includeTypeList = includeMapListKey;
					List<String> includeValueList = includeMapList.get(includeMapListKey);
					for (int i = 0; i < includeTypeList.size(); i++) {
						String blah = "";
						System.out.println("IncludeType: " + includeTypeList.get(i));
						// includeRecordValue
						// .append(copyRecord.getFieldValue(includeTypeList.get(i)).asString().trim());
					}
					includeValueIn = String.join("", includeValueList);
					System.out.println("CopyLayout=" + includeMapCopylayout + "  includeValueIn=" + includeValueIn);
					if (includeRecordValue.toString().equalsIgnoreCase(includeValueIn)) {
						String includeTypeIn = String.join("", includeTypeList);
						System.out.println("RecordType=" + includeTypeIn + "::RecordValue=" + includeValueIn);
						// recTypeValue = true;

						// LOG.debug("Including Line::" + lineNum+ " -
						// RecordType="+includeTypeIn+"::RecordValue=" +
						// includeValueIn); recTypeValue = true;
						break includeLoop;
					}
				}
			}
		}

	}

}
