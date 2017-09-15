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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public class GetCopyLayout {
	public static JSONObject jsonObject;

	public GetCopyLayout(String fileName) {
		jsonObject = JsonFileReader.readJsonFile(fileName);
	}

	public JSONObject getJSONObject() {
		return jsonObject;
	}

	public String getHeaderLayout(JSONObject jsonObject) {
		return jsonObject.optString("header_layout");
	}

	public String getHivePartitionData(JSONObject jsonObject) {
		return jsonObject.optString("hive_partition_data");
	}

	public String getPartitionDateFormat(JSONObject jsonObject) {
		boolean currentDate = jsonObject.optBoolean("appendCurrentDate");
		boolean currentDay = jsonObject.optBoolean("appendCurrentDay");

		boolean currentMonth = jsonObject.optBoolean("appendCurrentMonth");
		boolean currentYear = jsonObject.optBoolean("appendCurrentMonth");
		String dateFormat = null;
		if (currentYear) {
			dateFormat = "currentYear";
		}
		if (currentMonth) {
			dateFormat = "currentMonth";
		}
		if (currentDate || currentDay) {
			dateFormat = "currentDate";
		}
		if (!(currentDate) && !(currentMonth) && !(currentYear)) {
			dateFormat = "NOTSET";
		}
		return dateFormat;
	}

	public void enableJobJson(JSONObject jsonObject, String jobJsonFile) {
		// Store RecordName and IncludeRecords
		String headerObject = jsonObject.optString("header_layout");

		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int ra = 0; ra < recordArray.length(); ra++) {
			String copyLayout = recordArray.getJSONObject(ra).getString("copy_layout");
			String[] copyLayoutArray = copyLayout.split("/");
			String copyLayoutForHdfs = "./" + copyLayoutArray[copyLayoutArray.length - 1];
			recordArray.getJSONObject(ra).put("copy_layout", copyLayoutForHdfs);
		}
		JSONObject outputJson = new JSONObject();
		if (headerObject != null) {
			if (!(headerObject.isEmpty())) {
				String[] headerObjectArray = headerObject.split("/");
				String headerObjectForHdfs = "./" + headerObjectArray[headerObjectArray.length - 1];
				outputJson.putOpt("header_layout", headerObjectForHdfs);
			}
		}
		outputJson.put("records", recordArray);
		try {
			JsonFileWriter.writeJsonFile(jobJsonFile, outputJson);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getCopyLayout(JSONObject jsonObject, String recordName) {
		String copyLayout = null;
		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int i = 0; i < recordArray.length(); i++) {
			String recName = recordArray.getJSONObject(i).getString("name");
			if (recName.equalsIgnoreCase(recordName)) {
				copyLayout = recordArray.getJSONObject(i).getString("copy_layout");
			}
		}
		return copyLayout;
	}

	public List<String> getRecNames(JSONObject jsonObject) {
		List<String> copyRecArrays = new ArrayList<String>();
		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int i = 0; i < recordArray.length(); i++) {
			String recName = recordArray.getJSONObject(i).getString("name");
			copyRecArrays.add(recName);
		}
		return copyRecArrays;
	}

	public Map<String, String> getRecord2CopyLayout(JSONObject jsonObject) {
		// Store RecordName and IncludeRecords
		Map<String, String> copyLayoutRecMap = new HashMap<String, String>();
		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int ra = 0; ra < recordArray.length(); ra++) {
			String recName = recordArray.getJSONObject(ra).getString("record");
			String copyLayout = recordArray.getJSONObject(ra).getString("copy_layout");
			copyLayoutRecMap.put(recName, copyLayout);
		}
		return copyLayoutRecMap;
	}

	public Map<String, String> getRecord2HiveTable(JSONObject jsonObject) {
		// Store RecordName and IncludeRecords
		Map<String, String> copyLayoutRecMap = new HashMap<String, String>();
		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int ra = 0; ra < recordArray.length(); ra++) {
			String recName = recordArray.getJSONObject(ra).getString("record");
			String hivetable = recordArray.getJSONObject(ra).getString("hive_table");
			copyLayoutRecMap.put(recName, hivetable);
		}
		return copyLayoutRecMap;
	}

	// New Method to Map record to copylayout

	public Map<String, Map<String, Map<List<String>, List<String>>>> getIncludeRecords(JSONObject jsonObject) {
		// Store RecordName and IncludeRecords
		Map<String, Map<String, Map<List<String>, List<String>>>> copyLayoutRecMap = new LinkedHashMap<String, Map<String, Map<List<String>, List<String>>>>();
		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int ra = 0; ra < recordArray.length(); ra++) {
			String recName = recordArray.getJSONObject(ra).getString("record");
			JSONArray includeRecsArray = recordArray.getJSONObject(ra).optJSONArray("include_records");
			if (includeRecsArray == null) {
				return null;
			}
			LinkedHashMap<String, Map<List<String>, List<String>>> includeRecMap = new LinkedHashMap<String, Map<List<String>, List<String>>>();
			for (int ira = 0; ira < includeRecsArray.length(); ira++) {
				LinkedHashMap<List<String>, List<String>> recordMap = new LinkedHashMap<List<String>, List<String>>();
				List<String> recInTypeList = new ArrayList<String>();
				List<String> recInValueList = new ArrayList<String>();

				String incRecName = includeRecsArray.getJSONObject(ira).getString("name");
				JSONObject recNameObj = includeRecsArray.getJSONObject(ira).getJSONObject("entries");
				Iterator<?> keys = recNameObj.keys();
				while (keys.hasNext()) {
					String key = (String) keys.next();
					String value = recNameObj.getString(key);
					recInTypeList.add(key);
					recInValueList.add(value);
				}
				recordMap.put(recInTypeList, recInValueList);
				includeRecMap.put(incRecName, recordMap);
			}
			copyLayoutRecMap.put(recName, includeRecMap);
		}
		return copyLayoutRecMap;
	}

	public Map<String, Map<String, Map<List<String>, List<String>>>> getExcludeRecords(JSONObject jsonObject) {
		// Store RecordName and ExcludeRecords
		Map<String, Map<String, Map<List<String>, List<String>>>> copyLayoutRecMap = new LinkedHashMap<String, Map<String, Map<List<String>, List<String>>>>();
		JSONArray recordArray = jsonObject.getJSONArray("records");
		for (int ra = 0; ra < recordArray.length(); ra++) {
			String recName = recordArray.getJSONObject(ra).getString("record");
			JSONArray excludeRecsArray = recordArray.getJSONObject(ra).optJSONArray("exclude_records");
			if (excludeRecsArray == null) {
				return null;
			}
			LinkedHashMap<String, Map<List<String>, List<String>>> excludeRecMap = new LinkedHashMap<String, Map<List<String>, List<String>>>();
			for (int era = 0; era < excludeRecsArray.length(); era++) {
				LinkedHashMap<List<String>, List<String>> recordMap = new LinkedHashMap<List<String>, List<String>>();
				List<String> recExTypeList = new ArrayList<String>();
				List<String> recExValueList = new ArrayList<String>();

				String excRecName = excludeRecsArray.getJSONObject(era).getString("name");
				JSONObject recNameObj = excludeRecsArray.getJSONObject(era).getJSONObject("entries");
				Iterator<?> keys = recNameObj.keys();
				while (keys.hasNext()) {
					String key = (String) keys.next();
					String value = recNameObj.getString(key);
					recExTypeList.add(key);
					recExValueList.add(value);
				}
				recordMap.put(recExTypeList, recExValueList);
				excludeRecMap.put(excRecName, recordMap);
			}
			copyLayoutRecMap.put(recName, excludeRecMap);
		}
		return copyLayoutRecMap;
	}
}
