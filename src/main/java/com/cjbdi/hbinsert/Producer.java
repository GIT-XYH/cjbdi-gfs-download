package com.cjbdi.hbinsert;

import com.alibaba.fastjson.JSONObject;
import com.cjbdi.gfs.data.DataBlockFile;
import com.cjbdi.gfs.data.DataStore;
import com.cjbdi.gfs.data.DataStoreManager;
import com.cjbdi.utils.StringUtil;
import java.io.File;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Producer extends Thread{
	private static UnzipUtil unzipper = new UnzipUtil();
	private static Config config = new Config();

	public static void main(String [] args) {
		readFile(args);
	}
	/**
	 * produce the data and send to queue in MainExtract
	 */	
	public static void readFile(String [] args) {
		System.out.println("start produce data");
		String rootPath = args[0];
//		String dataSet = args[1];
		String savePath = args[1];
		System.out.println(rootPath+"--"+savePath);
		File dataPath = new File(rootPath);
		System.out.println("11111");
		long total = 0;
		int a=1;
		int b= 0;
		File[] subPath = unzipper.fileWalker(dataPath, config.start_dir, config.end_dir);
		System.out.println("22222");
		String text = "";
		for(File dsmPath:subPath) {
			//me
			System.out.println("dsmPath: " + dsmPath);
			DataStoreManager dsm = new DataStoreManager(dsmPath, true);
//			if (dsm==null||!dsm.path().contains(dataSet)) continue;
			dsm.open();
			Map<String, DataStore> dsList = dsm.dataStores();
			for(String key:dsList.keySet()) {
				//me
				System.out.println("key: s" + key);
//				if (!key.contains("db_mses")) {
//					continue;
//				}
				DataStore ds = dsm.getDataStore(key);
				ds.open();
				int fileCount = ds.fileCount();
				if (ds.path().endsWith("/5")||ds.path().endsWith("/6")||ds.path().endsWith("/7")||ds.path().endsWith("/8")) {
					System.out.println(ds.path());
					System.out.println(fileCount);
					for (int i = 0; i < fileCount; i++) {
						try {
							DataBlockFile dbf = ds.getDataFile(i);
							DataBlockFile.Iterator fileitor = dbf.iterator();
							while (fileitor.hasNext()) {
								total += 1;
								if (total % 10000 == 0) {
									System.out.println("已扫描的文件数量: " + total);
								}
								try {
									fileitor.next();
									Ws singleWs = unzipper.unzip(fileitor, dsmPath.toString(), key);
									if (singleWs != null && !StringUtil.isEmpty(singleWs.c_wsText)) {
										JSONObject jsonObject = new JSONObject();
										jsonObject.put("c_stm", singleWs.c_stm);
										jsonObject.put("db", singleWs.db);
										jsonObject.put("c_rowkey", singleWs.c_rowkey);
										jsonObject.put("c_wsText", singleWs.c_wsText);
										text += JSONObject.toJSONString(jsonObject) + "\n";
										if (a % 1000 == 0) {
											System.out.println(ds.path());
											System.out.println("符合要求的文件数量: " + a);
										}
										if (a % 10000 == 0) {
											b++;
											String jsonTextName = savePath + "jsonData_" + b;
											CreateFileUtil.createJsonFile(text, jsonTextName);
											text = "";
										}
										a += 1;
									}
								} catch (Exception e) {
								}
							}
							dbf.close();
						} catch (Exception e) {
						}
					}
				}
				ds.close();
			}
			dsm.close();
		}
		if (!StringUtil.isEmpty(text)) {
			b++;
			String jsonTextName = savePath + "jsonData_" + b;
			CreateFileUtil.createJsonFile(text, jsonTextName);
		}
	}

	public static boolean isMatch(String content) {
		if (StringUtil.isEmpty(content)) return false;
		if (content != null) {
			String pattern = "民.{0,10}事.{0,10}(裁|判).{0,10}(定|决).{0,10}书";
			Pattern r = Pattern.compile(pattern);
			Matcher matcher = r.matcher(content);
			if (matcher.find()) {
				return true;
			}
		}
		return false;
	}
}

