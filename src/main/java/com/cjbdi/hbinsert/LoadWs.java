package com.cjbdi.hbinsert;

import com.cjbdi.file.FileUtil;
import com.cjbdi.gfs.data.DataBlockFile;
import com.cjbdi.gfs.data.DataStore;
import com.cjbdi.gfs.data.DataStoreManager;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Date;
import java.util.Map;

public class LoadWs{
	private static UnzipUtil unzipper = new UnzipUtil();
	private static Config config = new Config();
	public static void main(String [] args) {
		loadFile(args);
	}
	/**
	 * produce the data and send to queue in MainExtract
	 */	
	public static void loadFile(String [] args) {
		long startTime = System.currentTimeMillis();
		System.out.println("下载开始时间为: " + new Date() + (startTime));
		//文书路径(含有datastores目录的上一路径)
		String rootPath = args[0];
		//保存路径, 含文件名
		String savePathWithTitle = args[1];
		File dataPath = new File(rootPath);
		long total = 0;
		long saveCount = 0;
		//遍历压缩文件
		File[] subPath = unzipper.fileWalker(dataPath, config.start_dir, config.end_dir);
		//dsmPath和datastores索引目录在统一级
		for(File dsmPath:subPath) {
			if(dsmPath != null) {
//				System.out.println("dsmPath: " + dsmPath);
				DataStoreManager dsm = new DataStoreManager(dsmPath, true);
				dsm.open();
				Map<String, DataStore> dsList = dsm.dataStores();
				for (String key : dsList.keySet()) {
					//me
					System.out.println("key: " + key);
					DataStore ds = dsm.getDataStore(key);
					ds.open();
					int fileCount = ds.fileCount();
					System.out.println("路径 " + ds.path() + " 的文件数量为: " + fileCount);
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
									byte[] stream = fileitor.unzippedData();
//									fileitor.unzippedData();
//									FileUtil.saveData(new File(savePathWithTitle + System.currentTimeMillis() + ".doc"), stream);

									FileUtils.writeByteArrayToFile(new File(savePathWithTitle + saveCount + ".doc"),stream);
//									saveCount++;
//									if (saveCount % 10000 == 0) {
//										System.out.println("下载的文件数量为: " + saveCount);
//									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							dbf.close();
						} catch (Exception e) {
//							e.printStackTrace();
						}
					}
					ds.close();
				}
				dsm.close();
			}
		}
		long endTime = System.currentTimeMillis();
		long loadTime = (endTime - startTime) / 1000;
		System.out.println("遍历总时间为: " + loadTime + "秒");
		System.out.println("遍历的文件数量为: " + saveCount);
		System.out.println("平均每秒文件的遍历读取数量为: " + saveCount/loadTime);
	}

}

