package com.cjbdi.hbinsert;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;
import com.cjbdi.file.FileUtil;
import com.cjbdi.gfs.data.DataBlockFile;
import com.cjbdi.gfs.data.DataStore;
import com.cjbdi.gfs.data.DataStoreManager;

class DsmGroup {
	public DataStoreManager dsm = null;
	public Map<String, DataStore> dsList = null;

	public DsmGroup(DataStoreManager dsmName, Map<String, DataStore> dsListName) {
		this.dsm = dsmName;
		this.dsList = dsListName;
	}
}

public class UnzipUtil {
	private static Logger logger = Logger.getLogger(UnzipUtil.class.getName());

	/**
	 * trave the zipped file
	 *
	 * @param dataPath: input path
	 * @param start: a subdir in input path, starting read data from this subdir
	 * @param end: a subdir in input path, ending read data from this subdir
	 * @return dsmPath
	 */
	public File[] fileWalker(File dataPath, int start, int end) {
		File[] levelOneDir = dataPath.listFiles();
		int len_dir = levelOneDir.length;
		int start_dir = start;
		int end_dir = end;
		File[] sub_path = Arrays.copyOfRange(levelOneDir, start_dir, end_dir);
		return sub_path;
	}

	/**
	 * open the DataStoreManager object
	 *
	 * @param dsmPath: the DataStoreManager path
	 * @return a DsmGroup object
	 */
	public DsmGroup openDataStoreManager(File dsmPath) {
		DataStoreManager dsm = new DataStoreManager(dsmPath, true);
		boolean isOpen = dsm.open();
		if (isOpen) {
			Map<String, DataStore> dsList = dsm.dataStores();
			return new DsmGroup(dsm, dsList);
		}
		logger.warning(dsmPath + "DataStoreManager open failed!");
		return null;
	}

	/**
	 * open the DataStore object
	 *
	 * @param key: name of case type
	 * @param dsm: the DataStoreManager object
	 * @return A DataStore object
	 */
	public DataStore openDataStore(String key, DataStoreManager dsm) {
		DataStore ds = dsm.getDataStore(key);
		boolean isOpen = ds.open();
		if (isOpen) {
			return ds;
		}
		logger.warning(ds + "DataStore open failed!");
		return null;
	}

	/**
	 * unzip the DataBlockFile
	 *
	 * @param fileitor: a DataBlockFile.Iterator object
	 * @param dsmPath: the DataStoreManager path
	 * @param dsName: the DataStore name / name of case type
	 * @return A Ws object
	 */
	public void unzip(DataBlockFile.Iterator fileitor, String dsmPath, String dsName) {
		try {
			String name = new String(fileitor.name(), "UTF8");
			System.out.println(name);
			byte[] stream = fileitor.unzippedData();
//			FileUtil.saveData(new File("/tmp/xyh/wsDoc/ws" + new Date().getTime()+".doc"), stream);
//			FileUtil.saveData(new File("/Users/xuyuanhang/Desktop/wsDoc/ws" + new Date().getTime() + ".doc"), stream);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}

