package com.cjbdi.hbinsert;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import com.cjbdi.doc.detector.DocDetector;
import com.cjbdi.doc.detector.DocType;
import com.cjbdi.fp.docs.Doc2Text;
import com.cjbdi.gfs.data.DataBlockFile;
import com.cjbdi.gfs.data.DataStore;
import com.cjbdi.gfs.data.DataStoreManager;
import com.cjbdi.utils.CharsetUtil;

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
	public Ws unzip(DataBlockFile.Iterator fileitor, String dsmPath, String dsName) {
		String name = "";
		String c_stm = "";
		String c_ajbs = "";
		String c_mc = "";
		String c_nr = "";
		String c_rowkey = "";
		String c_wsText = "";
		String db = "";
		String schema = "";
		try {
			name = new String(fileitor.name(), "UTF8");
			byte[] stream = fileitor.unzippedData();
			String[] name_split = name.split("\\:");
			c_stm = name_split[0].trim();
			c_ajbs = name_split[1].trim();
			c_mc = name_split[3].trim();
			c_nr = name_split[4].trim();
			c_rowkey = c_stm + ((c_stm+c_mc+c_nr).hashCode()+"000000").replace("-", "").substring(0,6); //rowkey生成规则
			String[] path_split = dsmPath.split("\\/");
			db = path_split[path_split.length - 1].trim();
			schema = dsName.split("\\/")[0].trim();
			if (c_stm.isEmpty()) {
				c_stm = name_split[1].trim(); // 此文书为"09"法标的文书
			}
			DocType type = DocDetector.detect(stream);
			if (type == null) {
				if (CharsetUtil.isUTF8(stream)) {
					c_wsText = new String(stream, "UTF8");
				} else {
					c_wsText = new String(stream, "GBK");
				}
			} else {
				try {
					Doc2Text dt = new Doc2Text();
					if (!dt.handleData(stream)) {

					}
					c_wsText = dt.getPlainText();
				}catch (Exception e) {
				}
			}
			if (c_wsText == null || c_wsText.isEmpty()) {
				c_wsText = "";
			}
		} catch (Exception e) {
		}
		Ws wsObject = new Ws(db, schema, c_rowkey, c_stm, c_wsText, c_ajbs, c_mc, c_nr);
		return wsObject;
	}
}

