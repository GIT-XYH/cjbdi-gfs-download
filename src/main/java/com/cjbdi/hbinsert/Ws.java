package com.cjbdi.hbinsert;

import java.util.Hashtable;
import java.util.Map;

public class Ws {
	public String db = "";
	public String schema = "";
	public String c_rowkey = "";
	public String c_stm = "";
	public String c_wsText = "";
	public String c_ajbs = "";
	public String c_mc = "";
	public String c_nr = "";

	public Ws(String db, String schema, String c_rowkey, String c_stm, String c_wsText, String c_ajbs, String c_mc, String c_nr) {
		this.db = db;
		this.schema = schema;
		this.c_rowkey = c_rowkey;
		this.c_stm = c_stm;
		this.c_wsText = c_wsText;
		this.c_ajbs = c_ajbs;
		this.c_mc = c_mc;
		this.c_nr = c_nr;
	}
	
	public Map<String, String> toHBaseInsertHashtable() {
		Map<String, String> inLayer = new Hashtable<String, String>();
		inLayer.put("db", this.db);
		inLayer.put("schema", this.schema);
		inLayer.put("c_stm", this.c_stm);
		inLayer.put("c_ajbs", this.c_ajbs);
		inLayer.put("c_mc", this.c_mc);
		inLayer.put("c_nr", this.c_nr);
		inLayer.put("c_wsText", this.c_wsText);
		return inLayer;
	}
}
