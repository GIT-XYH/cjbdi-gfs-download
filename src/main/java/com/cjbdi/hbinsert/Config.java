package com.cjbdi.hbinsert;

import java.util.Vector;
import java.util.List;

public class Config {
	//压缩文件的本地路径
	public int start_dir = 0;	//slice the data dir   0-11 11-22 22-33  //区分33个法院的文书压缩文件
	public int end_dir = 33;		//slice the data dir
}