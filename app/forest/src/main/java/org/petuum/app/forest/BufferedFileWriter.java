package org.petuum.app.forest;
/**
 * @author yihuaf
 */
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BufferedFileWriter implements AutoCloseable{
	private BufferedWriter writer;

	public BufferedFileWriter(String path, boolean hdfs, boolean append) throws IOException {
            if (hdfs) {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		Path outFile = new Path(path);
		FileSystem fs = outFile.getFileSystem(conf);
		this.writer = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)));
            }
            else {
	         this.writer = new BufferedWriter(new FileWriter(path, append));
            }
	}

	@Override
	public void close() throws Exception {
		writer.close();
	}
	
	public void write(String content) throws IOException {
		writer.write(content);
		writer.flush();
	}
	
	
}
