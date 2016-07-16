package org.petuum.app.forest;

/**
 * @author yihuaf
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BufferedFileReader implements AutoCloseable {

    private BufferedReader reader;

    public BufferedFileReader(String path, boolean hdfs) throws IOException {
        if (hdfs) {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName());
            Path inFile = new Path(path);
            FileSystem fs = inFile.getFileSystem(conf); 
            if (!fs.exists(inFile) || !fs.isFile(inFile)) {
                throw new IOException();
            }
            reader = new BufferedReader(new InputStreamReader(fs.open(inFile)));
        }
        else {
            reader = new BufferedReader(new FileReader(path));
        }
    }

    public BufferedReader getReader() {
        return reader;
    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public void close() throws Exception {
        reader.close();
    }

}
