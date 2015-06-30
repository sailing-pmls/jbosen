package org.petuum.app.matrixfact;

/**
 * @author yihuaf
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BufferedFileReader implements AutoCloseable {

    private BufferedReader reader;

    public BufferedFileReader(String path) throws IOException {
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

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public void close() throws Exception {
        reader.close();
    }

}
