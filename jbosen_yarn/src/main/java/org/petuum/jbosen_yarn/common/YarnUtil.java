package org.petuum.jbosen_yarn.common;

/**
 * @author yihuaf
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnUtil {
    private final static Logger LOG = LoggerFactory.getLogger(YarnUtil.class);

    public static String copyToHDFS(FileSystem fs, String fileSrcPath,
            String HDFSPathPrefix, Map<String, LocalResource> localResources,
            String resources) throws IOException {

        String srcFileName = new Path(fileSrcPath).getName();
        Path dst = null;
        if (!HDFSPathPrefix.equals("")) {
            dst = new Path(HDFSPathPrefix, srcFileName);
        } else {
            dst = new Path(fs.getHomeDirectory(), srcFileName);
        }

        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission(
                        (short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(false, true, new Path(fileSrcPath), dst);
        }
        return dst.toUri().toString();
    }

    public static void addToLocalResources(FileSystem fs, String fileHDFSPath,
            String fileIdentifier, Map<String, LocalResource> localResources,
            String resources) throws IOException {
        Path dst = new Path(fileHDFSPath);
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileIdentifier, scRsrc);
    }

    public static void copyAndAddToLocalResources(FileSystem fs,
            String fileSrcPath, String fileIdentifier, String HDFSPathPrefix,
            Map<String, LocalResource> localResources, String resources)
            throws IOException {
        String fileHDFSPath = copyToHDFS(fs, fileSrcPath, HDFSPathPrefix,
                localResources, resources);
        addToLocalResources(fs, fileHDFSPath, fileIdentifier, localResources,
                resources);
    }

    public static void writeTxtFile(String path, String content) {
        try {
            File file = new File(path);
            if (!file.exists())
                file.createNewFile();
            BufferedWriter bw1 = new BufferedWriter(new FileWriter(path, false));
            bw1.write(content);
            bw1.flush();
            bw1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeFileHDFS(FileSystem fs, String HDFSDst, String content)
            throws IOException {
        Path dst = new Path(HDFSDst);
        if (fs.exists(dst)) {
            LOG.warn("File on HDFS already exist, will overwrite");
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                fs.create(dst)));
        writer.write(content);
        writer.close();
    }

    public static void execShell(String shell) {
        try {
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec(shell);
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
