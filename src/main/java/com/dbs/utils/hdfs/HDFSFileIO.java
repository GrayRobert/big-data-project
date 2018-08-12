package com.dbs.utils.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

public class HDFSFileIO {

        // Default Constructor
        public HDFSFileIO() {}

        public void addFile(String source, String dest, Configuration conf) throws IOException {

            FileSystem fileSystem = FileSystem.get(conf);

            // Get the filename out of the file path
            String filename = source.substring(source.lastIndexOf('/') + 1,source.length());

            // Create the destination path including the filename.
            if (dest.charAt(dest.length() - 1) != '/') {
                dest = dest + "/" + filename;
            } else {
                dest = dest + filename;
            }

            // Check if the file already exists
            Path path = new Path(dest);
            if (fileSystem.exists(path)) {
                System.out.println("File " + dest + " already exists");
                return;
            }

            // Create a new file and write data to it.
            FSDataOutputStream out = fileSystem.create(path);
            InputStream in = new BufferedInputStream(new FileInputStream(new File(
                    source)));

            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }

            // Close all the file descriptors
            in.close();
            out.close();
            fileSystem.close();
        }

        /**
         * read a file from hdfs
         * @param file
         * @param conf
         * @throws IOException
         */
        public String readFile(String file, Configuration conf) throws IOException {
            FileSystem fileSystem = FileSystem.get(conf);

            String out = "";

            byte[] b = new byte[1024];

            Path path = new Path(file);
            if (!fileSystem.exists(path)) {
                System.out.println("File " + file + " does not exists");
                return out;
            }

            FSDataInputStream inputStream = fileSystem.open(path);

            out= IOUtils.toString(inputStream, "UTF-8");

            inputStream.close();
            fileSystem.close();

            return out;
        }

        /**
         * delete a directory in hdfs
         * @param file
         * @throws IOException
         */
        public void deleteFile(String file, Configuration conf) throws IOException {
            FileSystem fileSystem = FileSystem.get(conf);

            Path path = new Path(file);
            if (!fileSystem.exists(path)) {
                System.out.println("File " + file + " does not exists");
                return;
            }

            fileSystem.delete(new Path(file), true);

            fileSystem.close();
        }

        /**
         * create directory in hdfs
         * @param dir
         * @throws IOException
         */
        public void mkdir(String dir, Configuration conf) throws IOException {
            FileSystem fileSystem = FileSystem.get(conf);

            Path path = new Path(dir);
            if (fileSystem.exists(path)) {
                System.out.println("Dir " + dir + " already not exists");
                return;
            }

            fileSystem.mkdirs(path);

            fileSystem.close();
        }

}
