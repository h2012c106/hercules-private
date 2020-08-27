package com.xiaohongshu.db.node.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CompressUtils {

    private static void zip(ZipOutputStream out, File f, String base) throws IOException {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            base = StringUtils.isEmpty(base) ? "" : base + "/";
            for (File file : files == null ? new File[0] : files) {
                zip(out, file, base + file.getName());
            }
        } else {
            BufferedInputStream in = null;
            try {
                out.putNextEntry(new ZipEntry(base));
                in = new BufferedInputStream(new FileInputStream(f));
                IOUtils.copy(in, out);
            } finally {
                IOUtils.closeQuietly(in);
            }
        }
    }

    public static File getFile(String inputFileName) throws IOException {
        File file = new File(inputFileName);
        if (!file.exists()) {
            throw new IOException(String.format("The log file [%s] does not exist.", inputFileName));
        } else {
            return file;
        }
    }

    public static void zip(String inputFileName, OutputStream outputStream) throws IOException {
        ZipOutputStream out = null;
        try {
            File file = getFile(inputFileName);
            out = new ZipOutputStream(outputStream);
            zip(out, file, null);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }


}
