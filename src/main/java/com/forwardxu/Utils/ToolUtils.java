package com.forwardxu.Utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.DecimalFormat;
import java.util.zip.CRC32;

public class ToolUtils {

    Log log = LogFactory.getLog(this.getClass());

    private static final int SIZE = 1024 * 1024 * 800;

    /**
     * 判断是否存在null或者空字符串
     *
     * @param params
     * @return
     */
    public static boolean isNull(String... params) {
        if (params == null)
            return true;

        for (String param : params) {
            if (param == null || "".equals(param.trim()))
                return true;
        }

        return false;
    }

    /**
     * 判断是否是null对象
     *
     * @param params
     * @return
     */
    public static boolean isNull(Object... params) {
        if (params == null)
            return true;

        for (Object param : params) {
            if (param == null)
                return true;
        }

        return false;
    }

    public static String genFullFileName(String fileDir, String fileName) {

        if (fileDir.endsWith(File.separator)) {
            return fileDir + fileName;
        } else {
            return fileDir + File.separator + fileName;
        }
    }

    /**
     * 获取不带后缀的文件名
     *
     * @param pathandname
     * @return
     */
    public static String getFileName(String pathandname) {
        int end = pathandname.lastIndexOf(".");
        return pathandname.substring(0, end);
    }

    /**
     * Compute the hash for the given key.
     *
     * @return a positive integer hash
     */
    public long hash(final byte[] k) {
        long rv = 0;
        ByteBuffer buf = ByteBuffer.wrap(k);
        int seed = 0x1234ABCD;

        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        rv = seed ^ (buf.remaining() * m);

        long ky;
        while (buf.remaining() >= 8) {
            ky = buf.getLong();

            ky *= m;
            ky ^= ky >>> r;
            ky *= m;

            rv ^= ky;
            rv *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            // for big-endian version, do this first:
            // finish.position(8-buf.remaining());
            finish.put(buf).rewind();
            rv ^= finish.getLong();
            rv *= m;
        }

        rv ^= rv >>> r;
        rv *= m;
        rv ^= rv >>> r;
        buf.order(byteOrder);

        return rv & 0xffffffffL; /* Truncate to 32-bits */
    }

    /**
     * Compute the hash for the given key.
     *
     * @return a positive integer hash
     */
    public long hash(String k) {
        return hash(k.getBytes());
    }

    public final void logMemory() {
        log.info("Max Memory: " + Runtime.getRuntime().maxMemory() / 1048576 + " Mb" +
                "Total Memory: " + Runtime.getRuntime().totalMemory() / 1048576 + " Mb" +
                "Free Memory: " + Runtime.getRuntime().freeMemory() / 1048576 + " Mb");
    }

    public Integer GetFileNums(File file) {
        double nums = 0.0;
        if (file.exists() && file.isFile()) {
            long fileS = file.length();
            nums = Math.ceil((double) fileS / SIZE);
        }
        return (int) nums;
    }

    public String GetFileSize(File file) {
        String size;
        if (file.exists() && file.isFile()) {
            long fileS = file.length();
            DecimalFormat df = new DecimalFormat("#.00");
            if (fileS < 1024) {
                size = df.format((double) fileS) + "BT";
            } else if (fileS < 1048576) {
                size = df.format((double) fileS / 1024) + "KB";
            } else if (fileS < 1073741824) {
                size = df.format((double) fileS / 1048576) + "MB";
            } else {
                size = df.format((double) fileS / 1073741824) + "GB";
            }
        } else if (file.exists() && file.isDirectory()) {
            size = "";
        } else {
            size = "0BT";
        }
        return size;
    }
}
