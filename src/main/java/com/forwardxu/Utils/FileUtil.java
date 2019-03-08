package com.forwardxu.Utils;

import com.forwardxu.Utils.ToolUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class FileUtil {

    Log log = LogFactory.getLog(this.getClass());

    // 定义单个文件的大小这里采用800m
    private static final int SIZE = 1024 * 1024 * 800;

    private FastDateFormat fastDateFormat;

    private Long begin, end, zxsj;

    public static String currentWorkDir = System.getProperty("user.dir") + "\\";

    private ToolUtils toolUtils = new ToolUtils();

    public FileUtil() {
        fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
    }

    /**
     * 功能说明：合并文件
     *
     * @param sourceDir
     * @param targetDir
     * @throws Exception
     */
    public void mergeFile(File sourceDir, File targetDir, String fileName) throws Exception {
        DB db = DBMaker.fileDB(new File(targetDir.getName() + System.currentTimeMillis()))
                .fileMmapEnable()
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .allocateIncrement(1024 * 1024 * 1024)
                .fileMmapCleanerHackEnable()
                .closeOnJvmShutdown()
                .make();

        Date curDate = new Date(System.currentTimeMillis());
        String strCurrTime = fastDateFormat.format(curDate);
        begin = new Date().getTime();
        // 读取properties文件的拆分信息
        File[] files = sourceDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".properties");
            }
        });
        File file = files[0];
        // 获取该文件的信息
        Properties pro = new Properties();
        FileInputStream fis = new FileInputStream(file);
        pro.load(fis);
        //String fileName = pro.getProperty("fileName");
        int splitCount = Integer.valueOf(pro.getProperty("partCount"));
        if (files.length != 1) {
            throw new Exception(sourceDir + ",该目录下没有解析的properties文件或不唯一");
        }

        // 获取该目录下所有的碎片文件
        File[] partFiles = sourceDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".sort");
            }
        });
        // 将碎片文件存入到集合中
        List<FileInputStream> al = new ArrayList<FileInputStream>();
        for (int i = 0; i < splitCount; i++) {
            try {
                al.add(new FileInputStream(partFiles[i]));
            } catch (Exception e) {
                // 异常
                e.printStackTrace();
            }
        }
        try {
            // 构建文件流集合
            Enumeration<FileInputStream> en = Collections.enumeration(al);
            // 将多个流合成序列流
            SequenceInputStream sis = new SequenceInputStream(en);
            File mergeFile = new File(targetDir, fileName + ".merge");
            FileOutputStream fos = new FileOutputStream(mergeFile);
            byte[] b = new byte[SIZE];
            int len = 0;
            while ((len = sis.read(b)) != -1) {
                fos.write(b, 0, len);
            }
            fos.close();
            sis.close();
            fis.close();

            HTreeMap<String, Long> map = db.hashMap("merge_of_map");
            try (LineIterator it = FileUtils.lineIterator(mergeFile, "UTF-8")) {
                long count = 0L;
                while (it.hasNext()) {
                    String[] lines = it.nextLine().split(" ");
                    if (map.get(lines[0]) == null) {
                        count = Long.parseLong(lines[1]);
                    } else {
                        count = map.get(lines[0]) + Long.parseLong(lines[1]);
                    }
                    map.put(lines[0], count);
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            TreeMap<Long, String> top100Map = new TreeMap<Long, String>(new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return (o1 > o2) ? 1 : -1;
                }
            });

            map.forEach((k, v) -> {
                top100Map.put(v, k);
                //只保留前面TopN个元素
                if (top100Map.size() > 100) {
                    top100Map.pollLastEntry();
                }
            });

            try (FileOutputStream fo = new FileOutputStream(new File(targetDir,
                    mergeFile.getName().replace(".merge", "")))) {
                top100Map.forEach((k, v) -> {
                    try {
                        fo.write((v + " " + k).getBytes());
                        fo.write("\n".getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                });
                fo.flush();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            end = new Date().getTime();
            zxsj = (end - begin) / 1000;
            log.info("当前时间为:" + strCurrTime + " 执行时间为: " + zxsj + " s");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 功能说明：拆分文件
     *
     * @param file
     * @param targetPath
     */
    public void splitFile(File file, String targetPath) {
        try {
            logMemory();
            Date curDate = new Date(System.currentTimeMillis());
            String strCurrTime = fastDateFormat.format(curDate);
            begin = new Date().getTime();
            String fileSize = GetFileSize(file);
            Integer splitCount = GetFileNums(file);
            log.info("The file's Size:" + fileSize + " spilt file's num:" + splitCount);

            /**
             * 切割文件时，记录 切割文件的名称和切割的子文件个数以方便合并
             * 这个信息为了简单描述，使用键值对的方式，用到了properties对象
             */
            Properties pro = new Properties();
            // 定义输出的文件夹路径
            File dir = new File(targetPath);
            // 判断文件夹是否存在，不存在则创建
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // 将碎片文件存入到集合中
            List<FileOutputStream> al = new ArrayList<FileOutputStream>();
            List<StringBuffer> alsb = new ArrayList<StringBuffer>();

            for (int i = 0; i < splitCount; i++) {
                try {
                    al.add(new FileOutputStream(new File(dir, i + ".part")));
                    alsb.add(new StringBuffer());
                } catch (Exception e) {
                    // 异常
                    e.printStackTrace();
                }
            }

//            BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
//            // 用5M的缓冲读取文本文件
//            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "utf-8"), 100 * 1024 * 1024);
//            String line;
//            while ((line = reader.readLine()) != null) {
//                Integer target_file_name = (int) (hashUtils.hash(line) % splitCount);
//                alsb.get(target_file_name).append(line);
//                alsb.get(target_file_name).append("\n");
//                Integer len = alsb.get(target_file_name).toString().getBytes().length;
//                if (len % 1024 == 0) {
//                    al.get(target_file_name).write(alsb.get(target_file_name).toString().getBytes());
//                    al.get(target_file_name).flush();
//                    alsb.get(target_file_name).setLength(0);
//                }
//            }
//
//            for (int i = 0; i < al.size(); i++) {
//                if (alsb.get(i).length() > 0) {
//                    al.get(i).write(alsb.get(i).toString().getBytes());
//                    al.get(i).flush();
//                }
//                al.get(i).close();
//            }
//
//            fis.close();

            // 切割文件
            try (LineIterator it = FileUtils.lineIterator(file, "UTF-8")) {
                while (it.hasNext()) {
                    String line = it.nextLine();
                    Integer target_file_name = (int) (toolUtils.hash(line) % splitCount);
                    alsb.get(target_file_name).append(line);
                    alsb.get(target_file_name).append("\n");
                    Integer len = alsb.get(target_file_name).toString().getBytes().length;
                    if (len % 1024 == 0) {
                        al.get(target_file_name).write(alsb.get(target_file_name).toString().getBytes());
                        al.get(target_file_name).flush();
                        alsb.get(target_file_name).setLength(0);
                    }
                }

                for (int i = 0; i < al.size(); i++) {
                    if (alsb.get(i).length() > 0) {
                        al.get(i).write(alsb.get(i).toString().getBytes());
                        al.get(i).flush();
                    }
                    al.get(i).close();
                }

            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            end = new Date().getTime();
            zxsj = (end - begin) / 1000;
            log.info("当前时间为:" + strCurrTime + " 执行时间为: " + zxsj + " s");

            // 将被切割的文件信息保存到properties中
            pro.setProperty("partCount", splitCount + "");
            pro.setProperty("fileName", file.getName());
            pro.setProperty("currTime", strCurrTime);
            pro.setProperty("zxsj", zxsj.toString());
            FileOutputStream fo = new FileOutputStream(new File(dir, (splitCount++) + ".properties"));
            // 写入properties文件
            pro.store(fo, "save file info");
            fo.close();
            logMemory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void readSplitFileToWC(File dir, File targetDir) throws Exception {
        DB db = DBMaker.fileDB(new File(targetDir.getName() + System.currentTimeMillis()))
                .fileMmapEnable()
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .allocateIncrement(1024 * 1024 * 1024)
                .fileMmapCleanerHackEnable()
                .closeOnJvmShutdown()
                .make();
//        Map<String, Long> map = new HashMap<>();

        Date curDate = new Date(System.currentTimeMillis());
        String strCurrTime = fastDateFormat.format(curDate);
        begin = new Date().getTime();
        // 读取properties文件的拆分信息
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".properties");
            }
        });
        File file = files[0];
        // 获取该文件的信息
        Properties pro = new Properties();
        FileInputStream fis = new FileInputStream(file);
        pro.load(fis);
        String fileName = pro.getProperty("fileName");
        int splitCount = Integer.valueOf(pro.getProperty("partCount"));
        if (files.length != 1) {
            throw new Exception(dir + ",该目录下没有解析的properties文件或不唯一");
        }

        // 获取该目录下所有的碎片文件
        File[] partFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".part");
            }
        });

        for (File partFile : partFiles) {
            HTreeMap<String, Long> map = db.hashMap("name_of_map");

            try (LineIterator it = FileUtils.lineIterator(partFile, "UTF-8")) {
                long count = 0L;
                while (it.hasNext()) {
                    String line = it.nextLine();
                    if (map.get(line) == null) {
                        count = 1;
                    } else {
                        count = map.get(line) + 1;
                    }
                    map.put(line, count);
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            TreeMap<Long, String> top100Map = new TreeMap<Long, String>(new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return (o1 > o2) ? 1 : -1;
                }
            });

            map.forEach((k, v) -> {
                top100Map.put(v, k);
                //只保留前面TopN个元素
                if (top100Map.size() > 100) {
                    top100Map.pollLastEntry();
                }
            });

            try (FileOutputStream fo = new FileOutputStream(new File(targetDir,
                    partFile.getName().replace(".part", "") + ".sort"))) {
                top100Map.forEach((k, v) -> {
                    try {
                        fo.write((v + " " + k).getBytes());
                        fo.write("\n".getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                });
                fo.flush();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        end = new Date().getTime();
        zxsj = (end - begin) / 1000;
        log.info("当前时间为:" + strCurrTime + " 执行时间为: " + zxsj + " s");

        // 将被切割的文件信息保存到properties中
        pro.setProperty("partCount", splitCount + "");
        pro.setProperty("fileName", file.getName());
        pro.setProperty("currTime", strCurrTime);
        pro.setProperty("zxsj", zxsj.toString());
        FileOutputStream fo = new FileOutputStream(new File(targetDir, (splitCount++) + ".properties"));
        // 写入properties文件
        pro.store(fo, "save file info");
        fo.close();
        logMemory();
    }

    /**
     * 功能说明：按文件大小均匀拆分文件
     *
     * @param file
     */
    public void splitFileBySize(File file, File targetPath) {
        try {
            FileInputStream fs = new FileInputStream(file);
            // 定义缓冲区
            byte[] b = new byte[SIZE];
            FileOutputStream fo = null;
            int len = 0;
            int count = 0;

            /**
             * 切割文件时，记录 切割文件的名称和切割的子文件个数以方便合并
             * 这个信息为了简单描述，使用键值对的方式，用到了properties对象
             */
            Properties pro = new Properties();
            // 定义输出的文件夹路径
            File dir = targetPath;
            // 判断文件夹是否存在，不存在则创建
            if (!dir.exists()) {
                dir.mkdirs();
            }
            // 切割文件
            while ((len = fs.read(b)) != -1) {
                fo = new FileOutputStream(new File(dir, (count++) + ".part"));
                fo.write(b, 0, len);
                fo.close();
            }
            // 将被切割的文件信息保存到properties中
            pro.setProperty("partCount", count + "");
            pro.setProperty("fileName", file.getName());
            fo = new FileOutputStream(new File(dir, (count++) + ".properties"));
            // 写入properties文件
            pro.store(fo, "save file info");
            fo.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

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

    public Integer GetFileNums(File file) {
        double nums = 0.0;
        if (file.exists() && file.isFile()) {
            long fileS = file.length();
            nums = Math.ceil((double) fileS / SIZE);
        }
        return (int) nums;
    }

    private final void logMemory() {
        log.info("Max Memory: " + Runtime.getRuntime().maxMemory() / 1048576 + " Mb" +
                "Total Memory: " + Runtime.getRuntime().totalMemory() / 1048576 + " Mb" +
                "Free Memory: " + Runtime.getRuntime().freeMemory() / 1048576 + " Mb");
    }
}
