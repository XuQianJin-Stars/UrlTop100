package com.forwardxu.filemerge;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

public class FileMerge {

    Log log = LogFactory.getLog(this.getClass());

    private static final int SIZE = 1024 * 1024 * 800;

    private FastDateFormat fastDateFormat;

    private Long begin, end, zxsj;

    public static void main(String[] args) throws Exception {
        if (args != null) {
            if (args.length == 3) {
            } else {
                System.err.println("Example: #sourceDir, #targetDir, #fileName");
                return;
            }
        } else {
            System.err.println("Example: #sourceDir, #targetDir, #fileName");
            return;
        }

        String sourceDir = args[0];
        String targetDir = args[1];
        String fileName = args[2];
        new FileMerge().mergeFile(new File(sourceDir), new File(targetDir), fileName);
    }

    public FileMerge() {
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
}
