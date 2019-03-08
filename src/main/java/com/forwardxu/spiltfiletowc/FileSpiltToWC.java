package com.forwardxu.spiltfiletowc;

import com.forwardxu.Utils.ToolUtils;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

public class FileSpiltToWC {
    Log log = LogFactory.getLog(this.getClass());

    private static final int SIZE = 1024 * 1024 * 800;

    private FastDateFormat fastDateFormat;

    private Long begin, end, zxsj;

    private ToolUtils toolUtils = new ToolUtils();

    public static void main(String[] args) throws Exception {
        if (args != null) {
            if (args.length == 2) {
            } else {
                System.err.println("Example: #dir, #targetDir");
                return;
            }
        } else {
            System.err.println("Example: #dir, #targetDir");
            return;
        }

        String dir = args[0];
        String targetDir = args[1];

        new FileSpiltToWC().readSplitFileToWC(new File(dir), new File(targetDir));
    }

    public FileSpiltToWC() {
        fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
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
            throw new Exception(dir + ",there are no parsed properties files or are not unique");
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
             * when cutting files, record the name of the cutting file and the number of cutting subfiles for easy combination
             * this information is described in the properties object using key-value pairs for simplicity
             */
            Properties pro = new Properties();
            // define the output folder path
            File dir = targetPath;
            // to determine if the folder exists or not, create it
            if (!dir.exists()) {
                dir.mkdirs();
            }
            // 切割文件
            while ((len = fs.read(b)) != -1) {
                fo = new FileOutputStream(new File(dir, (count++) + ".part"));
                fo.write(b, 0, len);
                fo.close();
            }
           // save the file information to properties
            pro.setProperty("partCount", count + "");
            pro.setProperty("fileName", file.getName());
            fo = new FileOutputStream(new File(dir, (count++) + ".properties"));
          // write to the properties file
            pro.store(fo, "save file info");
            fo.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * function description: split file
     *
     * @param file
     * @param targetPath
     */
    public void splitFile(File file, String targetPath) {
        try {
            toolUtils.logMemory();
            Date curDate = new Date(System.currentTimeMillis());
            String strCurrTime = fastDateFormat.format(curDate);
            begin = new Date().getTime();
            String fileSize = toolUtils.GetFileSize(file);
            Integer splitCount = toolUtils.GetFileNums(file);
            log.info("The file's Size:" + fileSize + " spilt file's num:" + splitCount);

            /**
             * when cutting files, record the name of the cutting file and the number of cutting subfiles for easy combination
             * this information is described in the properties object using key-value pairs for simplicity
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
            toolUtils.logMemory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
