package com.forwardxu;

import com.forwardxu.makedata.GenBigFile;
import com.forwardxu.filemerge.FileMerge;
import com.forwardxu.filespilt.AppMain;
import com.forwardxu.spiltfiletowc.FileSpiltToWC;
import org.junit.Test;

import java.io.File;

public class AppMainFileTest {
    @Test
    public void testMakeData() {
        GenBigFile genBigFile = new GenBigFile();
        genBigFile.main(new String[]{"D:\\soucecode\\data\\data6666.txt", "10", "600"});
    }

    @Test
    public void testAppMainSpiltFile() {
        AppMain appMain = new AppMain();
        appMain.main(new String[]{"D:\\soucecode\\data", "newdata.txt", "10", "PRODUCERCONSUMER", "24", "8", "10240"});
    }

    @Test
    public void testReadSplitFileToWC() {
        try {
            FileSpiltToWC fileSpiltToWC = new FileSpiltToWC();
            fileSpiltToWC.main(new String[]{"D:\\data2\\s0", "D:\\data2\\s1"});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFileMerge() {
        try {
            FileMerge fileMerge = new FileMerge();
            fileMerge.main(new String[]{"D:\\data2\\s1", "D:\\data2\\s2", "data.txt"});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSplitFileBySize() {
        try {
            String sourcePath = "E:\\data\\data.txt";
            String targetPath = "E:\\data\\";
            FileSpiltToWC fileUtil = new FileSpiltToWC();
            fileUtil.splitFileBySize(new File(sourcePath), new File(targetPath));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAllJob() {
        try {
            String targetFile1 = "D:\\soucecode\\data\\s0";
            File sourceFile1 = new File("D:\\soucecode\\data\\makedatatext.txt");
            FileSpiltToWC fileUtil = new FileSpiltToWC();
            fileUtil.splitFile(sourceFile1, targetFile1);

            String targetFile2 = "D:\\soucecode\\data\\s1";
            File sourceFile2 = new File(targetFile1);
            File targetPath2 = new File(targetFile2);
            fileUtil.readSplitFileToWC(sourceFile2, targetPath2);

            FileMerge fileMerge = new FileMerge();
            String targetPath3 = "D:\\soucecode\\data\\s2";
            File targetFile3 = new File(targetPath3);
            fileMerge.mergeFile(targetPath2, targetFile3, "data.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Test
//    public void testFileSplit() {
//        try {
//            /*String targetPath = "D:\\soucecode\\data";
//            File file = new File("D:\\soucecode\\data\\data.txt");*/
//            String targetPath = "D:\\soucecode\\data\\s0";
//            File file = new File("D:\\soucecode\\data\\newdata.txt");
//            FileSpiltToWC fileUtil = new FileSpiltToWC();
//            fileUtil.splitFile(file, targetPath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
