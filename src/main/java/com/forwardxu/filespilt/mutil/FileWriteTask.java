package com.forwardxu.filespilt.mutil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.forwardxu.filespilt.FileSpiltter;
import lombok.Builder;
import lombok.Data;

/**
 * file write task.
 * write the contents of the file to the sub-file, and automatically generate and switch the sub-file when writing.
 * subfiles are written once, using NIO writes
 */
@Data
public class FileWriteTask extends Thread {
    /**
     * task number
     */
    private int taskSeq;
    /**
     * task name
     */
    private String taskName;
    /**
     * file cutter
     */
    private FileSpiltter fileSpiltter;
    /**
     * total data actually written
     */
    private int writenSize;
    /**
     * a blocking queue used to exchange the contents of a child file
     */
    private BlockingQueue<FileLine> queue;
    /**
     * subfile count
     */
    private static AtomicInteger subFileCounter = new AtomicInteger(0);
    /**
     * handles the cache contents of files in progress
     */
    private List<FileLine> subFileCache;
    /**
     * the number of bytes of cached content in the file being processed
     */
    private int subFileCacheSize;

    private volatile static boolean isDone = false;

    /**
     * 100G files divided into 800M, about 130 files are needed
     */
    private static int targetFileSize = 130;

    /**
     * read the FileLine contents from the queue and write to the child file, making sure the child file size <10M
     */
    @Override
    public void run() {
        FileLine fileLine = null;

// there will be several more attempts until the master is set to isDone=true, and then exit
        while (!isDone) {
            try {
                fileLine = queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (fileLine == null) {
                continue;
            }

            int totalSize = this.subFileCacheSize + (int) fileLine.getLineSize();
// the content of the cached child file is already greater than the upper limit
            if (totalSize >= this.fileSpiltter.getSubFileSizeLimit()) {
                writeSubFile();
            }
            this.subFileCache.add(fileLine);
            this.subFileCacheSize += fileLine.getLineSize();
        }

//        On exit, write the remaining file cache from task to disk
        writeSubFile();

    }


    /**
     * 集中一次写入整个子文件
     */
    private void writeSubFile() {
        int subFileNo = subFileCounter.getAndIncrement();
        String subFileName = fileSpiltter.genSubFileFullName(subFileNo);
//        RandomAccessFile randomAccessFile = null;
        List<FileOutputStream> al = new ArrayList<FileOutputStream>();
        try {
//            randomAccessFile = new RandomAccessFile(subFileName, "rw");
//            FileChannel writer = randomAccessFile.getChannel();
//            ByteBuffer writerBuffer = writer.map(FileChannel.MapMode.READ_WRITE, 0, this.subFileCacheSize);
//            for (FileLine fileLine : this.subFileCache) {
//                writerBuffer.put(fileLine.getLineContent());
//            }
//            writerBuffer.clear();
//            writerBuffer = null;

            for (int i = 0; i < targetFileSize; i++) {
                try {
                    al.add(new FileOutputStream(new File(fileSpiltter.getFileDir(),
                            fileSpiltter.getFileName() + i + ".part"), true));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            for (FileLine fileLine : this.subFileCache) {
                al.get(fileLine.getFileId()).write(fileLine.getLineContent());
                al.get(fileLine.getFileId()).flush();
            }

            for (int i = 0; i < al.size(); i++) {
                al.get(i).close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            if (randomAccessFile != null) {
//                try {
//                    randomAccessFile.close();
//                } catch (IOException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
        }

        System.out.println(Thread.currentThread().getName());
        this.writenSize += this.subFileCacheSize;

        //reset
        this.subFileCache.clear();
// the following line of code causes the thread to block TODO
//		this.subFileCache = null;
        this.subFileCacheSize = 0;
    }

    public FileWriteTask(int taskSeq, FileSpiltter fileSpiltter, BlockingQueue<FileLine> queue) {
        this.taskSeq = taskSeq;
        this.fileSpiltter = fileSpiltter;
        this.queue = queue;
        this.writenSize = 0;
        this.taskName = "FileWriteTask_" + this.taskSeq;
        this.subFileCache = new ArrayList<FileLine>();
    }

    public static boolean isDone() {
        return isDone;
    }

    public static void setDone(boolean isDone) {
        FileWriteTask.isDone = isDone;
    }

    @Override
    public String toString() {
        return "FileWriteTask [taskSeq=" + taskSeq + ", taskName=" + taskName + ", writenSize=" + writenSize
                + ", subFileCacheSize=" + subFileCacheSize + "]";
    }


}
