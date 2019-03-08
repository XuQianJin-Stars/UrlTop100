package com.forwardxu.filespilt.mutil;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.forwardxu.filespilt.Master;
import com.forwardxu.Utils.ToolUtils;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Consumer producer mode, multithreaded read + multithreaded write +NIO, greatly improved performance
 * Start parameter: D: temp test.csv 10 PRODUCERCONSUMER 24 8 20480
 */
public class MutilThreadReadMaster extends Master {

    Log log = LogFactory.getLog(this.getClass());

    private ExecutorService fileReadPool;

    private ExecutorService fileWritePool;

    /**
     * A blocking queue used to exchange the contents of a child file
     */
    private BlockingQueue<FileLine> queue;

    private int readTaskNum;

    private int writeTaskNum;

    private int queueSize;

    private TaskAllocater taskAllocater;

    public MutilThreadReadMaster(String fileDir, String fileName, int subFileSizeLimit, int readTaskNum, int writeTaskNum, int queueSize) {
        super(fileDir, fileName, subFileSizeLimit);
        this.readTaskNum = readTaskNum;
        this.writeTaskNum = writeTaskNum;
        this.queueSize = queueSize;
        this.fileReadPool = new ThreadPoolExecutor(this.readTaskNum, this.readTaskNum, 0l,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(300), new FileReadThreadFactory());
        this.fileWritePool = new ThreadPoolExecutor(this.writeTaskNum, this.writeTaskNum, 0l,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(300), new FileWriteThreadFactory());

        queue = new LinkedBlockingQueue<FileLine>(this.queueSize);
    }

    public void init() {
        //String orignFileFullName, int readTaskNum, int writeTaskNum, int maxLineSize, FileSpiltter fileSpiltter
        taskAllocater = new TaskAllocater(ToolUtils.genFullFileName(this.getFileDir(), this.getFileName()), this.readTaskNum,
                this.writeTaskNum, 1024, this.getFileSpiltter(), this.queue);
    }


    @Override
    public void excute() {
        log.info("begin to spilt...");
        long startTime = System.currentTimeMillis();
        //allocateReadTask
        List<FileReadTask> fileReadTasks = this.taskAllocater.initFileReadTask();

        //allocatWriteTask
        List<FileWriteTask> fileWriteTasks = this.taskAllocater.initFileWriteTask();

        //submit FileReadTasks
        List<Future> fileReadFutureList = new ArrayList<Future>();
        for (FileReadTask task : fileReadTasks) {
            Future future = this.fileReadPool.submit(task, task);
            fileReadFutureList.add(future);
        }

        //submit FileWriteTasks
        List<Future> fileWriteFutureList = new ArrayList<Future>();
        for (FileWriteTask task : fileWriteTasks) {
            Future future = this.fileWritePool.submit(task, task);
            fileWriteFutureList.add(future);
        }

        //get read result and shutdown fileReadPool
        int totalReadedSize = getTotalReadedSize(fileReadFutureList);
        this.fileReadPool.shutdown();

        //Determine whether you can stop fileWrite thread.FileRead thread was previously stopped and no new tasks will be generated. If the task queue is empty, you can stop
        while (true) {
            if (this.queue.isEmpty()) {
                //Tell all filewritetasks to stop task processing
                FileWriteTask.setDone(true);
                break;
            }
        }
        //get write result and shutdown fileWritePool
        int totalWritenSize = getTotalWritenSize(fileWriteFutureList);
        this.fileWritePool.shutdown();

        //check file spilt result
        //Checks whether the source file size and the final written file size are equal.
        if (totalReadedSize == totalWritenSize) {
            log.info("File split successful! The source file size is:" + totalReadedSize + ", The subfile size after splitting is:" + totalWritenSize);
        } else {
            log.warn("File split successful! The source file size is:" + totalReadedSize + ", The subfile size after splitting is:" + totalWritenSize);
        }
        long endTime = System.currentTimeMillis();
        log.info("durition（ms）=" + (endTime - startTime));
        log.info("readTaskNum=" + this.readTaskNum);
        log.info("writeTaskNum=" + this.writeTaskNum);
        log.info("queueSize=" + this.queueSize);


        // Save the cut file information to properties
        try {
            Properties pro = new Properties();
            pro.setProperty("durition（ms）", Long.toString(endTime - startTime));
            pro.setProperty("fileName", getFileName());
            pro.setProperty("readTaskNum", Integer.toString(this.readTaskNum));
            pro.setProperty("writeTaskNum", Integer.toString(this.writeTaskNum));
            pro.setProperty("queueSize", Integer.toString(this.queueSize));
            pro.setProperty("partCount", "130");

            FileOutputStream fo = new FileOutputStream(new File(getFileDir(), ToolUtils.getFileName(getFileName()) + ".properties"));
            // Write to properties file
            pro.store(fo, "save file info");
            fo.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int getTotalReadedSize(List<Future> fileReadFutureList) {
        Future futureTemp = null;
        int totalReadedSize = 0;
        while (true) {
            for (Iterator<Future> it = fileReadFutureList.iterator(); it.hasNext(); ) {
                futureTemp = it.next();
                if (futureTemp.isDone()) {
                    try {
                        totalReadedSize += ((FileReadTask) futureTemp.get()).getReadedSize();
                    } catch (InterruptedException e) {
                        log.error("Failed to get thread execution result.");
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        log.error("Failed to get thread execution result.");
                        e.printStackTrace();
                    }

                    it.remove();
                }
            }
            if (fileReadFutureList == null || fileReadFutureList.size() == 0) {
                break;
            }
        }
        return totalReadedSize;
    }


    private int getTotalWritenSize(List<Future> fileWriteFutureList) {
        Future futureTemp = null;
        int totalWritenSize = 0;
        while (true) {
            for (Iterator<Future> it = fileWriteFutureList.iterator(); it.hasNext(); ) {
                futureTemp = it.next();
                if (futureTemp.isDone()) {
                    try {
                        totalWritenSize += ((FileWriteTask) futureTemp.get()).getWritenSize();
                    } catch (InterruptedException e) {
                        log.error("Failed to get thread execution result.");
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        log.error("Failed to get thread execution result.");
                        e.printStackTrace();
                    }

                    it.remove();
                }
            }
            if (fileWriteFutureList == null || fileWriteFutureList.size() == 0) {
                break;
            }
        }
        return totalWritenSize;
    }


}
