package com.forwardxu.filespilt.mutil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.forwardxu.filespilt.Constants;
import com.forwardxu.filespilt.FileSpiltter;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TaskAllocater {

    /**
     * full name of original document
     */
    private String orignFileFullName;

    /**
     * number of read operation tasks
     */
    private int readTaskNum;

    /**
     * number of write operation tasks
     */
    private int writeTaskNum;
    /**
     * maximum number of bytes in a row
     */
    private int maxLineSize;
    /**
     * file cutter
     */
    private FileSpiltter fileSpiltter;

    /**
     * a blocking queue used to exchange the contents of a child file
     */
    private BlockingQueue<FileLine> queue;

    public TaskAllocater(String orignFileFullName, int readTaskNum, int writeTaskNum, int maxLineSize, FileSpiltter fileSpiltter, BlockingQueue<FileLine> queue) {
        this.orignFileFullName = orignFileFullName;
        this.readTaskNum = readTaskNum;
        this.writeTaskNum = writeTaskNum;
        this.maxLineSize = maxLineSize;
        this.fileSpiltter = fileSpiltter;
        this.queue = queue;
    }

    /**
     * complete initialization of FileReadTask based on the original file size and number of file read tasks
     *
     * @return List<FileReadTask> FileReadTask任务列表
     */
    public List<FileReadTask> initFileReadTask() {
        if (this.readTaskNum <= 0) {
            throw new IllegalArgumentException("The number of file read tasks must be greater than 0!");
        }

        RandomAccessFile orginFile = null;
        long orignFileSize = 0;
        try {
            orginFile = new RandomAccessFile(this.orignFileFullName, "r");
            //Gets the file length (in bytes)
            long copycount = orginFile.length() / Integer.MAX_VALUE;
            System.out.println("copycount:" + copycount);
            orignFileSize = orginFile.length();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        long avgToReadSize = orignFileSize / this.readTaskNum;

        List<FileReadTask> taskList = new ArrayList<FileReadTask>();

        long lastEndFilePointer = -1;
        long revisedEndFilePointer = 0;
        for (int i = 0; i < this.readTaskNum; i++) {
            FileReadTask task = null;
            //The last task also reads the remaining unallocated data
            if (i == this.readTaskNum - 1) {
                task = new FileReadTask(i, lastEndFilePointer + 1, orignFileSize - 1, this.orignFileFullName, this.queue);
            } else {
                revisedEndFilePointer = reviseEndFilePointer(lastEndFilePointer + avgToReadSize, orginFile);
                task = new FileReadTask(i, lastEndFilePointer + 1, revisedEndFilePointer, this.orignFileFullName, this.queue);
                lastEndFilePointer = revisedEndFilePointer;
            }
            taskList.add(task);
            System.out.println("Create a FileReadTask：" + task);
        }
        return taskList;
    }

    /**
     * 修正任务的结束文件指针位置，加上第一个换行符/回车符 的偏移量，确保‘完整行’需求
     *
     * @param endFilePointer
     * @param orginFile
     * @return revisedEndFilePointer -- a corrected endFilePointer
     * TODO this scheme needs to be given the maximum row size in advance, which is difficult to have this limit in the actual situation, to be optimized.
     */
    private long reviseEndFilePointer(long endFilePointer, RandomAccessFile orginFile) {
        long revisedEndFilePointer = endFilePointer;
        byte[] tempBytes = new byte[this.maxLineSize];
        try {
            orginFile.seek(endFilePointer - this.maxLineSize + 1);
            orginFile.readFully(tempBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int revisedNum = 0;
        for (int i = tempBytes.length - 1; i >= 1; i--) {
            //换行符或者回车符，做修正
//            if (Constants.ENTER_CHAR_ASCII == tempBytes[i - 1] && Constants.NEW_LINE_CHAR_ASCII == tempBytes[i]) {
            if (Constants.NEW_LINE_CHAR_ASCII == tempBytes[i]) {
                break;
            } else {
                revisedNum++;
            }
        }
        return revisedEndFilePointer - revisedNum;
    }

    /**
     * writes the task list based on the specified readTaskNum initialization file.
     *
     * @return List<filewritetask> -- file writes to task List</filewritetask>
     */
    public List<FileWriteTask> initFileWriteTask() {
        if (this.writeTaskNum <= 0) {
            throw new IllegalArgumentException("The number of file write tasks must be greater than 0!");
        }

        List<FileWriteTask> taskList = new ArrayList<FileWriteTask>();
        for (int i = 0; i < this.writeTaskNum; i++) {
            FileWriteTask task = new FileWriteTask(i, this.fileSpiltter, this.queue);
            taskList.add(task);
            System.out.println("Create a FileWriteTask：" + task);
        }
        return taskList;
    }
}
