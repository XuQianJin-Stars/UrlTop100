package com.forwardxu.filespilt.mutil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

import com.forwardxu.filespilt.Constants;
import com.forwardxu.Utils.ToolUtils;
import lombok.Builder;
import lombok.Data;

/**
 * file reading task
 */
@Data
public class FileReadTask extends Thread {
    /**
     * task number
     */
    private int taskSeq;
    /**
     * task name
     */
    private String taskName;
    /**
     * the point at which the reading begins
     */
    private long beginFilePointer;
    /**
     * the point at which the reading ends
     */
    private long endFilePointer;
    /**
     * the total size to be read
     */
    private long toReadSize;
    /**
     * full path to source file
     */
    private String orignFileFullName;
    /**
     * the actual data size read
     */
    private int readedSize;
    /**
     * number of bytes read at a time
     */
    private int readSizeOneTime;
    /**
     * a blocking queue used to exchange the contents of a child file
     */
    private BlockingQueue<FileLine> queue;

    /**
     * 100G files divided into 800M, about 130 files are needed
     */
    private static int targetFileSize = 130;

    public FileReadTask(int taskSeq, long beginFilePointer, long endFilePointer, String orignFileFullName, BlockingQueue<FileLine> queue) {
        this.taskSeq = taskSeq;
        this.beginFilePointer = beginFilePointer;
        this.endFilePointer = endFilePointer;
        this.orignFileFullName = orignFileFullName;
        this.queue = queue;
        this.readSizeOneTime = 10 * 1024 * 1024;
        this.taskName = "FileReadTask_" + this.taskSeq;
        this.readedSize = 0;
        this.toReadSize = this.endFilePointer - this.beginFilePointer + 1;
    }


    /**
     * reads the contents of the file from the specified file directory and converts each line to a FileLine object, which is written to the queue.
     * line end flag: when /n is read
     */
    @Override
    public void run() {
        try {
            @SuppressWarnings("resource")
            FileInputStream orginFile = new FileInputStream(this.orignFileFullName);
            long index = this.beginFilePointer;
            long totalSpiltedSize = 0;
            MappedByteBuffer inputBuffer = null;
            ToolUtils toolUtils = new ToolUtils();
//            if it is a whole line, the last two characters are /n, so index is less than this.endfilepointer need to be entered
            for (; index < this.endFilePointer; ) {
//				System.err.println(Thread.currentThread().getName()+": index = "+index);
                totalSpiltedSize = Math.min(this.readSizeOneTime, this.endFilePointer - index + 1);
                inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                        index, totalSpiltedSize);
                try {
                    index = spiltFileLine(inputBuffer, index, totalSpiltedSize);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return;
                }

            }

// for the last task, the last line may not contain the /n symbol, and the remaining unsplit file line content may be written directly in the presence of a separate line
            if (this.endFilePointer - index >= 0) {
                int fileLineSize = (int) (this.endFilePointer - index);
                byte[] fileLineCache = new byte[fileLineSize];
                inputBuffer.get(fileLineCache, 0, fileLineSize);
                int fileId = (int) (toolUtils.hash(fileLineCache) % targetFileSize);
                FileLine fileLine = new FileLine(fileLineSize, fileLineCache, fileId);
                //将fileLine对象放入queue中
                try {
// write the message in blocking mode, and once the queue is full, the message will not be written to it,
// ensuring that oom will not appear in the case of slow consumer processing
                    this.queue.put(fileLine);
//					System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
                    this.readedSize += fileLineSize;
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    /**
     * split the byte data for each row from the byte cache and queue it
     *
     * @param inputBuffer target byte cache
     * @param index is the starting byte location of the split
     * @param totalSpiltedSize the total number of bytes that need to be split
     * @return index after splitting, the starting position of the latest byte to be processed
     * @throws Exception
     * @ TODO can directly use RandomAccessFile. ReadLine () method
     */
    long spiltFileLine(MappedByteBuffer inputBuffer, long index, long totalSpiltedSize) throws Exception {
        long subIndex = index;
        boolean newLineFlag = false;
        ToolUtils toolUtils = new ToolUtils();
        for (int i = 0; i < totalSpiltedSize - 1; i++) {
// a carriage return/line feed is encountered
//			if(Constants.ENTER_CHAR_ASCII == inputBuffer.get(i) && Constants.NEW_LINE_CHAR_ASCII == inputBuffer.get(i+1)) {
            if (Constants.NEW_LINE_CHAR_ASCII == inputBuffer.get(i + 1)) {
                newLineFlag = true;
// also contains the /n bytes after /r in the line
                i++;
                int fileLineSize = (int) (subIndex + i + 1 - index);
                byte[] fileLineCache = new byte[fileLineSize];
                inputBuffer.get(fileLineCache, 0, fileLineSize);
                int fileId = (int) (toolUtils.hash(fileLineCache) % targetFileSize);
                FileLine fileLine = new FileLine(fileLineSize, fileLineCache, fileId);

// put the fileLine objects in the queue
                try {
                    this.queue.put(fileLine);
//					System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
                    this.readedSize += fileLineSize;
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                index += fileLineSize;
            }
        }
        if (!newLineFlag) {
// the last line, written separately
            if (totalSpiltedSize < this.readSizeOneTime) {
                byte[] tempBytes = new byte[(int) totalSpiltedSize];
                inputBuffer.get(tempBytes, 0, (int) totalSpiltedSize);
                int fileId = (int) (toolUtils.hash(tempBytes) % targetFileSize);
                FileLine fileLine = new FileLine(totalSpiltedSize, tempBytes, fileId);
                try {
                    this.queue.put(fileLine);
//					System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
                    this.readedSize += totalSpiltedSize;
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                index += totalSpiltedSize;
// the file line is too large to be read by line. The readSizeOneTime parameter needs to be adjusted
            } else {
                System.err.println("index=" + index + ",totalSpiltedSize=" + totalSpiltedSize);

                System.err.println("this content has no new line, and its length has greater than " + this.readSizeOneTime);
                throw new Exception("this content has no new line, and its length has greater than " + this.readSizeOneTime);
            }
        }
        return index;
    }

    private void writeSubFile(MappedByteBuffer inputBuffer, int index, int totalSpiltedSize) {
        String subFileName = "D:\\temp\\error.txt";

        byte[] tempBytes = new byte[totalSpiltedSize];
        inputBuffer.get(tempBytes, 0, totalSpiltedSize);

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(subFileName);
            fos.write(tempBytes);
            fos.flush();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public String toString() {
        return "FileReadTask [taskSeq=" + taskSeq + ", taskName=" + taskName + ", beginFilePointer=" + beginFilePointer
                + ", endFilePointer=" + endFilePointer + ", toReadSize=" + toReadSize + ", orignFileFullName="
                + orignFileFullName + ", readedSize=" + readedSize + ", readSizeOneTime=" + readSizeOneTime + "]";
    }


}
