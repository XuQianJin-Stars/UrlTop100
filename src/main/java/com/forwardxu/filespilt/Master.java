package com.forwardxu.filespilt;

import com.forwardxu.filespilt.mutil.MutilThreadReadMaster;
import lombok.Builder;
import lombok.Data;

/**
 * Java programming implementation:
 * TXT or CSV file is 8GB, containing 100 million pieces of data, each line of data is no more than 1KB,
 * currently, we need to split the 100 million pieces of data into 10MB CSV and write them into the same directory.
 * it is required that each CSV data must be a complete line, and all files cannot be larger than 10MB.
 * <p>
 * server configuration: 4-core CPU, 10GB of physical memory, please give the general memory configuration of the virtual machine
 */
@Data
public abstract class Master {

    private String fileDir;

    private String fileName;

    private int subFileSizeLimit;

    private FileSpiltter fileSpiltter;


    public Master(String fileDir, String fileName, int subFileSizeLimit) {
        this.fileDir = fileDir;
        this.fileName = fileName;
        //TODO 更好的做法：根据上送的单位值进行换算
        this.subFileSizeLimit = subFileSizeLimit * 1024 * 1024;
        this.fileSpiltter = new FileSpiltter(this.subFileSizeLimit, this.fileDir, this.fileName);
    }

    /**
     * factory method, which produces the master example based on the masterType
     *
     * @param masterType master类型
     *                   PRODUCERCONSUMER -- PRODUCER/CONSUMER pattern implementation
     * @return
     */
    public static Master getMasterInstance(
            String masterType,
            String fileDir,
            String fileName,
            int subFileSizeLimit,
            int readTaskNum,
            int writeTaskNum,
            int queueSize,
            int bufferSize) {
        return new MutilThreadReadMaster(fileDir, fileName, subFileSizeLimit, readTaskNum, writeTaskNum, queueSize);
    }

    /**
     * other initialization operations
     */
    public void init() {

    }

    /**
     * execution of business logic
     */
    public abstract void excute();
}
