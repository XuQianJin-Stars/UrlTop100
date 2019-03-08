package com.forwardxu.filespilt.mutil;

import lombok.Builder;
import lombok.Data;

import java.util.Arrays;

/**
 * A line in a file that serves as the smallest unit of execution for a read/write task.
 */
@Data
@Builder
public class FileLine {

    /**
     * row byte size, unit byte
     */
    private long lineSize;
    /**
     * row content byte array
     */
    private byte[] lineContent;

    /**
     * the value generated by hashing the content and mod the number of files
     */
    private int fileId;

    public FileLine(long lineSize, byte[] lineContent, int fileId) {
        this.lineSize = lineSize;
        this.lineContent = lineContent;
        this.fileId = fileId;
    }

    @Override
    public String toString() {
        return "FileLine [lineSize=" + lineSize + ", lineContent=" + Arrays.toString(lineContent) + ", fileId=" + fileId + "]";
    }


}