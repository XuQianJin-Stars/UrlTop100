package com.forwardxu.filespilt;

import com.forwardxu.Utils.ToolUtils;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * File split stage, only support single thread operation
 */
@Data
public class FileSpiltter {
    /**
     * The path of the split subfile
     */
    private String fileDir;

    /**
     * The size limit of the split subfile
     */
    private int subFileSizeLimit;

    /**
     * Current cache file contents smaller than subFileSizeLimit
     */
    private List<String> fileCache;

    /**
     * The current cache size is less than the subFileSizeLimit
     */
    private int fileCacheSize;

    /**
     * number of subfiles split
     * since it is a single-threaded operation, atomic counters are not used here
     */
    private int subFileCounter;

    /**
     * subfile full name template to be generated
     */
    private String fileNameTemplate;

    /**
     * source file name
     */
    private String fileName;

    public FileSpiltter(int subFileSizeLimit, String fileDir, String fileNameTemplate) {
        this.fileDir = fileDir;
        this.fileNameTemplate = fileNameTemplate;
        this.fileName = ToolUtils.getFileName(fileNameTemplate);
        this.subFileSizeLimit = subFileSizeLimit;
        this.fileCacheSize = 0;
        this.fileCache = new ArrayList<String>();
    }

    /**
     * the name of the generated child file
     *
     * @return Subfile name
     */
    private String genSubFileName() {
        String fileName = "";
        String[] fileNameItems = this.fileNameTemplate.split(Constants.FILENAME_SEPARATOR);
        if (fileNameItems.length == 1) {
            fileName = fileNameItems[0] + this.subFileCounter;
        } else {
            StringBuffer sb = new StringBuffer();
            sb.append(fileNameItems[0]).append(this.subFileCounter)
                    .append(".").append(fileNameItems[1]);
            fileName = sb.toString();
        }
        return fileName;
    }

    /**
     * the name of the generated child file
     *
     * @param subFileNo —— Subfile number
     * @return Subfile name
     */
    public String genSubFileFullName(int subFileNo) {
        String fileName = "";
        String[] fileNameItems = this.fileNameTemplate.split(Constants.FILENAME_SEPARATOR);
        if (fileNameItems.length == 1) {
            fileName = fileNameItems[0] + subFileNo;
        } else {
            StringBuffer sb = new StringBuffer();
            sb.append(fileNameItems[0]).append(subFileNo)
                    .append(".").append(fileNameItems[1]);
            fileName = sb.toString();
        }
        return ToolUtils.genFullFileName(this.fileDir, fileName);
    }

    /**
     * Checks if the specified file size is exceeded
     *
     * @return true —— More than
     * false —— No more than
     */
    public boolean overFileLimit(int size) {
        return size >= this.subFileSizeLimit;
    }
}
