package com.forwardxu.filespilt;


import com.forwardxu.Utils.ToolUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AppMain {
    private static Log log = LogFactory.getLog(AppMain.class);

    /**
     * parameter 1: the directory where the source file is located
     * parameter 2: original file name
     * parameter 3: subfile size upper limit, must be an integer, unit :M *
     * parameter 4: execution mode: PRODUCERCONSUMER -- producer-consumer mode;
     * parameter 5: number of read tasks, effective when parameter 4 is PRODUCERCONSUMER, default is 24
     * parameter 6: write task number, which is valid when parameter 4 is PRODUCERCONSUMER, default is 8; When the mode is DISRUPTOR, it must be a multiple of 2
     * parameter 7: task queue size, effective when parameter 4 is PRODUCERCONSUMER, default is 10240; When PRODUCERCONSUMER, this means all consumers share a queue, and when DISRUPTOR, each consumer enjoys a separate queue
     * when 4 is a Disruptor, the default is 1024
     *
     * @param args
     */
    public static void main(String[] args) {
        int readTaskNum = 24;
        int writeTaskNum = 8;
        int queueSize = 10240;
        int bufferSize = 1024;
        //获取参数
        if (args != null) {
            if (args.length == 4) {

            } else if (args.length == 7 || args.length == 8) {
                readTaskNum = Integer.valueOf(args[4]);
                writeTaskNum = Integer.valueOf(args[5]);
                queueSize = Integer.valueOf(args[6]);
            } else if (args.length == 8) {
                bufferSize = Integer.valueOf(args[7]);
            } else {
                log.error("Example: #fileDir, #fileName, #subFileSizeLimit, #mode [, #readTaskNum, #writeTaskNum, #queueSize, #bufferSize],");
                return;
            }
        } else {
            log.error("Example: #fileDir, #fileName, #subFileSizeLimit, #mode,");
            return;
        }
        String fileDir = args[0];
        String fileName = args[1];
        String subFileSizeLimitStr = args[2];
        String mode = args[3];


        //参数合法性校验
        if (ToolUtils.isNull(fileDir, fileName, subFileSizeLimitStr)) {
            log.error("Some parameters are null! Examples: #fileDir, #fileName, #subFileSizeLimit");
            return;
        }

        String[] fileNameItems = fileName.split(Constants.FILENAME_SEPARATOR);
        if (fileNameItems.length != 2) {
            log.error("Parameter fileName format error! Example: fileName. CSV or .TXT");
            return;
        }

        if ((mode != null && (!mode.equals(Constants.MASTER_TYPE_PRODUCER_CONSUMER)))) {
            log.error("The parameter mode must be PRODUCERCONSUMER");
            return;
        }

        int subFileSizeLimit = 0;
        try {
            subFileSizeLimit = Integer.valueOf(subFileSizeLimitStr);
        } catch (NumberFormatException e) {
            log.error("The upper limit of the child file size must be an integer.");
            return;
        }

        //Get the master instance through the factory method
        Master master = Master.getMasterInstance(mode, fileDir, fileName, subFileSizeLimit, readTaskNum, writeTaskNum, queueSize, bufferSize);
        log.info("The master is: " + master.getClass().getName());
        master.init();
        //Start the master
        master.excute();

    }
}
