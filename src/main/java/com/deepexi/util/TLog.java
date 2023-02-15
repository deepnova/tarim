package com.deepexi.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * TLog
 *
 */
public class TLog {

    public final static Logger logger = LogManager.getLogger(TLog.class);

    public static void debug(String message){
        logger.debug("[Tarim] " + message);
    }

    public static void info(String message){
        logger.info("[Tarim] " + message);
    }

    public static void warn(String message){
        logger.warn("[Tarim] " + message);
    }

    public static void error(String message){
        logger.error("[Tarim] " + message);
    }
}

