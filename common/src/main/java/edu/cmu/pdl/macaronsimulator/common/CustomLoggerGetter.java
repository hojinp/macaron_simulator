package edu.cmu.pdl.macaronsimulator.common;

import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

public class CustomLoggerGetter {
    public static Logger logger = null;

    public static Logger getCustomLogger() {
        if (logger != null) {
            return logger;
        }
        logger = Logger.getGlobal();
        logger.setUseParentHandlers(false);
        CustomLogFormatter customLogFormatter = new CustomLogFormatter();
        final StreamHandler streamHandler = new StreamHandler(System.err, customLogFormatter) {
            @Override
            public synchronized void publish(final LogRecord record) {
                super.publish(record);
                flush();
            }
        };
        
        logger.addHandler(streamHandler);
        logger.setLevel(Level.INFO);
        return logger;
    }
}
