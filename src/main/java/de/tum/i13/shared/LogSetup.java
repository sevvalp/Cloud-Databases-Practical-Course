package de.tum.i13.shared;

import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.*;

public class LogSetup {
    public static void setupLogging(Path logfile) {
        Logger logger = LogManager.getLogManager().getLogger("");
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");

        FileHandler fileHandler = null;
        try {
            fileHandler = new FileHandler(logfile.getFileName().toString(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fileHandler.setFormatter(new SimpleFormatter());
        logger.addHandler(fileHandler);

        for (Handler h : logger.getHandlers()) {
            h.setLevel(Level.ALL);
        }
        logger.setLevel(Level.ALL); //we want log everything
    }
    public static void setLogFile(Path logfile){
        Logger logger = LogManager.getLogManager().getLogger("");
        FileHandler fileHandler = null;
        try {
            fileHandler = new FileHandler(logfile.getFileName().toString(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fileHandler.setFormatter(new SimpleFormatter());

        //Only allows logging to one file
//        Handler[] handlers = logger.getHandlers();
//        if(handlers.length > 0){
//            for(int i =0; i<handlers.length; i++)
//                logger.removeHandler(handlers[i]);
//        }

        logger.addHandler(fileHandler);

    }
    public static void setLogLevel(Level logLevel){
        Logger logger = LogManager.getLogManager().getLogger("");

        for (Handler h : logger.getHandlers()) {
            h.setLevel(logLevel);
        }
        logger.setLevel(logLevel);
    }
}