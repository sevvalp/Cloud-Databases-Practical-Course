package de.tum.i13.server.nio;

import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.server.processor.KVProcessor;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartSimpleNioServer {

    public static Logger logger = Logger.getLogger(StartSimpleNioServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        logger.info("Config: " + cfg.toString());

        DiskManager manager = DiskManager.getInstance();

        logger.info("starting server");

        /* TODO: handle arguments
         * -p   Sets the port of the server
         * -a   Which address the server should listen to, default is 127.0.0.1
         * -b   Bootstrap broker (Used by the client as the first broker to connect to and all other brokers to
         *      bootstrap in the next Milestones)
         * -d   Directory for files (Put here the files you need to persist the data, the directory is created and you
         *      can rely on that it exists)
         * -l   Relative path of the logfile, e.g., "echo.log"
         * -ll  Loglevel, e.g., INFO, ALL
         * -c   Size of the cache, e.g., 100 keys
         * -s   Cache displacement strategy, FIFO, LRU, LFU
         * -h   Displays the help
         */

        CommandProcessor kvProcessor = new KVProcessor();

        SimpleNioServer sn = new SimpleNioServer(kvProcessor);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        sn.start();
    }
}
