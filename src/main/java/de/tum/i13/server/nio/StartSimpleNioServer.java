package de.tum.i13.server.nio;

import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVECSCommunicator;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;
import static de.tum.i13.shared.Util.getFreePort;

public class StartSimpleNioServer {

    public static Logger logger = Logger.getLogger(StartSimpleNioServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        logger.info("Config: " + cfg.toString());

        DiskManager manager = DiskManager.getInstance();

        logger.info("starting server");

        int intraPort = getFreePort(); //5551;
        KVStore kvStore = new KVServer(cfg.cacheStrategy, cfg.cacheSize, cfg.bootstrap, cfg.listenaddr, cfg.port, intraPort);
        CommandProcessor kvProcessor = new KVCommandProcessor(kvStore);

        Thread thread = new Thread(new KVECSCommunicator(kvStore,cfg.bootstrap,cfg.listenaddr, cfg.port, intraPort));
        thread.start();

        DiskManager disk = DiskManager.getInstance();
        disk.initDiskManager(cfg.dataDir.toString());

        SimpleNioServer sn = new SimpleNioServer(kvProcessor);
        ((KVServer) kvStore).setServer(sn);

        sn.bindSockets(cfg.listenaddr, cfg.port, intraPort);
        //sn.bindSockets("127.0.0.1", 5153); //port: 5551f
        sn.start();


    }

}
