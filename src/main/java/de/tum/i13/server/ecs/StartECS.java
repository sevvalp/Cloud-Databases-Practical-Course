package de.tum.i13.server.ecs;

import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartECS {

    public static Logger logger = Logger.getLogger(StartECS.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = Config.parseCommandlineArgs(args);
        setupLogging(cfg.logfile);
        logger.info("Config: " + cfg);
        logger.info("Starting ECS");

        ECSServer ecs = new ECSServer();
        CommandProcessor ecsProcessor = new ECSCommandProcessor(ecs);
        SimpleNioServer sn = new SimpleNioServer(ecsProcessor);
        ecs.setServer(sn);

        sn.bindSocket(cfg.listenaddr, cfg.port);
        sn.start();
        //ecs.startHeartbeat();
    }
}
