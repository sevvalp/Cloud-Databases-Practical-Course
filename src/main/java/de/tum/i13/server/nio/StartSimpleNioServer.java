package de.tum.i13.server.nio;

import de.tum.i13.server.echo.EchoLogic;
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

        logger.info("starting server");

        //Replace with your Key Value command processor
        CommandProcessor echoLogic = new EchoLogic();

        SimpleNioServer sn = new SimpleNioServer(echoLogic);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        sn.start();
    }
}
