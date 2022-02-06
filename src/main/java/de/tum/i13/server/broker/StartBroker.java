package de.tum.i13.server.broker;

import de.tum.i13.server.nio.SimpleNioServer;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartBroker {

    public static Logger logger = Logger.getLogger(StartBroker.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = Config.parseCommandlineArgs(args);
        setupLogging(cfg.logfile);
        logger.info("Config: " + cfg);
        logger.info("Starting Broker");

        Broker broker = new Broker();
        CommandProcessor ecsProcessor = new BrokerCommandProcessor(broker);
        SimpleNioServer sn = new SimpleNioServer(ecsProcessor);
        broker.setServer(sn);

        sn.bindSocket("127.0.0.1", 5155);
        sn.start();
    }
}
