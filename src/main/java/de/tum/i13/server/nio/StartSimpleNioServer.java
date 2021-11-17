package de.tum.i13.server.nio;

import de.tum.i13.client.TestClient;
import de.tum.i13.server.disk.DiskManager;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.LogSetup;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import static de.tum.i13.shared.LogSetup.setLogFile;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartSimpleNioServer {

    public static Logger logger = Logger.getLogger(StartSimpleNioServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        logger.info("Config: " + cfg.toString());

        DiskManager manager = DiskManager.getInstance();

        logger.info("starting server");

        // TODO: find a proper way to handle arguments

        Scanner scanner = new Scanner(System.in);
        String input;

        try {
            while (true) {
                System.out.print("Server> ");
                input = scanner.nextLine();
                if (input.length() == 0)
                    break;

                handleArguments(cfg, input);
            }
        } catch (Exception e) {
            logger.throwing(StartSimpleNioServer.class.getName(), "main", e);
        }

        KVStore kvStore = new KVServer(cfg.cacheStrategy, cfg.cacheSize);
        CommandProcessor kvProcessor = new KVCommandProcessor(kvStore);

        DiskManager disk = DiskManager.getInstance();
        disk.initDiskManager(cfg.dataDir.toString());

        SimpleNioServer sn = new SimpleNioServer(kvProcessor);
        ((KVServer) kvStore).setServer(sn);

        sn.bindSockets(cfg.listenaddr, cfg.port);
        //sn.bindSockets("127.0.0.1", 5153); //port: 5551
        sn.start();

    }

    private static void handleArguments(Config cfg, String input) {
        String[] command = input.split("\\s+");

            switch (command[0]) {
                case "-p":
                    arrangePort(cfg,command[1]);
                    break;
                case "-a":
                    arrangeAddress(cfg,command[1]);
                    break;
                case "-b":
                    arrangeBootstrap(cfg,command[1]);
                    break;
                case "-d":
                    arrangeDirectory(cfg, command[1]);
                    break;
                case "-l":
                    arrangeLogFile(cfg, command[1]);
                    break;
                case "-ll":
                    arrangeLoglevel(cfg,command[1]);
                    break;
                case "-c":
                    arrangeCacheSize(cfg,command[1]);
                    break;
                case "-s":
                    arrangeCacheStrategy(cfg,command[1]);
                    break;
                case "-h":
                    displayHelp(command);
                    break;
                default:
                    displayHelp(command);
                    break;
            }

    }


    private static void arrangeCacheStrategy(Config cfg, String cStrategy) {

        ArrayList<String> cacheStrgyList = new ArrayList<>( Arrays.asList("FIFO", "LFU", "LRU"));
        cStrategy = cStrategy.toUpperCase(Locale.ROOT);
        if(cStrategy.isEmpty() || !cacheStrgyList.contains(cStrategy)){
            System.out.println("Cache strategy not found");
            logger.info("Cache strategy not found");
        }
        cfg.cacheStrategy = cStrategy;
        logger.info("Cache strategy set to:" + cStrategy);
        logger.info("Config: " + cfg.toString());

    }

    private static void arrangeCacheSize(Config cfg, String cacheSize) {

        try{
            int size = Integer.parseInt(cacheSize);
            if(size < 0 || size > 1073741824){ //Math.pow(2,30)
                System.out.println("Given cache size is out of bounds");
                logger.info("Given cache size is out of bounds");
            }
            cfg.cacheSize = size;
            logger.info("Cache size set to:" + size);
            logger.info("Config: " + cfg.toString());
        }catch(NumberFormatException e){
            logger.info("Cache size is not valid");
            System.out.println("Cache size is not valid");
        }
    }

    private static void arrangeLoglevel(Config cfg, String logLevel){

        ArrayList<String> logLevelList = new ArrayList<>( Arrays.asList("ALL", "CONFIG", "FINE", "FINEST","INFO", "OFF", "SEVERE","WARNING"));
        if(logLevel.isEmpty() || !logLevelList.contains(logLevel)){
            System.out.println("Wrong log level");
        }else{
            cfg.loglevel = logLevel.toUpperCase(Locale.ROOT);
            LogSetup.setLogLevel(Level.parse(cfg.loglevel));
            logger.info("Log level set to:" + logLevel);
            logger.info("Config: " + cfg.toString());
        }
    }

    private static void arrangePort(Config cfg, String portNr){
        try{
            int port = Integer.parseInt(portNr);
            if(!(port>0 && port<65535)){
                logger.info("Port number is out of bounds" + port);
                System.out.println("Port number is out of bounds");
            }else{
                cfg.port = port;
                logger.info("Port number set to:" + port);
                logger.info("Config: " + cfg.toString());

            }

        }catch(NumberFormatException e){
            logger.info("Port number is not valid");
            System.out.println("Port number is not valid");
        }
        return;
    }

    private static void arrangeAddress(Config cfg, String address){
        if(address.isEmpty() || address.split("\\.").length != 4){
            System.out.println("Address is not valid");
            logger.info("Address is not valid");
        }
        else{
            cfg.listenaddr = address;
            logger.info("Address set to:" + address);
            logger.info("Config: " + cfg.toString());
        }
    }

    private static void arrangeBootstrap(Config cfg, String bBroker){

        try {
            String[] info = bBroker.split("\\:");
            if(info[0].split("\\.").length!=4 || info.length != 2) {
                System.out.println("Address is not valid");
                logger.info("Address is not valid");
            }
            else{
                InetAddress address =  InetAddress.getByAddress(info[0].getBytes(TELNET_ENCODING));
                int port = Integer.parseInt(info[1]);
                cfg.bootstrap = new InetSocketAddress(address,port);
                logger.info("Bootstrap set to:" + bBroker);
                logger.info("Config: " + cfg.toString());
            }
        } catch (UnknownHostException e) {
            System.out.println("Address is not valid");
            logger.info("Address is not valid");
        } catch (UnsupportedEncodingException e) {
            System.out.println("Address is not valid");
            logger.info("Address is not valid");
        } catch (NumberFormatException e){
            System.out.println("Port is not valid");
            logger.info("Port is not valid");
        }
    }

    private static void arrangeDirectory(Config cfg, String path){

        try{
            if (path.charAt(path.length() - 1) != '/') path += "/";

            Path fpath = Paths.get(path);

            if (!Files.exists(fpath)) {
                    File dir = new File(path);
                    dir.mkdir();
            }

            cfg.dataDir = fpath;
            logger.info("Data directory set to: " + path);
            logger.info("Config: " + cfg.toString());
        } catch(SecurityException e){
            System.out.println("An error occurred while creating the data folder");
            logger.info("An error occurred while creating the data folder");
        }catch (InvalidPathException e){
            System.out.println("Path is not valid");
            logger.info("Path is not valid");
        }
    }

    private static void arrangeLogFile(Config cfg, String logFile){

        try{
            Path logpath = Paths.get(logFile);
            cfg.logfile = logpath;
            setLogFile(logpath);
            logger.info("Log file set to: " + logFile);
            logger.info("Config: " + cfg.toString());
        }catch (InvalidPathException e){
            System.out.println("Path is not valid");
            logger.info("Path is not valid");
        }

    }

    private static void displayHelp(String[] commands) {
        logger.info("Printing help");

        for (String c : commands) {
            switch (c) {
                case "-p": {
                    System.out.println("-p <port> - Sets the port of the server");
                    System.out.println("\t<port> - The port of the service on the respective server.");
                    break;
                }
                case "-a": {
                    System.out.println("-a <address> - Sets which address the server should listen to, default is 127.0.0.1");
                    System.out.println("\t<address> - Hostname or IP address of the server.");
                    break;
                }
                case "-b": {
                    System.out.println("-b <bootstrap> - Sets bootstrap broker");
                    System.out.println("\t<bootstrap> - bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153");
                    break;
                }
                case "-d": {
                    System.out.println("-d <directory> Sets directory for files which need to persist");
                    System.out.println("\t<directory> - Directory for files, default=data/");
                    break;
                }
                case "-l": {
                    System.out.println("-l <logfilepath> - Sets relative path of the logfile");
                    System.out.println("\t<logfilepath> - Logfile, defaultValue = echo.log.");
                    break;
                }
                case "-ll": {
                    System.out.println("-ll <loglevel> - Sets the logger to the specified log level");
                    System.out.println("\t<level> - One of the following log levels: "
                            + "(ALL|SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST).");
                    break;
                }
                case "-c": {
                    System.out.println("-c <size> - Sets size of the cache to the specified size");
                    System.out.println("\t<size> - integer size value" );
                    break;
                }
                case "-s": {
                    System.out.println("-s <strategy> - Sets cache displacement strategy to the specified strategy");
                    System.out.println("\t<strategy> - One of the following cache strategy: "
                            + "(FIFO|LFU|LRU).");
                    break;
                }
                case "-h": {
                    System.out.println("We standardized the command line parameters");
                    System.out.println("Commands:");
                    System.out.println("-p <port> - Sets the port of the server");
                    System.out.println("-a <address> - Sets which address the server should listen to, default is 127.0.0.1");
                    System.out.println("-b <bootstrap> - Sets bootstrap broker where clients and other brokers connect first" +
                            " to retrieve configuration, port and ip, e.g., 192.168.1.1:5153");
                    System.out.println("-d <directory> Sets directory for files which need to persist");
                    System.out.println("-l <logfilepath> - Sets relative path of the logfile");
                    System.out.println("-ll <loglevel> - Sets the logger to the specified log level");
                    System.out.println("-c <size> - Sets size of the cache to the specified size");
                    System.out.println("-s <strategy> - Sets cache displacement strategy to the specified strategy");
                    System.out.println("-h Prints this help message");
                    break;
                }
            }
        }
    }

}
