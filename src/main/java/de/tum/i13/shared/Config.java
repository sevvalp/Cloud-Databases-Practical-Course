package de.tum.i13.shared;

import picocli.CommandLine;

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
import java.util.logging.Level;

import static de.tum.i13.shared.Constants.TELNET_ENCODING;
import static de.tum.i13.shared.LogSetup.setLogFile;

public class Config {
    @CommandLine.Option(names = "-p", description = "sets the port of the server", defaultValue = "5153")
    public int port;

    @CommandLine.Option(names = "-a", description = "which address the server should listen to", defaultValue = "127.0.0.1")
    public String listenaddr;

    @CommandLine.Option(names = "-b", description = "bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153", defaultValue = "clouddatabases.i13.in.tum.de:5153")
    public InetSocketAddress bootstrap;

    @CommandLine.Option(names = "-d", description = "Directory for files", defaultValue = "data/")
    public Path dataDir;

    @CommandLine.Option(names = "-l", description = "Logfile", defaultValue = "echo.log")
    public Path logfile;

    @CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
    public boolean usagehelp;

    @CommandLine.Option(names = "-ll", description = "Set log level", defaultValue = "ALL")
    public String loglevel;

    @CommandLine.Option(names = "-c", description = "Set cache size", defaultValue = "1073741824")
    public int cacheSize;

    @CommandLine.Option(names = "-s", description = "Set cache displacement strategy", defaultValue = "FIFO")
    public String cacheStrategy;

    public static Config parseCommandlineArgs(String[] args) {
        Config cfg = new Config();
        CommandLine.ParseResult parseResult = new CommandLine(cfg).registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter()).parseArgs(args);

        if(!Files.exists(cfg.dataDir)) {
            try {
                Files.createDirectory(cfg.dataDir);
            } catch (IOException e) {
                System.out.println("Could not create directory");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //checks for cache strategy
        ArrayList<String> cacheStrgyList = new ArrayList<>( Arrays.asList("FIFO", "LFU", "LRU"));
        cfg.cacheStrategy = cfg.cacheStrategy.toUpperCase(Locale.ROOT);
        if(cfg.cacheStrategy.isEmpty() || !cacheStrgyList.contains(cfg.cacheStrategy)){
            System.out.println("Cache strategy not found");
            System.exit(-1);
        }

        //checks for cache size
        try{
            if(cfg.cacheSize < 0 || cfg.cacheSize > 1073741824){ //Math.pow(2,30)
                System.out.println("Given cache size is out of bounds");
                System.exit(-1);
            }
        }catch(NumberFormatException e){
            System.out.println("Cache size is not valid");
            e.printStackTrace();
            System.exit(-1);
        }

        //arrange loglevel
        ArrayList<String> logLevelList = new ArrayList<>( Arrays.asList("ALL", "CONFIG", "FINE", "FINEST","INFO", "OFF", "SEVERE","WARNING"));
        cfg.loglevel = cfg.loglevel.toUpperCase(Locale.ROOT);
        if(cfg.loglevel.isEmpty() || !logLevelList.contains(cfg.loglevel)){
            System.out.println("Wrong log level");
            System.exit(-1);
        }else{
            LogSetup.setLogLevel(Level.parse(cfg.loglevel));
        }

        //arrange port
        if(!(cfg.port>0 && cfg.port<65535)){
            System.out.println("Port number is out of bounds");
            System.exit(-1);
        }

        //arrange log file
        try{
            setLogFile(cfg.logfile);
        }catch (InvalidPathException e){
            System.out.println("Path is not valid");
            e.printStackTrace();
            System.exit(-1);
        }

        //display help
        if(cfg.usagehelp)
            displayHelp();

        if(!parseResult.errors().isEmpty()) {
            for(Exception ex : parseResult.errors()) {
                ex.printStackTrace();
            }

            CommandLine.usage(new Config(), System.out);
            System.exit(-1);
        }

        return cfg;
    }

    @Override
    public String toString() {
        return "Config{" +
                "port=" + port +
                ", listenaddr='" + listenaddr + '\'' +
                ", bootstrap=" + bootstrap +
                ", dataDir=" + dataDir +
                ", logfile=" + logfile +
                ", logLevel=" + loglevel +
                ", cacheSize=" + cacheSize +
                ", cacheStrategy=" + cacheStrategy +
                ", usagehelp=" + usagehelp +
                '}';
    }

    private static void displayHelp() {

        System.out.println("We standardized the command line parameters");
        System.out.println("Commands:");
        System.out.println("-p <port> - Sets the port of the server");
        System.out.println("\t<port> - The port of the service on the respective server.");
        System.out.println("-a <address> - Sets which address the server should listen to, default is 127.0.0.1");
        System.out.println("\t<address> - Hostname or IP address of the server.");
        System.out.println("-b <bootstrap> - Sets bootstrap broker where clients and other brokers connect first" +
                " to retrieve configuration, port and ip, e.g., 192.168.1.1:5153");
        System.out.println("\t<bootstrap> - bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153");
        System.out.println("-d <directory> Sets directory for files which need to persist");
        System.out.println("\t<directory> - Directory for files, default=data/");
        System.out.println("-l <logfilepath> - Sets relative path of the logfile");
        System.out.println("\t<logfilepath> - Logfile, defaultValue = echo.log.");
        System.out.println("-ll <loglevel> - Sets the logger to the specified log level");
        System.out.println("\t<level> - One of the following log levels: "
                + "(ALL|SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST).");
        System.out.println("-c <size> - Sets size of the cache to the specified size");
        System.out.println("\t<size> - integer size value" );
        System.out.println("-s <strategy> - Sets cache displacement strategy to the specified strategy");
        System.out.println("\t<strategy> - One of the following cache strategy: "
                + "(FIFO|LFU|LRU).");
        System.out.println("-h Prints this help message");
    }
}

