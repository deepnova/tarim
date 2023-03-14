package com.deepexi.tarimdb;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.tarimkv.*;
import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;
/**
 * TarimServer
 *
 */
public class TarimServer {
    public final static Logger logger = LogManager.getLogger(TarimServer.class);

    public static int parseArgs(String[] args, BasicConfig conf) {
        Option opt1 = new Option("m", "mode", true, "node mode('dnode' or 'mnode').");
        opt1.setRequired(true);
        Option opt2 = new Option("f", "conf", true, "config filename with path.");
        opt2.setRequired(true);

        Options options = new Options();
        options.addOption(opt1);
        options.addOption(opt2);

        CommandLine cli = null;
        CommandLineParser cliParser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            cli = cliParser.parse(options, args);
        } catch (ParseException e) {
            helpFormatter.printHelp("\n", options);
            //e.printStackTrace();
            return -1;
        }
        
        if(cli.hasOption("m")){
            conf.mode = cli.getOptionValue("m","dnode");
        }
        if(cli.hasOption("f")){
            conf.configFile = cli.getOptionValue("f","");
        }
        return 0;
    }

    public static void main( String[] args ) {

        logger.debug( "TarimServer: Hello Tarim!" );

        BasicConfig bconf = new BasicConfig();
        int ret = parseArgs(args, bconf);
        logger.info("[args] mode: " + bconf.mode + ", conf: " + bconf.configFile);

        //AbstractNode node;
        try{
            if(bconf.mode.equals(BasicConfig.DATANODE)){
                DataNode node = new DataNode(bconf);
                node.init();
                node.start();
                logger.debug( "DataNode end!" );
                node.blockUntilShutdown();
            }else if(bconf.mode.equals(BasicConfig.METANODE)){
                MetaNode node = new MetaNode(bconf);
                node.init();
                node.start();
                node.blockUntilShutdown();
            }else{
                logger.error("[args] unknown mode: " + bconf.mode);
            }
        } catch(InterruptedException e){
            logger.error("InterruptedException error");
        } catch(Exception e){
            logger.error("Exception error");
        }

        logger.debug( "TarimServer stopped!" );

        // 为什么这里会编译报错
        //node.init();
        //node.run();
    }

    public static void testRocksdb() {
        TestRocksDB test = new TestRocksDB();
        test.rocksdbSample();
    }
}
