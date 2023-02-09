package com.deepexi;

import org.apache.commons.cli.*;

import com.deepexi.tarimdb.*;
import com.deepexi.tarimkv.*;
/**
 * TarimServer
 *
 */
public class TarimServer
{
    public static class BasicConfig { // 怎么不使用 static
        public String mode; // data, meta
        public String configFile; 
        public BasicConfig(){
            mode = "data";
            configFile = "";
        }
    }

    public static int parseArgs(String[] args, BasicConfig conf) {
        Option opt1 = new Option("m", "mode", true, "node mode('data' or 'meta').");
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
            conf.mode = cli.getOptionValue("m","data");
        }
        if(cli.hasOption("f")){
            conf.configFile = cli.getOptionValue("f","");
        }
        return 0;
    }

    public static void main( String[] args ) {

        System.out.println( "TarimServer: Hello World!" );

        BasicConfig bconf = new BasicConfig();
        int ret = parseArgs(args, bconf);
        System.out.println("[args] mode: " + bconf.mode + ", conf: " + bconf.configFile);

        DataNode(bconf.configFile); // or MetaNode(bconf.configFile);
    }

    public static int DataNode(String configFile) {

        TarimKV kv = new TarimKV();
        TarimKVMetaClient kvMetaClient = new TarimKVMetaClient();
        TarimDB db = new TarimDB(kv, kvMetaClient);
        kv.init();
        db.run();
        return 0;
    }

    public static int MetaNode(String configFile) {

        TarimKVMeta kvMeta = new TarimKVMeta();
        kvMeta.init();
        kvMeta.run();
        return 0;
    }

    public static void testRocksdb() {
        TestRocksDB test = new TestRocksDB();
        test.rocksdbSample();
    }
}
