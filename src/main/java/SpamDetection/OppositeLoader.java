package SpamDetection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by iosifidis on 09.08.16.
 */
public class OppositeLoader {

    static Logger logger = Logger.getLogger(OppositeLoader.class);
    private final HashMap<String, String> dictionary = new HashMap();

    public OppositeLoader() {
        FileInputStream fstream = null;
        try {
            Path pt=new Path("hdfs://nameservice1/user/iosifidis/opposites.txt");

//            Path pt=new Path("opposites.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

            String strLine;

            while ((strLine = br.readLine()) != null) {

                dictionary.put(strLine.split(",")[0], strLine.split(",")[1]);
                dictionary.put(strLine.split(",")[1], strLine.split(",")[0]);
            }
//            logger.info(dictionary);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, String> getDictionary() {
        return dictionary;
    }
}
