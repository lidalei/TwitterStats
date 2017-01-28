package hbaseApp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;


/**
 * Created by Sophie on 1/26/17.
 */
public class TwitterStats {

    // connect to HBase server and create a table
    private static final Configuration CONFIGURATION = HBaseConfiguration.create();

    private static final byte[] TABLE_NAME_BYTES = Bytes.toBytes("twitterStats");

    private static final HTableDescriptor TABLE_DESCRIPTOR = new HTableDescriptor(TableName.valueOf(TABLE_NAME_BYTES));

    public static void main(String[] args) {

        System.out.println("Hello, HBase!");

        // empty arguments
        if(args.length < 1) {
            System.out.println("The arguments cannot be empty. Check the reference!");
            return;
        }

        String mode = args[0];
        if(mode.equals("1") && args.length >= 7) {// mode 1, given time interval and language, find top N most used words

            String zkHost = args[1];
            System.out.println("zkHost:" + zkHost);

            String startTime = args[2];
            System.out.println("startTime:" + startTime);

            String endTime = args[3];
            System.out.println("endTime:" + endTime);

            final int n = Integer.valueOf(args[4]);
            System.out.println("n:" + n);

            String lang = args[5];
            System.out.println("lang:" + lang);

            String outputFolder = args[6];
            System.out.println("outputFolder:" + outputFolder);

        }
        else if(mode.equals("2") && args.length >= 7) {// mode 2, given time interval and a list of languages, find top N words for each language
            String zkHost = args[1];
            System.out.println("zkHost:" + zkHost);

            String startTime = args[2];
            System.out.println("startTime:" + startTime);

            String endTime = args[3];
            System.out.println("endTime:" + endTime);

            final int n = Integer.valueOf(args[4]);
            System.out.println("n:" + n);

            String[] langList = args[5].split(",");
            System.out.println("langList:" + Arrays.toString(langList));

            String outputFolder = args[6];
            System.out.println("outputFolder:" + outputFolder);

        }
        else if(mode.equals("3") && args.length >= 6) {//mode 3, given time interval, find top N words among all languages
            String zkHost = args[1];
            System.out.println("zkHost:" + zkHost);

            String startTime = args[2];
            System.out.println("startTime:" + startTime);

            String endTime = args[3];
            System.out.println("endTime:" + endTime);

            final int n = Integer.valueOf(args[4]);
            System.out.println("n:" + n);

            String outputFolder = args[5];
            System.out.println("outputFolder:" + outputFolder);

        }
        else if(mode.equals("4") && args.length >= 3) {//mode 4, create the table
            String zkHost = args[1];
            System.out.println("zkHost:" + zkHost);

            String dataFolder = args[2];
            System.out.println("dataFolder:" + dataFolder);

            Path dataFolderPath = FileSystems.getDefault().getPath(dataFolder);

            // https://docs.oracle.com/javase/tutorial/essential/io/dirs.html#listdir
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolderPath, "*.log")) {
                HBaseAdmin admin = new HBaseAdmin(CONFIGURATION);

                // TODO, parallel reads
                for (Path entry: stream) {
                    String fileName = entry.getFileName().toString();
                    String fileNameWithoutExt = fileName.substring(0, fileName.indexOf("."));
                    System.out.println(fileNameWithoutExt);

                    // add column family
                    byte[] columnFamilyBytes = Bytes.toBytes(fileNameWithoutExt);

                    HColumnDescriptor cf = new HColumnDescriptor(columnFamilyBytes);

                    TABLE_DESCRIPTOR.addFamily(cf);

                    BufferedReader trendingTopicsReader = Files.newBufferedReader(entry, StandardCharsets.UTF_8);

                    String trendingTopics = null;
                    while((trendingTopics = trendingTopicsReader.readLine()) != null) {
                        // System.out.println(trendingTopics);
                        // TODO, split into timestamp, top 3 frequent hashtags

                    }
                }

                if(admin.tableExists(TABLE_NAME_BYTES)) {
                    admin.disableTable(TABLE_NAME_BYTES);
                    admin.deleteTable(TABLE_NAME_BYTES);
                }
                admin.createTable(TABLE_DESCRIPTOR);
            }
            catch (IOException e) {
                // IOException can never be thrown by the iteration.
                // In this snippet, it can only be thrown by newDirectoryStream.
                System.err.println(e);
            }

        }
        else {
            System.out.println("The value of mode is not valid. Valida values are from 1 to 4.");
        }

    }

}
