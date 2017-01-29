package hbaseApp;

import master2016.StreamTopK;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
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

    private static final byte[] TOP_HASH_TAG1_BYTES = Bytes.toBytes("topHashTag1");
    private static final byte[] TOP_HASH_TAG2_BYTES = Bytes.toBytes("topHashTag2");
    private static final byte[] TOP_HASH_TAG3_BYTES = Bytes.toBytes("topHashTag3");

    private static final byte[] TOP_HASH_TAG1_FREQ_BYTES = Bytes.toBytes("topHashTag1Freq");
    private static final byte[] TOP_HASH_TAG2_FREQ_BYTES = Bytes.toBytes("topHashTag2Freq");
    private static final byte[] TOP_HASH_TAG3_FREQ_BYTES = Bytes.toBytes("topHashTag3Freq");


    private static byte[] topHashTagBytes(int i) {
        switch(i) {
            case 1:
                return TOP_HASH_TAG1_BYTES;
            case 2:
                return TOP_HASH_TAG2_BYTES;
            case 3:
                return TOP_HASH_TAG3_BYTES;
            default:
                return Bytes.toBytes("topHashTag" + i);
        }
    }

    private static byte[] topHashTagFreqBytes(int i) {
        switch(i) {
            case 1:
                return TOP_HASH_TAG1_FREQ_BYTES;
            case 2:
                return TOP_HASH_TAG2_FREQ_BYTES;
            case 3:
                return TOP_HASH_TAG3_FREQ_BYTES;
            default:
                return Bytes.toBytes("topHashTag" + i + "Freq");
        }
    }



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

            byte[] startTsBytes = Bytes.toBytes(args[2]);
            System.out.println("startTimestamp:" + new String(startTsBytes, StandardCharsets.UTF_8));

            byte[] endTsBytes = Bytes.toBytes(args[3]);
            System.out.println("endTimestamp:" + new String(endTsBytes, StandardCharsets.UTF_8));

            final int n = Integer.valueOf(args[4]);
            System.out.println("n:" + n);

            byte[] langBytes = Bytes.toBytes(args[5]);
            System.out.println("lang:" + new String(langBytes, StandardCharsets.UTF_8));

            String outputFolder = args[6];
            System.out.println("outputFolder:" + outputFolder);

            StreamTopK streamTopK = new StreamTopK(n, 10000);

            try{
                HConnection conn = HConnectionManager.createConnection(CONFIGURATION);
                HTable table = new HTable(TableName.valueOf(TABLE_NAME_BYTES), conn);

                Scan scan = new Scan(startTsBytes, endTsBytes);

                scan.addFamily(langBytes);

                ResultScanner resScanner = table.getScanner(scan);

                try{
                    // TODO, deal with hashtags
                    for(Result res : resScanner) {
                        // System.out.println("row: " + res);
                        res.getMap();
                    }
                }
                finally {
                    resScanner.close();
                }

                conn.close();

            }
            catch (IOException e) {
                System.out.println("IO error while creating a connection.");
            }


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

            // Create schema, https://docs.oracle.com/javase/tutorial/essential/io/dirs.html#listdir
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolderPath, "*.log")) {
                HBaseAdmin admin = new HBaseAdmin(CONFIGURATION);

                for (Path entry: stream) {
                    String fileName = entry.getFileName().toString();
                    String fileNameWithoutExt = fileName.substring(0, fileName.indexOf("."));
                    System.out.println(fileNameWithoutExt);

                    // add column family
                    byte[] columnFamilyBytes = Bytes.toBytes(fileNameWithoutExt);
                    HColumnDescriptor cf = new HColumnDescriptor(columnFamilyBytes);
                    TABLE_DESCRIPTOR.addFamily(cf);
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

            // add data, store top 3 hashtags into different columns of a column family (language).
            // In fact, storing them as different versions might be a good idea. Let's stick to this now.
            // TODO, parallel reads.
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolderPath, "*.log")) {
                HConnection conn = HConnectionManager.createConnection(CONFIGURATION);
                HTable table = new HTable(TableName.valueOf(TABLE_NAME_BYTES), conn);

                for (Path entry: stream) {
                    String fileName = entry.getFileName().toString();
                    String fileNameWithoutExt = fileName.substring(0, fileName.indexOf("."));
                    System.out.println(fileNameWithoutExt);

                    // add column family
                    byte[] columnFamilyBytes = Bytes.toBytes(fileNameWithoutExt);

                    BufferedReader trendingTopicsReader = Files.newBufferedReader(entry, StandardCharsets.UTF_8);

                    String trendingTopics = null;
                    while((trendingTopics = trendingTopicsReader.readLine()) != null) {
                        // System.out.println(trendingTopics);
                        // split into timestamp, top 3 frequent hashtags
                        String[] topicsArr = trendingTopics.split(",");
                        byte[] tsKey = Bytes.toBytes(topicsArr[0]);

                        Put put = new Put(tsKey);

                        for(int i = 0; (3 + (i << 1)) < topicsArr.length; ++i) {
                            byte[] hashTag = Bytes.toBytes(topicsArr[2 + (i << 1)]);
                            byte[] freq = Bytes.toBytes(topicsArr[3 + (i << 1)]);

                            // add column and value
                            put.add(columnFamilyBytes, topHashTagBytes(i + 1), hashTag);
                            put.add(columnFamilyBytes, topHashTagFreqBytes(i + 1), freq);
                        }

                        table.put(put);
                    }
                }
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
