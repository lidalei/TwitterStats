package hbaseApp;

import master2016.StreamTopK;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.Map.Entry;


/**
 * Created by Sophie on 1/26/17.
 */
public class TwitterStats {

    // connect to HBase server and create a table
    private static final Configuration CONFIGURATION = HBaseConfiguration.create();

    private static final byte[] TABLE_NAME_BYTES = Bytes.toBytes("twitterStats");

    private static final HTableDescriptor TABLE_DESCRIPTOR = new HTableDescriptor(TableName.valueOf(TABLE_NAME_BYTES));

    // TODO, change to out
    private static final String DATA_GLOB = "*.log";

    private static final String[] ALL_LANGUAE_LIST = {"all"};

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


    private static String getOutputFileName(String queryMode) {
        return "21_query" + queryMode + ".out";
    }


    /**
     *
     * Write top N hashtags to files
     * @param outputFolder, output folder String
     * @param queryMode
     * @param langHashtagFreqMap, lang maps to top N hashtag and frequency
     * @param startTs
     * @param endTs
     */
    private static void writeToFiles(String outputFolder, String queryMode, HashMap<String, List<Entry<String, Integer>>> langHashtagFreqMap, String startTs, String endTs) {
        try{
            // create if the output folder does not exist
            Path outputFolderPath = FileSystems.getDefault().getPath(outputFolder);
            if(!Files.isDirectory(outputFolderPath)) {
                Files.createDirectory(outputFolderPath);
            }

            // create file
            Path resFilePath = FileSystems.getDefault().getPath(outputFolder, getOutputFileName(queryMode));
            BufferedWriter resWriter = Files.newBufferedWriter(resFilePath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND, StandardOpenOption.WRITE);

            // write content
            for(String lang : langHashtagFreqMap.keySet()) {
                List<Entry<String, Integer>> topK = langHashtagFreqMap.get(lang);
                StringBuilder strBuilder = new StringBuilder(topK.size() * 40);
                if(!queryMode.equals("3")) {
                    strBuilder.append(lang + ",");
                }

                for(int i = 1; i <= topK.size(); ++i) {
                    strBuilder.append(i + "," + topK.get(i - 1).getKey() + ",");
                }
                strBuilder.append(startTs + ",").append(endTs);
                resWriter.write(strBuilder.toString());
                resWriter.newLine();
            }

            resWriter.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Fatal error: " + e.getMessage() + ". To terminate the program.");
        }
        catch (IOException e) {
            e.printStackTrace();
            System.out.println("Fatal error: " + e.getMessage() + ". To terminate the program.");
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

        // mode 1, given time interval and language, find top N most used words.
        // mode 2, given time interval and a list of languages, find top N words for each language.
        // mode 3, given time interval, find top N words among all languages.
        if(((mode.equals("1") || mode.equals("2")) && args.length >= 7) || (mode.equals("3") && args.length >= 6)) {
            String zkHost = args[1];
            System.out.println("zkHost:" + zkHost);
            // TODO, use zkHost

            byte[] startTsBytes = Bytes.toBytes(args[2]);
            System.out.println("startTimestamp:" + new String(startTsBytes, StandardCharsets.UTF_8));

            byte[] endTsBytes = Bytes.toBytes(args[3]);
            System.out.println("endTimestamp:" + new String(endTsBytes, StandardCharsets.UTF_8));

            if(Bytes.toString(startTsBytes).compareTo(Bytes.toString(endTsBytes)) > 0) {
                System.out.println("Star time cannot be later than end time.");
                return;
            }

            final int n = Integer.valueOf(args[4]);
            System.out.println("n:" + n);

            String[] langList = mode.equals("3") ? ALL_LANGUAE_LIST : args[5].split(",");

            // transform to List of byte[]
            ArrayList<byte[]> langBytesList = new ArrayList<>(langList.length);
            HashMap<String, StreamTopK> streamTopKs = new HashMap<>(langList.length * 2);
            for(String lang : langList) {
                langBytesList.add(Bytes.toBytes(lang));
                streamTopKs.put(lang, new StreamTopK(n));
            }

            System.out.println("langList:");
            for(byte[] langBytes : langBytesList) {
                System.out.println(Bytes.toString(langBytes));
            }

            String outputFolder = mode.equals("3") ? args[5] : args[6];
            System.out.println("outputFolder:" + outputFolder);

            try{
                HConnection conn = HConnectionManager.createConnection(CONFIGURATION);
                HTable table = new HTable(TableName.valueOf(TABLE_NAME_BYTES), conn);

                Scan scan = new Scan(startTsBytes, endTsBytes);

                if(!mode.equals("3")) {
                    for(byte[] langBytes : langBytesList) {
                        scan.addFamily(langBytes);
                    }
                }

                ResultScanner resScanner = table.getScanner(scan);

                try{
                    for(Result res : resScanner) {
                        NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfColValMap = res.getNoVersionMap();

                        Set<byte[]> langBytesSet = cfColValMap.keySet();
                        for(byte[] langBytes : langBytesSet) {
                            for(int i = 0; i < 3; ++i) {
                                String hashtag = Bytes.toString(cfColValMap.get(langBytes).get(topHashTagBytes(i + 1)));
                                if(hashtag != null) {
                                    // transform to string first and get int value later to avoid IllegalArgumentException
                                    int freq = Integer.valueOf(Bytes.toString(cfColValMap.get(langBytes).get(topHashTagFreqBytes(i + 1))));

                                    StreamTopK streamTopK = mode.equals("3") ? streamTopKs.get(ALL_LANGUAE_LIST[0]) : streamTopKs.get(Bytes.toString(langBytes));
                                    streamTopK.add(hashtag, freq);
                                }
                            }
                        }
                    }
                }
                finally {
                    resScanner.close();
                }

                // write to files
                HashMap<String, List<Entry<String, Integer>>> dataToWrite = new HashMap<>(langList.length * 2);
                for(String lang : streamTopKs.keySet()) {
                    dataToWrite.put(lang, streamTopKs.get(lang).topk());

                    // TODO, remove after finishing developemnt
                    System.out.println(lang + streamTopKs.get(lang).topk());
                }
                writeToFiles(outputFolder, mode, dataToWrite, Bytes.toString(startTsBytes), Bytes.toString(endTsBytes));

                conn.close();
            }
            catch (IOException e) {
                System.out.println("IO error while creating a connection.");
            }
        }
        else if(mode.equals("4") && args.length >= 3) {//mode 4, create the table
            String zkHost = args[1];
            System.out.println("zkHost:" + zkHost);

            String dataFolder = args[2];
            System.out.println("dataFolder:" + dataFolder);

            Path dataFolderPath = FileSystems.getDefault().getPath(dataFolder);

            // Create schema, https://docs.oracle.com/javase/tutorial/essential/io/dirs.html#listdir
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolderPath, DATA_GLOB)) {
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
