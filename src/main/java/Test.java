import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

/**
 * Created by Sophie on 1/24/17.
 */
public class Test {


    public static void main(String[] args) throws IOException {

        // create table
        Configuration conf = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(conf);

        byte[] tableName = Bytes.toBytes("t1");

        byte[] cfName = Bytes.toBytes("f1");


        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor cf = new HColumnDescriptor(cfName);

        cf.setMaxVersions(5);

        table.addFamily(cf);

        admin.createTable(table);


        // open a table
        Configuration connConf = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(connConf);

        HTable openTable = new HTable(TableName.valueOf(tableName), connection);

        // put
        byte[] key = "row1".getBytes();
        byte[] column = Bytes.toBytes("name");
        byte[] value = Bytes.toBytes("upm");

        Put put = new Put(key);

        put.add(cfName, column, value);

        openTable.put(put);

        // get
        Get get = new Get(key);

        // fetch only a column
//        get.addColumn(cfName, column);

        // several versions of this cell value
        get.setMaxVersions(5);

        Result res = openTable.get(get);

        if(res != null && !res.isEmpty()) {
            while(res.advance()) {
                Cell current = res.current();
                // access the value
                CellUtil.cloneValue(current);
                // process res
            }
        }


        byte[] name = res.getValue(cfName, column);

        // delete
        Delete del = new Delete(key);

        openTable.delete(del);


        // scan
        // startKey, endKey
        byte[] startKey = Bytes.toBytes("UCM");
        byte[] endKey = Bytes.toBytes("UPC");
        Scan scan = new Scan(startKey, endKey);

        // filters
        Filter f = new SingleColumnValueFilter(cfName, column,
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("UPM"));

        scan.setFilter(f);

        ResultScanner resScanner = openTable.getScanner(scan);

        Result result = resScanner.next();

        while (result!= null && !result.isEmpty()) {
            // do sth with result
            result = resScanner.next();
        }


        // coprocessors, running user code on region server side.
        // Running logic in the place where data is put.
        // Avoiding sending raw data, hence saving network bandwidth.
        // One is stored procedure alike (Endpoint, for example, max value of a column), user defined functionality.
        // The other is trigger alike (Observer), executed when events occur.

        // exercise

        // check with name key
        Get keyGet = new Get(KeyGenerator.generateKey("Soria", "Romeo", 1));

        Result keyRes = openTable.get(keyGet);

        if(keyRes == null || keyRes.isEmpty()) {
            System.out.println("None.");
        }
        else {
            System.out.println("Yes.");
        }

        // count "all" users
        long count = 0;

        Scan keyScan = new Scan();

        ResultScanner keyResScanner = openTable.getScanner(keyScan);

        Result keyScanRes = keyResScanner.next();

        while (keyScanRes != null && !keyScanRes.isEmpty()) {
            count += 1;
            keyScanRes = keyResScanner.next();
        }


        // range scan
        byte[] startScanKey = KeyGenerator.generateStartKey("Rodriguez");
        byte[] endScanKey = KeyGenerator.generateEndKey("Rodriguez");

        Scan rangeScan = new Scan(startScanKey, endScanKey);

        ResultScanner rangeScanResScanner = openTable.getScanner(rangeScan);

        Result rangeScanRes = rangeScanResScanner.next();

        while(rangeScanRes != null && !rangeScanRes.isEmpty()) {
            // do sth
            rangeScanRes = rangeScanResScanner.next();
        }

        // filter scan
        Filter rangeScanFilter = new SingleColumnValueFilter(cfName, Bytes.toBytes("province"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("ZARAGOZA"));

        rangeScan.setFilter(rangeScanFilter);

        // traverse versions in a cell
        Get multiVersionGet = new Get(KeyGenerator.generateKey("Rodriguez", "Calle", 220));

        multiVersionGet.addColumn(cfName, "Session".getBytes());

        multiVersionGet.setMaxVersions(3);

        Result multiVersionRes = openTable.get(multiVersionGet);

        if(multiVersionRes != null && !multiVersionRes.isEmpty()) {
            CellScanner cellScanner = multiVersionRes.cellScanner();

            while(cellScanner.advance()) {
                Cell cell = cellScanner.current();

                byte[] val = CellUtil.cloneValue(cell);

                System.out.println(Bytes.toLong(val));
             }

        }



    }



}
