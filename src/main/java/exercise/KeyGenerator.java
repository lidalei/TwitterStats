package exercise;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Sophie on 1/24/17.
 */
public class KeyGenerator {

    // define key
    public static byte[] generateKey(String lastName, String firstName, int id) {
        byte[] key = new byte[44];

        System.arraycopy(Bytes.toBytes(lastName), 0, key, 0, lastName.length());

        System.arraycopy(Bytes.toBytes(firstName), 0, key, 20, firstName.length());

        System.arraycopy(Bytes.toBytes(id), 0, key, 40, 4);

        return key;
    }

    // generate start key
    public static byte[] generateStartKey(String lastName) {
        byte[] key = new byte[44];

        System.arraycopy(Bytes.toBytes(lastName), 0, key, 0, lastName.length());

        for(int i = 20; i < 44; ++i) {
            key[i] = (byte)-255;
        }

        return key;
    }

    // generate end key
    public static byte[] generateEndKey(String lastName) {
        byte[] key = new byte[44];

        System.arraycopy(Bytes.toBytes(lastName), 0, key, 0, lastName.length());

        for(int i = 20; i < 44; ++i) {
            key[i] = (byte)255;
        }

        return key;
    }
}
