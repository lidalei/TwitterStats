package master2016;

import java.util.*;
import java.util.Map.Entry;

/**
 * Created by Sophie on 12/10/16.
 */
public class StreamTopK {

    private int k = 3;
    // create a hash map to store <term, frequency> pairs
    private HashMap<String, Integer> counters = null;


    public StreamTopK(int k, int initialCapacity) {
        if(k <= 0) {
            this.k = 3;
        }
        else {
            this.k = k;
        }

        if(initialCapacity <= 0) {
            this.counters = new HashMap<>(10000);
        }
        else {
            this.counters = new HashMap<>(initialCapacity);
        }

    }

    public StreamTopK(int k) {
        this(k,10000);
    }

    public StreamTopK() {
        this(3);
    }


    /**
     * Add the term into counters or increase its associative frequency
     * @param term
     */
    public void add(String term) {
        add(term, 1);
    }


    /**
     * add a term's frequency, when it is not in counters, create the <key, frequency>
     * @param term
     * @param frequency
     */
    public void add(String term, int frequency) {
        if(counters.containsKey(term)) {
            counters.put(term, counters.get(term) + frequency);
        }
        else {
            counters.put(term, frequency);
        }
    }

    /**
     * return terms with top k frequencies
     * @return
     */
    public List<Entry<String, Integer>> topk() {
        List<Entry<String,Integer>> countersAsList = new LinkedList<>(counters.entrySet());

        // sort the list in terms of their values in descending order!!!
        Collections.sort(countersAsList, new Comparator<Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                // if the values are the same, sort by key in ascending order
                int compare = o1.getValue().compareTo(o2.getValue());
                if(compare == 0) {
                    return o1.getKey().compareTo(o2.getKey());
                }
                // otherwise sort by values in descending order
                return -compare;
            }
        });

        if(k > countersAsList.size()) {
            // fromIndex until endIndex
            return countersAsList;
        }
        else {
            // fromIndex until endIndex
            return countersAsList.subList(0, k);
        }
    }

    public void clear() {
        counters.clear();
    }
}