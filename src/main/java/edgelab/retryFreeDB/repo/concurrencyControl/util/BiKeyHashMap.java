package edgelab.retryFreeDB.repo.concurrencyControl.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BiKeyHashMap<K1, K2, V> {
    private final Map<K1, List<Entry<K1, K2, V>>> map1 = new HashMap<>();
    private final Map<K2, Entry<K1, K2, V>> map2 = new HashMap<>();

    public boolean containsResource(K2 key2) {
        return map2.containsKey(key2);
    }

    // Internal Entry class to store key1, key2, and value
    private static class Entry<K1, K2, V> {
        K1 key1;
        K2 key2;
        V value;

        Entry(K1 key1, K2 key2, V value) {
            this.key1 = key1;
            this.key2 = key2;
            this.value = value;
        }
    }

    // Add an entry with two keys
    public synchronized void put(K1 key1, K2 key2, V value) {
//        tx1, r1, v1, tx1, r2, v2, tx2, r1, v2, tx3, r1, v2
//        tx1 -> tx1, r2, v2 - tx2 ->
//        r1 -> tx2, r1, v2, r2 -> tx1, r2, v2


        if (map2.containsKey(key2)) {
            map1.get(map2.get(key2).key1).remove(map2.get(key2));
        }

        Entry<K1, K2, V> entry = new Entry<>(key1, key2, value);
        map1.putIfAbsent(key1, new ArrayList<>());
        map1.get(key1).add(entry);

        map2.put(key2, entry);
    }


    // Retrieve an entry by the second key
    public V getByResource(K2 key2) {
        Entry<K1, K2, V> entry = map2.get(key2);
        return (entry != null) ? entry.value : null;
    }

    // Remove an entry by the first key
    public void removeByTransaction(K1 key1) {
        List<Entry<K1, K2, V>> entries = map1.remove(key1);
        if (entries != null) {
            for (Entry<K1, K2, V> entry :
                    entries) {
                map2.remove(entry.key2);
            }
        }
    }

}
