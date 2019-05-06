/*
Kevin Baltazar Reyes
CSC 415
SFSU Spring 2019
 */

package edu.sfsu;

import java.util.HashMap;
import java.util.concurrent.Semaphore;

public class ConcurrentHashTable<K, V> {

    // Class member variables.
    Semaphore mutex = new Semaphore(1); //used for reader and write
    Semaphore write = new Semaphore(1); //used just for writer
    int read_count = 0;

    // TODO: Implement. Use a java.util.HashMap as the underlying hash table.
    HashMap<K, V> javaMap;

    // Public methods

    /**
     * Creates a new ConcurrentHashTable.
     *
     * @param initialSize the initial size of the table.
     */
    public static ConcurrentHashTable create(int initialSize) {
        return new ConcurrentHashTable(initialSize);
    }

    /**
     * Inserts a new value in the hash table.
     */
    public void insert(K key, V value) {
        // TODO: Implement. Writer
        try {
            write.acquire();
//            System.out.println("Write semaphore acquired for insert operator");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        javaMap.put(key, value);
        write.release();
    }

    /**
     * Looks for a value with key k. Return null if the value does not exist.
     */
    public V search(K k) {
        // TODO: Implement. Reader
        if (javaMap.containsKey(k)) {
            try {
                mutex.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            read_count++;
            if (read_count == 1) {
                try {
                    write.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            mutex.release();

            V valueReturned = javaMap.get(k);   //reading is performed

            try {
                mutex.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            read_count--;
            if (read_count == 0) {  //makes sure that read count gets back to zero before releasing the write semaphore
                write.release();
            }
            mutex.release();

            //            System.out.println("The value that is returned is: " + javaMap.get(k));
            return valueReturned;
        } else {
            return null;
        }

    }

    /**
     * Deletes the value/key par for k, or does nothing if the key does not exist.
     *
     * @param k
     */
    public void delete(K k) {
        // TODO: Implement. Writer
        if (javaMap.containsKey(k)) {
            try {
                write.acquire();
//            System.out.println("Write semaphore acquired for insert operator");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            javaMap.remove(k);
            write.release();
        }
    }

    // Private methods

    /**
     * Private constructor.
     */
    private ConcurrentHashTable(int initialSize) {
        // TODO: Implement
        javaMap = new HashMap<K, V>(initialSize);

    }


}