package edu.sfsu;

import java.util.concurrent.atomic.AtomicInteger;

public class Tester {

    private static int MAX_INSERT_THREADS = 200;
    private static int MAX_COMPARE_THREADS = 5;
    private static int KEY_LENGTH = 5;
    private static int VALUE_LENGTH = 20;

    private String[] keys = new String[MAX_INSERT_THREADS];
    private String[] values = new String[MAX_INSERT_THREADS];
    private ConcurrentHashTable<String, String> table = ConcurrentHashTable.create(10);
    private AtomicInteger mismatch_count =  new AtomicInteger(0);


    /**
     * Searches for a given key.
     */
    class SearchThread extends Thread {
        String key;
        String expectedValue;

        public SearchThread(String key, String expectedValue) {
            this.key = key;
            this.expectedValue = expectedValue;
        }

        @Override
        public void run() {
            System.out.println("Searching for " + key);
            String databaseValue = table.search(key);
            if (!databaseValue.equals(expectedValue)) {
                System.out.println(String.format(
                        "Mismatch found. Key: %s Expected: %s Found: %s",
                        key,
                        expectedValue,
                        databaseValue
                ));
                mismatch_count.addAndGet(1);
            }
        }
    }

    /**
     * Inserts strings into the table.
     */
    class InsertThread extends Thread {
        String key;
        String value;

        public InsertThread(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            System.out.println(String.format("Inserting key: %s value: %s",
                    key, value));
            table.insert(key, value);
        }
    }

    void runTest() throws InterruptedException {
        Thread insertThreads[] = new Thread[MAX_INSERT_THREADS];
        Thread searchThreads[] = new Thread[MAX_INSERT_THREADS];

        // Insert random strings
        for (int i = 0; i < MAX_INSERT_THREADS; i++) {
            keys[i] = new RandomString(KEY_LENGTH).nextString();
            values[i] = new RandomString(VALUE_LENGTH).nextString();
            insertThreads[i] = new InsertThread(keys[i], values[i]);
            insertThreads[i].run();
        }

        // Join on the inserts
        for (int i = 0; i < MAX_INSERT_THREADS; i++) {
            insertThreads[i].join();
        }

        // Run the comparisons
        for (int i = 0; i < MAX_INSERT_THREADS; ) {
            for (int j = 0; j < MAX_COMPARE_THREADS && i < MAX_INSERT_THREADS; i++, j++) {
                String key = keys[i];
                String expectedValue = values[i];
                searchThreads[i] = new SearchThread(key, expectedValue);
                searchThreads[i].run();
            }
        }
        for (int i = 0; i < MAX_COMPARE_THREADS; i++) {
            searchThreads[i].join();
        }

        System.out.println(String.format("Process ended. %d mismatches found", mismatch_count.get()));
    }

    public static void main(String[] args) throws Exception {
        Tester tester = new Tester();
        tester.runTest();
    }
}