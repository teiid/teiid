/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.buffer.storage.file;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.core.util.UnitTestUtil;

/**
 * Test out that fileStorageAreas can be
 * 1) created
 * 2) data can be added
 * 3) data can be retrieved
 * 4) data read is same as data written
 * 5) Storage area can be removed.
 *
 * This test requires a MetaMatrix Repository is available and valid.
 * Install MM, run setupMM, run mmenv from dos prompt and run test. 
 */
public class TestFileStorageManager extends TestCase {
	
	public TestFileStorageManager(String name)	{
		super(name);
	}
	
	// Set up the fixture for this testcase: the tables for this test.
	public StorageManager getStorageManager(String MAX_FILE_SIZE) {
        try {
            Properties resourceProps = new Properties();
            resourceProps.put(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, UnitTestUtil.getTestScratchPath());
            if (MAX_FILE_SIZE != null) {
                resourceProps.put(BufferManagerPropertyNames.MAX_FILE_SIZE, MAX_FILE_SIZE);
            }
            StorageManager sm = new FileStorageManager();
            sm.initialize(resourceProps);
            return sm;
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failure during storage manager initialization: " + e.getMessage()); //$NON-NLS-1$
            
            // won't be called
            return null;
        }
	}
    
    public static TupleBatch exampleBatch(int begin, int end) {
        int count = end-begin+1;
        List[] rows = new List[count];
        for(int i=0; i < count; i++) {
            rows[i] = new ArrayList();
            rows[i].add(new Integer(i+begin));
            rows[i].add("" + (i+begin));     //$NON-NLS-1$
        }
        return new TupleBatch(begin, rows);        
    }

    private static byte[] convertToBytes(Object object) throws IOException {
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);

            oos.writeObject(object);
            return baos.toByteArray();
        } finally {
            if(oos != null) {
                try {
                    oos.close();
                } catch(IOException e) {
                }
            }
        }
    }

    /**
     * Get the tuple batch with the number of rows that will fit inside of a byte[] of the given length
     * after it has been serialized. This algorithm is very closely tied to the implementation of exampleBatch()
     * and ObjectOutputStream, so any changes to either will make this method and all related tests unusable,
     * unless it is changed to reflect the new sizes
     * @param bytes
     * @return
     * @since 4.2
     */
    public static TupleBatch exampleBatchThatFitsInBytes(int bytes, int startRow) {
        int rows = 0;
        if (bytes < 343) {
            rows = 1;
        } else {
            int currentBytes = 343;
            rows = 2;
            while(true) {
                currentBytes+= 31 + Math.floor(Math.log(rows + 1)/Math.log(10) /*log base-10 of rows */); 
                if (currentBytes > bytes) {
                    break;
                }
                rows++;
            }
        }
        return exampleBatch(startRow, startRow + rows - 1);
    }
    
    public static void helpCompareBatches(TupleBatch expectedBatch, TupleBatch actualBatch) {
        List[] expectedRows = expectedBatch.getAllTuples();
        List[] actualRows = actualBatch.getAllTuples();

        assertEquals("Differing number of rows ", expectedRows.length, actualRows.length); //$NON-NLS-1$
        for(int i=0; i<expectedRows.length; i++) {
            assertEquals("Differing rows at " + i, expectedRows[i], actualRows[i]);     //$NON-NLS-1$
        }
    }

    public void helpTestMultiThreaded(int OPEN_FILES, int NUM_THREADS, int NUM_BATCHES, int BATCH_SIZE) {
        Properties resourceProps = new Properties();        
        String nonExistentDirectory = UnitTestUtil.getTestScratchPath() + File.separator + "testMultiThread"; //$NON-NLS-1$
        resourceProps.put(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, nonExistentDirectory);
        resourceProps.put(BufferManagerPropertyNames.MAX_OPEN_FILES, "" + OPEN_FILES); //$NON-NLS-1$
        final StorageManager sm = getStorageManager(null);
        try {
            sm.initialize(resourceProps);
        } catch(MetaMatrixComponentException e) {
            fail("Unexpected exception during initialization: " + e.getMessage()); //$NON-NLS-1$
        }
        
        // Create threads
        AddGetWorker[] threads = new AddGetWorker[NUM_THREADS];
        for(int i=0; i<threads.length; i++) {
            threads[i] = new AddGetWorker(sm, new TupleSourceID("local,1:" + i), NUM_BATCHES, BATCH_SIZE); //$NON-NLS-1$
        }
        
        // Run the threads
        for(int i=0; i<threads.length; i++) {
            threads[i].start();    
        }        
        
        // Recover
        for(int i=0; i<threads.length; i++) {
            while(true) {
                try {
                    threads[i].join();                          
                    break;
                } catch(InterruptedException e) {
                }
            } 
        }        
        
        // Check for errors
        for(int i=0; i<threads.length; i++) {
            if(threads[i].assertion != null) { 
                throw threads[i].assertion;
            } else if(threads[i].error != null) {
                threads[i].error.printStackTrace();
                fail("Thread " + i + " got an error: " + threads[i].error.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
            }
        }

        // Check open file count, should be 0
        assertEquals("Open file count should be 0 after test", 0, ((FileStorageManager)sm).getOpenFiles());  //$NON-NLS-1$
                
        // Clean up
        sm.shutdown();
        
    }

    public void helpTestRandomThreads(int OPEN_FILES, int NUM_THREADS, int NUM_BATCHES, int BATCH_SIZE, int RANDOM_OPS) {
        Properties resourceProps = new Properties();        
        String nonExistentDirectory = UnitTestUtil.getTestScratchPath() + File.separator + "testMultiThread"; //$NON-NLS-1$
        resourceProps.put(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, nonExistentDirectory);
        resourceProps.put(BufferManagerPropertyNames.MAX_OPEN_FILES, "" + OPEN_FILES); //$NON-NLS-1$
        final StorageManager sm = getStorageManager(null);
        try {
            sm.initialize(resourceProps);
        } catch(MetaMatrixComponentException e) {
            fail("Unexpected exception during initialization: " + e.getMessage()); //$NON-NLS-1$
        }
        
        // Create threads
        RandomAccessWorker[] threads = new RandomAccessWorker[NUM_THREADS];
        for(int i=0; i<threads.length; i++) {
            threads[i] = new RandomAccessWorker(sm, new TupleSourceID("local,1:" + i), NUM_BATCHES, BATCH_SIZE, RANDOM_OPS); //$NON-NLS-1$
        }
        
        // Run the threads
        for(int i=0; i<threads.length; i++) {
            threads[i].start();    
        }        
        
        // Recover
        for(int i=0; i<threads.length; i++) {
            while(true) {
                try {
                    threads[i].join();                          
                    break;
                } catch(InterruptedException e) {
                }
            } 
        }        
        
        // Check for errors
        for(int i=0; i<threads.length; i++) {
            if(threads[i].assertion != null) { 
                throw threads[i].assertion;
            } else if(threads[i].error != null) {
                threads[i].error.printStackTrace();
                fail("Thread " + i + " got an error: " + threads[i].error.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
            }
        }

        // Check open file count, should be 0
        assertEquals("Open file count should be 0 after test", 0, ((FileStorageManager)sm).getOpenFiles());  //$NON-NLS-1$
                
        // Clean up
        sm.shutdown();
        
    }

    public void testAddGetBatch1() {
        StorageManager sm = getStorageManager(null);        
        TupleSourceID tsID = new TupleSourceID("local,1:0");     //$NON-NLS-1$
        TupleBatch batch = exampleBatch(1, 10);
        try {
            // Add one batch
            sm.addBatch(tsID, batch, null);
            
            // Get that batch
            TupleBatch actual = sm.getBatch(tsID, batch.getBeginRow(), null);                
            helpCompareBatches(batch, actual);
            
            // Remove the batches
            sm.removeBatches(tsID);
            
        } catch(MetaMatrixException e) {
            fail("Unexpected exception of type " + e.getClass().getName() + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testAddGetBatch2() {
        StorageManager sm = getStorageManager(null);
        TupleSourceID tsID = new TupleSourceID("local,1:0");     //$NON-NLS-1$
        
        int numBatches = 20;
        int batchSize = 100;
        
        try {
            // Add batches
            for(int i=0; i<numBatches; i++) {
                int begin = (i*batchSize)+1;
                int end = begin + batchSize - 1;
                TupleBatch batch = exampleBatch(begin, end);
                sm.addBatch(tsID, batch, null);                    
            }

            // Get batches
            for(int i=0; i<numBatches; i++) {
                int begin = (i*batchSize)+1;
                int end = begin + batchSize - 1;
                TupleBatch actual = sm.getBatch(tsID, begin, null);                
                helpCompareBatches(exampleBatch(begin, end), actual);
            }
 
            // Remove the batches
            sm.removeBatches(tsID);
           
        } catch(MetaMatrixException e) {
            fail("Unexpected exception of type " + e.getClass().getName() + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testCreateNewDirectory() {
        Properties resourceProps = new Properties();
        
        String nonExistentDirectory = UnitTestUtil.getTestScratchPath() + File.separator + "GONZO"; //$NON-NLS-1$
        resourceProps.put(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, nonExistentDirectory);
        StorageManager sm = new FileStorageManager();
        
        try { 
            sm.initialize(resourceProps);
            
            File file = new File(nonExistentDirectory);
            assertTrue("Directory doesn't exist", file.exists()); //$NON-NLS-1$
            assertTrue("Directory was created as a file", file.isDirectory()); //$NON-NLS-1$
            
        } catch(Exception e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testAddTwice() {
        StorageManager sm = getStorageManager(null);
        TupleSourceID tsID = new TupleSourceID("local,1:0");     //$NON-NLS-1$
        TupleBatch batch = exampleBatch(1, 20);
        
        try {
            // Add batch
            sm.addBatch(tsID, batch, null);
            
            // Remove batch (does nothing)
            sm.removeBatch(tsID, batch.getBeginRow());
            
            // Add batch again
            sm.addBatch(tsID, batch, null);
           
        } catch(MetaMatrixException e) {
            fail("Unexpected exception of type " + e.getClass().getName() + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } finally { 
            try {
                sm.removeBatches(tsID);    
            } catch(Exception e) {
            }
        }
    }
    
    // Test with more open files than number of threads
    public void testMultiThreaded1() {
        helpTestMultiThreaded(6, 5, 5, 100);
    }

    // Test with fewer open files than number of threads
    public void testMultiThreaded2() {
        helpTestMultiThreaded(1, 2, 5, 100);
    }

    // Test 1 random thread
    public void testRandomThreads1() {
        helpTestRandomThreads(10, 1, 5, 20, 30);
    }

    // Test several random threads
    public void testRandomThreads2() {
        helpTestRandomThreads(10, 5, 5, 20, 30);
    }

    // Test several random threads, low open files
    public void testRandomThreads3() {
        helpTestRandomThreads(2, 4, 5, 20, 30);
    }
    
    public void tstBatchSizeHelper() throws Exception {
//        for (int i = 1; i < 500; i++) {
//            System.out.println(i + ":" + convertToBytes(exampleBatch(1, i)).length);
//        }
//        for (int i = 100; i < 1024; i++) {
//            System.out.println("Fits in " + i + ":" + exampleBatchThatFitsInBytes(i, 1));
//        }
        for (int i = 343; i < 512000; i++) {
            TupleBatch batch = exampleBatchThatFitsInBytes(i, 1);
            byte[] bytes = convertToBytes(batch);
            assertTrue("Example test fails. Size for an exampleBatch under " + i + " bytes is too big: " + bytes.length, bytes.length <= i); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
    public void testCreatesSpillFiles() throws Exception {
        StorageManager sm = getStorageManager("1"); // 1KB //$NON-NLS-1$
        TupleSourceID tsID = new TupleSourceID("local,1:0");     //$NON-NLS-1$
        TupleBatch batch1 = exampleBatchThatFitsInBytes(1024 * 1024, 1);
        TupleBatch batch2 = exampleBatchThatFitsInBytes(200 * 1024, batch1.getEndRow() + 1);
        TupleBatch batch3 = exampleBatchThatFitsInBytes(2 * 1024 * 1024, batch2.getEndRow() + 1);
        sm.addBatch(tsID, batch1, null); // Fill up the first file
        sm.addBatch(tsID, batch2, null); // Should create a new file
        sm.addBatch(tsID, batch3, null); // Should create a new file
        
        helpCompareBatches(exampleBatch(batch1.getBeginRow(), batch1.getEndRow()), sm.getBatch(tsID, batch1.getBeginRow(), null));
        helpCompareBatches(exampleBatch(batch2.getBeginRow(), batch2.getEndRow()), sm.getBatch(tsID, batch2.getBeginRow(), null));
        helpCompareBatches(exampleBatch(batch3.getBeginRow(), batch3.getEndRow()), sm.getBatch(tsID, batch3.getBeginRow(), null));

        File storageFile1 = new File(UnitTestUtil.getTestScratchPath(), "b_" + tsID.getIDValue() + "_0"); //$NON-NLS-1$ //$NON-NLS-2$
        File storageFile2 = new File(UnitTestUtil.getTestScratchPath(), "b_" + tsID.getIDValue() + "_1"); //$NON-NLS-1$ //$NON-NLS-2$
        File storageFile3 = new File(UnitTestUtil.getTestScratchPath(), "b_" + tsID.getIDValue() + "_2"); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(storageFile1.exists());
        assertTrue(storageFile2.exists());
        assertTrue(storageFile3.exists());
        assertEquals(1048435, storageFile1.length());
        assertEquals(211806, storageFile2.length());
        assertEquals(2108106, storageFile3.length());
        sm.removeBatches(tsID);
        
        assertFalse(storageFile1.exists());
        assertFalse(storageFile2.exists());
        assertFalse(storageFile3.exists());
    }
    
    private static class AddGetWorker extends Thread {        
        private StorageManager sm;
        private TupleSourceID tsID;
        private int numBatches = 0;
        private int batchSize = 0;

        public AssertionFailedError assertion;
        public Throwable error;
        
        public AddGetWorker(StorageManager sm, TupleSourceID id, int numBatches, int batchSize) {
            this.sm = sm;
            this.tsID = id;            
            this.numBatches = numBatches;
            this.batchSize = batchSize;
        }
        
        public void run() {
            //System.out.println(tsID.toString() + ": starting");
            try {
                // Add batches in order
                for(int i=0; i<numBatches; i++) {
                    int begin = (i*batchSize)+1;
                    int end = begin + batchSize - 1;
                    TupleBatch batch = exampleBatch(begin, end);
                    //System.out.println(tsID.toString() + ": adding batch " + batch);            
                    sm.addBatch(tsID, batch, null);                    
                }
    
                // Get batches in order
                for(int i=0; i<numBatches; i++) {
                    int begin = (i*batchSize)+1;
                    int end = begin + batchSize - 1;
                    //System.out.println(tsID.toString() + ": getting batch " + begin);            
                    TupleBatch actual = sm.getBatch(tsID, begin, null);                
                    TestFileStorageManager.helpCompareBatches(
                        TestFileStorageManager.exampleBatch(begin, end), 
                        actual);
                }
     
            } catch(AssertionFailedError e) {
                this.assertion = e;               
            } catch(Throwable e) {
                this.error = e;
            } finally {
                // Remove all the batches
                //System.out.println(tsID.toString() + ": removing batches");   
                try {          
                    sm.removeBatches(tsID);    
                } catch(MetaMatrixComponentException e) {
                }
            }
            //System.out.println(tsID.toString() + ": ending");            
        }
    }

    private static class RandomAccessWorker extends Thread {        
        private StorageManager sm;
        private TupleSourceID tsID;
        private int numBatches = 0;
//        private int batchSize = 0;
        private int randomOps = 0;

        private TupleBatch[] batches;
        private boolean[] added;

        public AssertionFailedError assertion;
        public Throwable error;
        
        public RandomAccessWorker(StorageManager sm, TupleSourceID id, int numBatches, int batchSize, int randomOps) {
            this.sm = sm;
            this.tsID = id;            
            this.numBatches = numBatches;
//            this.batchSize = batchSize;
            this.randomOps = randomOps;

            // Create batches
            batches = new TupleBatch[numBatches];
            for(int i=0; i<numBatches; i++) {
                int begin = (i*batchSize)+1;
                int end = begin + batchSize - 1;
                batches[i] = exampleBatch(begin, end);
            }
            added = new boolean[numBatches];
            Arrays.fill(added, false);
        }
        
        public void run() {
            try { 
                // Randomly add, get, and remove batches but only get and remove after add
                Random random = new Random(System.currentTimeMillis());
                for(int i=0; i<randomOps; i++) {
                    int choice = random.nextInt(3);    
                    int batch = random.nextInt(numBatches);  

                    if(choice == 0) {
                        //System.out.println(tsID.toString() + ": adding batch " + batch);
                        // Add  
                        sm.addBatch(tsID, batches[batch], null);
                        added[batch] = true;
                    } else if(choice == 1) {
                        // Get
                        if(added[batch]) {
                            //System.out.println(tsID.toString() + ": getting batch " + batch);
                            TupleBatch actual = sm.getBatch(tsID, batches[batch].getBeginRow(), null);
                            TestFileStorageManager.helpCompareBatches(
                                batches[batch], 
                                actual);                                            
                        }
                    } else {
                        // Remove
                        if(added[batch]) {
                            //System.out.println(tsID.toString() + ": removing batch " + batch);
                            sm.removeBatch(tsID, batches[batch].getBeginRow());
                            added[batch] = false;
                        }                        
                    }                                        
                }                 
            } catch(AssertionFailedError e) {
                this.assertion = e;               
            } catch(Throwable e) {
                this.error = e;
            } finally {
                // Remove all the batches
                //System.out.println(tsID.toString() + ": removing batches");   
                try {          
                    sm.removeBatches(tsID);    
                } catch(MetaMatrixComponentException e) {
                }
            }
            //System.out.println(tsID.toString() + ": ending");            
        }
    }
}
