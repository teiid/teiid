/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;

import junit.framework.TestCase;


/**
 * @since 4.0
 */
public final class TestFileUtils extends TestCase {

    private static final String FILE_NAME = UnitTestUtil.getTestDataPath() + File.separator + "fakeScript.txt"; //$NON-NLS-1$

    private final static String TEMP_DIR_NAME = "tempdir"; //$NON-NLS-1$
    File tempDir;
    public static final String TEMP_FILE = "delete.me"; //$NON-NLS-1$
    public static final String TEMP_FILE_RENAMED = "delete.me.old"; //$NON-NLS-1$
    private final static String TEMP_FILE_NAME = UnitTestUtil.getTestDataPath() + File.separator + "tempfile.txt"; //$NON-NLS-1$
    private final static String TEMP_FILE_NAME2 = "tempfile2.txt"; //$NON-NLS-1$

    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================
    /**
     * Constructor for TestJDBCRepositoryWriter.
     * @param name
     */
    public TestFileUtils(String name) {
        super(name);
    }

    // =========================================================================
    //                 S E T   U P   A N D   T E A R   D O W N
    // =========================================================================

    public void setUp() throws Exception {
        super.setUp();

        //create a temp directory
        tempDir = new File(TEMP_DIR_NAME);
        tempDir.mkdir();
    }

    public void tearDown() throws Exception {
        super.tearDown();

        try {
            tempDir.delete();
        } catch (Exception e) {
        }

        try {
            new File(TEMP_FILE_NAME).delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            new File(TEMP_FILE_NAME2).delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests FileUtils.testDirectoryPermissions()
     * @since 4.3
     */
    public void testTestDirectoryPermissions() throws Exception {


        //positive case
        TestFileUtils.testDirectoryPermissions(TEMP_DIR_NAME);

        //negative case: dir doesn't exist
        try {
            TestFileUtils.testDirectoryPermissions("fakeDir"); //$NON-NLS-1$
            fail("Expected a MetaMatrixCoreException"); //$NON-NLS-1$
        } catch (TeiidException e) {
        }
    }



    /**
     * Tests FileUtils.remove()
     * @since 4.3
     */
    public void testRemove() throws Exception {
        ObjectConverterUtil.write(new FileInputStream(FILE_NAME), TEMP_FILE_NAME);

        //positive case
        FileUtils.remove(new File(TEMP_FILE_NAME));
        assertFalse("Expected File to not exist", new File(TEMP_FILE_NAME).exists());  //$NON-NLS-1$


        //call again - this should not throw an exception
        FileUtils.remove(new File(TEMP_FILE_NAME));
    }

    /**
     * Test whether it's possible to read and write files in the specified directory.
     * @param dirPath Name of the directory to test
     * @throws TeiidException
     * @since 4.3
     */
    public static void testDirectoryPermissions(String dirPath) throws TeiidException {

        //try to create a file
        File tmpFile = new File(dirPath + File.separatorChar + TestFileUtils.TEMP_FILE);
        boolean success = false;
        try {
            success = tmpFile.createNewFile();
        } catch (IOException e) {
        }
        if (!success) {
              throw new TeiidException("cannot create file in " + dirPath); //$NON-NLS-1$
        }

        //test if file can be written to
        if (!tmpFile.canWrite()) {
              throw new TeiidException("cannot write " +dirPath); //$NON-NLS-1$
        }

        //test if file can be read
        if (!tmpFile.canRead()) {
              throw new TeiidException("cannot read " + dirPath); //$NON-NLS-1$
        }

        //test if file can be renamed
        File newFile = new File(dirPath + File.separatorChar + TestFileUtils.TEMP_FILE_RENAMED);
        success = false;
        try {
            success = tmpFile.renameTo(newFile);
        } catch (Exception e) {
        }
        if (!success) {
              throw new TeiidException("failed to rename " + dirPath); //$NON-NLS-1$
        }

        //test if file can be deleted
        success = false;
        try {
            success = newFile.delete();
        } catch (Exception e) {
        }
        if (!success) {
            final String msg = CorePlugin.Util.getString("FileUtils.Unable_to_delete_file_in", dirPath); //$NON-NLS-1$
              throw new TeiidException(msg);
        }
    }

}
