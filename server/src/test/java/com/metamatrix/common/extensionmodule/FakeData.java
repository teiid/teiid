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

package com.metamatrix.common.extensionmodule;

import java.io.*;
import java.util.*;

import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.UnitTestUtil;

/**
 * Test data and utilities used by extension source manager related test classes.  This
 * class reflects test files located in the "testdata" directory.
 */
public final class FakeData {

    public static final String PARENT_DIRECTORY = UnitTestUtil.getTestDataPath() + "/extensionmodule"; //$NON-NLS-1$
	public static final String SYSTEM_PRINCIPAL = "System"; //$NON-NLS-1$

	/**
	 * List (unmodifiable) of ExtensionModuleDescriptor objects representing the
	 * set of test extension module files (defined elsewhere in this Class).
	 */
    public static final List DATA_FILES;
    static{
        List aList = new ArrayList();
        aList.add(TestJar1.DESCRIPTOR);
        aList.add(TestJar2.DESCRIPTOR);
        aList.add(TestJar7.DESCRIPTOR);
        aList.add(TestTextFile.DESCRIPTOR);
        aList.add(TestTextFile2.DESCRIPTOR);
        DATA_FILES = Collections.unmodifiableList(aList);
    }


    public static void init() throws IOException{
        TestJar1.data = loadFileIntoMemory(PARENT_DIRECTORY, TestJar1.SOURCE_NAME);
        TestJar2.data = loadFileIntoMemory(PARENT_DIRECTORY, TestJar2.SOURCE_NAME);
        TestJar7.data = createBigByteArray(32000000);
        TestTextFile.data = loadFileIntoMemory(PARENT_DIRECTORY, TestTextFile.SOURCE_NAME);
        TestTextFile2.data = loadFileIntoMemory(PARENT_DIRECTORY, TestTextFile2.SOURCE_NAME);
    }

	/**
	 * Given the name and parent directory of a file, loads the contents of the file into
	 * memory and returns it as a byte array.
	 */
    public static byte[] loadFileIntoMemory(String parentDirectory, String fileName) throws IOException{

        InputStream stream=null;
        byte[] data = null;
        //String parentDirectory = UnitTestUtil.getTestDataPath();
        try{
            File aFile = new File(parentDirectory, fileName);
            stream= new FileInputStream(aFile);
            int size = (int)aFile.length();
            
            data = ByteArrayHelper.toByteArray(stream, size+1);

        } finally {
            try{
                if (stream != null){
                    stream.close();
                }
            } catch (IOException e){
                //e.printStackTrace();
            }
        }
        return data;
    }
    
    
    /**
     * Create a really big byte array
     * @param size in bytes
     */
    public static byte[] createBigByteArray(int size) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 1);
        
        return bytes;
    }
    
    

    public static class TestJar1{
        public static final String SOURCE_NAME = "fake1.jar"; //$NON-NLS-1$
        public static final String TYPE = ExtensionModuleTypes.JAR_FILE_TYPE;
        public static final String DESCRIPTION = "A test JAR file with one class"; //$NON-NLS-1$
        private static final long CHECKSUM = 4287822908l;
        private static final long LAST_MODIFIED = (new File(PARENT_DIRECTORY, SOURCE_NAME)).lastModified();
        private static final int POSITION = 0;
        public static class Class1{
            public static final String CLASSNAME = "com.fake.MyClass1"; //$NON-NLS-1$
            public static Class theClass;
        }
        public static byte[] data;
        public static final ExtensionModuleDescriptor DESCRIPTOR = new DataFileImpl(SOURCE_NAME,TYPE,POSITION,DESCRIPTION,CHECKSUM,LAST_MODIFIED);
    }

    public static class TestJar2{
        public static final String SOURCE_NAME = "fake2.jar"; //$NON-NLS-1$
        public static final String TYPE = ExtensionModuleTypes.JAR_FILE_TYPE;
        public static final String DESCRIPTION = "A test JAR file with two classes"; //$NON-NLS-1$
        private static final long CHECKSUM = 360275295l;
        private static final long LAST_MODIFIED = (new File(PARENT_DIRECTORY, SOURCE_NAME)).lastModified();
        private static final int POSITION = 1;
        public static class Class1{
            public static final String CLASSNAME = "com.fake.MyClass2"; //$NON-NLS-1$
            public static Class theClass;
        }
        public static class Class2{
            public static final String CLASSNAME = "com.fake.MyClass2a"; //$NON-NLS-1$
            public static Class theClass;
        }
        public static byte[] data;
        public static final ExtensionModuleDescriptor DESCRIPTOR = new DataFileImpl(SOURCE_NAME,TYPE,POSITION,DESCRIPTION,CHECKSUM,LAST_MODIFIED);
    }
    
    //really BIG Jar (32M)
    public static class TestJar7{
        public static final String SOURCE_NAME = "big.jar"; //$NON-NLS-1$
        public static final String TYPE = ExtensionModuleTypes.JAR_FILE_TYPE;
        public static final String DESCRIPTION = "A test JAR file of no interest"; //$NON-NLS-1$
        private static final long CHECKSUM = 1895850785l;
        private static final long LAST_MODIFIED = (new File(PARENT_DIRECTORY, SOURCE_NAME)).lastModified();
        private static final int POSITION = 6;
        public static byte[] data;
        public static final ExtensionModuleDescriptor DESCRIPTOR = new DataFileImpl(SOURCE_NAME,TYPE,POSITION,DESCRIPTION,CHECKSUM,LAST_MODIFIED);
    }    

    public static class TestTextFile{
        public static final String SOURCE_NAME = "fake.txt"; //$NON-NLS-1$
        public static final String TYPE = ExtensionModuleTypes.FUNCTION_DEFINITION_TYPE;
        public static final String DESCRIPTION = "A test text file"; //$NON-NLS-1$
        private static final long CHECKSUM = 3438642310l;
        private static final long LAST_MODIFIED = (new File(PARENT_DIRECTORY, SOURCE_NAME)).lastModified();
        private static final int POSITION = 7;
        public static byte[] data;
        public static final ExtensionModuleDescriptor DESCRIPTOR = new DataFileImpl(SOURCE_NAME,TYPE,POSITION,DESCRIPTION,CHECKSUM,LAST_MODIFIED);
    }

    public static class TestTextFile2{
        public static final String SOURCE_NAME = "Nuge.log"; //$NON-NLS-1$
        public static final String TYPE = ExtensionModuleTypes.FUNCTION_DEFINITION_TYPE;
        public static final String DESCRIPTION = "A test text file"; //$NON-NLS-1$
        private static final long CHECKSUM = 3438642310l;
        private static final long LAST_MODIFIED = (new File(PARENT_DIRECTORY, SOURCE_NAME)).lastModified();
        private static final int POSITION = 8;
        public static byte[] data;
        public static final ExtensionModuleDescriptor DESCRIPTOR = new DataFileImpl(SOURCE_NAME,TYPE,POSITION,DESCRIPTION,CHECKSUM,LAST_MODIFIED);
    }

    private static class DataFileImpl extends ExtensionModuleDescriptor implements Serializable{
		private String name;
		private String type;
		private int pos;
		private String desc;
		private String modifiedString;
		private long checksum;
//		private long modified;
	    DataFileImpl(String name, String type, int pos, String desc, long checksum, long modified){
	        modifiedString = DateUtil.getDateAsString(new Date(modified));
	        this.name = name;
	        this.type = type;
	        this.desc = desc;
	        this.checksum = checksum;
//	        this.modified = modified;
	    } 
        public String getName(){return name;}
        public String getType(){return type;}
        public int getPosition(){return pos;}
        public boolean isEnabled(){return true;}
        public String getDescription(){return desc;}
        public String getCreatedBy(){return SYSTEM_PRINCIPAL;}
        public String getCreationDate(){return modifiedString;}
        public String getLastUpdatedBy(){return SYSTEM_PRINCIPAL;}
        public String getLastUpdatedDate(){return modifiedString;}
        public long getChecksum(){return checksum;}
        public int compareTo(Object obj){return ExtensionModuleDescriptorUtils.compareTo(this, obj);}
        public boolean equals(Object obj){return ExtensionModuleDescriptorUtils.equals(this, obj);}
	}

}
