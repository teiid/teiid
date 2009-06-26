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

package com.metamatrix.common.util.crypto.keymanage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.jdom.Document;
import org.jdom.Element;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.util.crypto.PasswordChangeUtility;
import com.metamatrix.common.xml.XMLReaderWriterImpl;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.core.vdb.VdbConstants;

/**
 * Utility used to migrate encrypted passwords in a file from one key to another.
 * This utility supports two types of files: XML (for connector bindings, VDBs, and config XMLs);
 * and properties (for bootstrap properties, config preferences, and materialized view connection properties).
 */
public class FilePasswordConverter {

    private static final int FILE_TYPE_UNKNOWN = -1;
    public static final int FILE_TYPE_XML = 1;
    public static final int FILE_TYPE_PROPERTIES = 2;
    public static final int FILE_TYPE_VDB = 3;
    
    private static String DEFAULT_KEY_FILE =  CryptoUtil.KEY_NAME;
    
    private static final String PASSWORD = "PASSWORD"; //$NON-NLS-1$
    
    private int fileType = FILE_TYPE_UNKNOWN;
    private String inputFile; 
    private String outputFile; 
    private String oldkeyFile; 
    private String newkeyFile; 
    
    private PasswordChangeUtility passwordChangeUtility;
    
    //List<String>: property/attribute names that were converted
    protected List converted = new ArrayList();
    //List<String>: property/attribute names that failed to be decrypted
    protected List failedDecrypt = new ArrayList();    
    //List<String>: property/attribute names that failed to be re-encrypted
    protected List failedEncrypt = new ArrayList();

    private Exception firstException = null;
    
    public FilePasswordConverter(String inputFile, String outputFile, int fileType, String oldkeyFile,
                                  String newkeyFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.fileType = fileType;
        this.oldkeyFile = oldkeyFile;
        this.newkeyFile = newkeyFile;
    }
    
    
    public void convert() throws Exception {
        passwordChangeUtility = new PasswordChangeUtility(oldkeyFile, newkeyFile);
        
        if (fileType == FILE_TYPE_XML) {
            convertXML();
        } else if (fileType == FILE_TYPE_PROPERTIES) {
            convertProperties();
        } else if (fileType == FILE_TYPE_VDB) {
            convertVDB();
        } else {
            throw new UnsupportedOperationException("unsupported file type "+fileType); //$NON-NLS-1$
        }
        
        printResults();
    }
    
    
    /**
     * Decrypt and recrypt any the text of any "Property" Elements
     * with attribute "name" that ends with "PASSWORD" 
     * @throws Exception
     * @since 4.3
     */
    public void convertXML() throws Exception {
        //read xml document
        XMLReaderWriterImpl xmlReaderWriter = new XMLReaderWriterImpl();
        FileInputStream fis = new FileInputStream(inputFile);
        Document document = xmlReaderWriter.readDocument(fis);
        fis.close();

        
        Element root = document.getRootElement();
        List descendants = root.getChildren();
        
        convertElementsRecursive(descendants);

        
        //write xml document
        FileOutputStream fos = new FileOutputStream(outputFile);
        xmlReaderWriter.setUseNewLines(false);
        xmlReaderWriter.writeDocument(document, fos);
        fos.flush();
        fos.close();
    }    
    
    
    /**
     * The .VDB file is in zip format, containing one *.DEF file in XML format.
     * In the .DEF file, Decrypt and recrypt any the text of any "Property" Elements
     * with attribute "name" that ends with "PASSWORD" 
     * @throws Exception
     * @since 4.3
     */
    public void convertVDB() throws Exception {
        FileUtils.copy(inputFile, outputFile);
        
        //for each entry in the input zip file
        ZipFile zipFile = new ZipFile(inputFile);
        for (Enumeration en = zipFile.entries(); en.hasMoreElements(); ) {
            ZipEntry entry = (ZipEntry) en.nextElement();
            
            //if it's a .DEF file
            if (! entry.isDirectory() && 
 //               entry.getName().toUpperCase().endsWith(CoreConstants.EXPORTED_VDB_FILE_EXTENSION.toUpperCase())) {
                
                entry.getName().equalsIgnoreCase(VdbConstants.DEF_FILE_NAME)) {                                      
                
                
                //read the contents of the zip entry as XML
                XMLReaderWriterImpl xmlReaderWriter = new XMLReaderWriterImpl();
                InputStream is = zipFile.getInputStream(entry);
                Document document = xmlReaderWriter.readDocument(is);
                is.close();
                
                Element root = document.getRootElement();
                List descendants = root.getChildren();
                
                //convert the passwords
                convertElementsRecursive(descendants);
                
                
                //write xml document to a temp file
                File tempFile = File.createTempFile("vdb-temp", ".tmp"); //$NON-NLS-1$//$NON-NLS-2$
                FileOutputStream fos = new FileOutputStream(tempFile);
                xmlReaderWriter.setUseNewLines(false);
                xmlReaderWriter.writeDocument(document, fos);
                fos.flush();
                fos.close();
                
                //remove the original zip entry from the output file
                ZipFileUtil.remove(outputFile, entry.getName(), false);
                
                //add a new zip entry to the output file
                ZipFileUtil.add(outputFile, entry.getName(), tempFile.getAbsolutePath());
                
                
                //cleanup
                tempFile.delete();    
            }
        }        
    }    
    
    
    
    /**
     * Decrypt the specified elements, and recursively decrypt the children.
     * @since 4.3
     * @param elements List<Element>
     */
    private void convertElementsRecursive(List elements) {
        
        for (Iterator iter = elements.iterator(); iter.hasNext();) {
            Element element = (Element) iter.next();            
            
            if (isEncrypted(element)) {
                String value = element.getText();
                String name = element.getAttributeValue("Name"); //$NON-NLS-1$
                
                if (value != null && value.length() > 0) {
                    //decrypt
                    String decryptedValue;
                    try {
                        decryptedValue = passwordChangeUtility.oldDecrypt(value);                        
                    } catch (Exception e) {
                        failedDecrypt.add(name);
                        if (firstException == null) {
                            firstException = e;
                        }
                        continue;
                    }
                    
                    //encrypt
                    try {
                        String convertedValue = passwordChangeUtility.newEncrypt(decryptedValue);
                        element.setText(convertedValue);
                        
                        converted.add(name);
                    } catch (Exception e) {
                        failedEncrypt.add(name);
                        if (firstException == null) {
                            firstException = e;
                        }
                    }
                }
            }
            
            convertElementsRecursive(element.getChildren());
        }
    }

    /**
     * @return whether the specified element is expected to be encrypted.
     */
    private boolean isEncrypted(Element element) {
        String elementName = element.getName();
        String attributeValue = element.getAttributeValue("Name"); //$NON-NLS-1$
        
        if (elementName == null || attributeValue == null) {
            return false;
        }
        return (elementName.equalsIgnoreCase("Property") && //$NON-NLS-1$
                        attributeValue.toUpperCase().endsWith(PASSWORD));    
    }
    

    /**
     * decrypt and recrypt any properties that end with "PASSWORD" 
     * @throws Exception
     * @since 4.3
     */
    public void convertProperties() throws Exception {
        String header = PropertiesUtils.loadHeader(inputFile);
        Properties properties = PropertiesUtils.load(inputFile);
        
        Properties convertedProperties = PropertiesUtils.clone(properties);
        Enumeration names = properties.propertyNames();
    
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            if (isEncrypted(name)) {
                String value = properties.getProperty(name);
                if (value != null && value.length() > 0) {
                    //decrypt
                    String decryptedValue;
                    try {
                        decryptedValue = passwordChangeUtility.oldDecrypt(value);                        
                    } catch (Exception e) {
                        failedDecrypt.add(name);
                        if (firstException == null) {
                            firstException = e;
                        }
                        continue;
                    }
                    
                    //encrypt
                    try {
                        String convertedValue = passwordChangeUtility.newEncrypt(decryptedValue);
                        convertedProperties.setProperty(name, convertedValue);
                        
                        converted.add(name);
                    } catch (Exception e) {
                        failedEncrypt.add(name);
                        if (firstException == null) {
                            firstException = e;
                        }
                    }
                }
            } 
        }
        
        PropertiesUtils.print(outputFile, convertedProperties, header);
    }
    
    /**
     * Print which properties were converted, and which were failures 
     * 
     * @since 4.3
     */
    private void printResults() throws Exception {
        System.out.println(); 
        if (converted.size() > 0) {
            System.out.println("CONVERTED properties:"); //$NON-NLS-1$
            prettyPrint(converted);
        } else {
            //failed, so remove the output file if it exists
            try { 
                new File(outputFile).delete();   
            } catch (Exception e) {
            }
            
            if (firstException == null) {
                throw new Exception("Did not find any properties to convert");  //$NON-NLS-1$              
            } 
            throw firstException;            
        }
        
        
        if (failedDecrypt.size() > 0) {
            System.out.println(); 
            System.out.println("FAILED TO DECRYPT properties:"); //$NON-NLS-1$
            prettyPrint(failedDecrypt);
        }
        
        if (failedEncrypt.size() > 0) {
            System.out.println(); 
            System.out.println("FAILED TO RE-ENCRYPT properties:"); //$NON-NLS-1$
            prettyPrint(failedEncrypt);
        }
        
        if (firstException != null) {
            System.out.println(); 
            System.out.println("Reason for failure: "); //$NON-NLS-1$
            firstException.printStackTrace();
        }
    }
    
    /**
     * Print the specified List, one element per line  
     * @param list
     * @since 4.3
     */
    private void prettyPrint(List list) {
        if (list.size() == 0) {
            System.out.println("(NONE)"); //$NON-NLS-1$
            return;
        }
        
        for (Iterator iter = list.iterator(); iter.hasNext();) {
            System.out.println(iter.next());
        }
    }
    
    
    
    
    /**
     * @param propertyName
     * @return whether the property with the specified name is expected to be encrypted.
     * @since 4.3
     */
    private boolean isEncrypted(String propertyName) {
        return propertyName.toUpperCase().endsWith(PASSWORD);
    }
    
    
    /**
     * Main method for running the utility.
     * See <code>printUsage()</code> for usage. 
     * @param args
     * @since 4.3
     */
    public static void main(String[] args) {
        final String XML = "-xml"; //$NON-NLS-1$
        final String PROPERTIES = "-properties"; //$NON-NLS-1$
        final String VDB = "-vdb"; //$NON-NLS-1$
        final String OLD_DEFAULT = "-oldDefault"; //$NON-NLS-1$
        final String NEW_DEFAULT = "-newDefault"; //$NON-NLS-1$

        int fileType = FILE_TYPE_UNKNOWN;
        String inputFile; 
        String outputFile; 
        String oldkeyFile; 
        String newkeyFile; 

        if (args.length < 4) {
            printUsage();
        }


        if (args[0].equalsIgnoreCase(XML)) {
            fileType = FILE_TYPE_XML;
        } else if (args[0].equalsIgnoreCase(PROPERTIES)) {
            fileType = FILE_TYPE_PROPERTIES;
        } else if (args[0].equalsIgnoreCase(VDB)) {
            fileType = FILE_TYPE_VDB;
        }
        
        if (fileType == FILE_TYPE_UNKNOWN) {
            printUsage();
        }
        

        inputFile = args[1];
        outputFile = args[2];
        
        
        if (args[3].equalsIgnoreCase(OLD_DEFAULT)) {
            if (args.length != 6) {
                printUsage();
            }

            oldkeyFile = DEFAULT_KEY_FILE;
            newkeyFile = args[4];
        } else if (args[3].equalsIgnoreCase(NEW_DEFAULT)) {
            if (args.length != 7) {
                printUsage();
            }

            oldkeyFile = args[4];

            newkeyFile = DEFAULT_KEY_FILE;
        } else {
            if (args.length != 8) {
                printUsage();
            }

            oldkeyFile = args[3];

            newkeyFile = args[6];
        }

        try {
            System.out.println("\nConverting " + inputFile); //$NON-NLS-1$
            
            FilePasswordConverter converter = new FilePasswordConverter(inputFile, outputFile, fileType, oldkeyFile, 
                                                            newkeyFile);
            converter.convert();

            System.out.println("\nDone converting to " + outputFile); //$NON-NLS-1$
            System.exit(0);        
            
        } catch (Exception e) {
            System.err.println("Error: "); //$NON-NLS-1$
            e.printStackTrace();
            System.exit(1);
        }
    }
    

    /**
     * Print usage, and exit. 
     * @since 4.3
     */
    private static void printUsage() {
        System.out.println();
        System.out.println("Usage: convertpasswords [-xml|-properties|-vdb] <inputFile> <outputFile> <oldkeyFile> <oldPasswordIsEncoded> <newkeyFile>"); //$NON-NLS-1$
        System.out.println();
        System.out.println("OR     convertpasswords [-xml|-properties|-vdb] <inputFile> <outputFile> -oldDefault <newkeyFile>"); //$NON-NLS-1$
        System.out.println();
        System.out.println("OR     convertpasswords [-xml|-properties|-vdb] <inputFile> <outputFile> -newDefault <oldkeyFile>"); //$NON-NLS-1$
        System.out.println();
        
        System.exit(1);
    }
    
}
