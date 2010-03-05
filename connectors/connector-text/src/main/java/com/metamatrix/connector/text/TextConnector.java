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

package com.metamatrix.connector.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.basic.BasicConnector;


/**
 * Implementation of text connector.
 */
public class TextConnector extends BasicConnector {

    private TextManagedConnectionFactory config;
    private int srcFiles = 0;
    private int srcFileErrs = 0;
    private Map<String, Properties> metadataProps = new HashMap<String, Properties>();
    private String parentDirectory;

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
		
        this.config = (TextManagedConnectionFactory)env;

        initMetaDataProps();
        
        // test connection
        TextConnection test = new TextConnection(this.config, metadataProps);
        test.close();
    }


    public Connection getConnection() throws ConnectorException {
        return new TextConnection(this.config, metadataProps);
    }

    private void initMetaDataProps() throws ConnectorException {
    	reinitFileCounts();
        try {
            readDescriptor(this.config.getDescriptorFile(),this.config.isPartialStartupAllowed());
            reinitFileCounts();
        } catch (ConnectorException ce) {
        	// If partial startup is not allowed, throw the exception
        	if(!this.config.isPartialStartupAllowed() ) {
        		reinitFileCounts();
        		throw ce;
            // If partial startup is allowed, only throw exception if no files connected
        	} else if(this.srcFileErrs==this.srcFiles) {
        		reinitFileCounts();
        		throw ce;
        	}
        }
    }

    
    private void reinitFileCounts() {
		this.srcFiles=0;
		this.srcFileErrs=0;
    }

    /**
     * Read Descriptor file and get properties info for acessing the file.
     * @param descriptorFile String standing for the name of descriptor file.
     * @throws ConnectorException throws if error occurs
     */
    private void readDescriptor(String descriptorFile, boolean startPartial) 
        throws ConnectorException {
        
        // Verify required items
        if (descriptorFile == null || descriptorFile.trim().length() == 0) {
            throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Descriptor_file_name_is_not_specified._2")); //$NON-NLS-1$
        }
        // Save first exception if there are multiple 
        ConnectorException connExcep = null;
        
        BufferedReader br = null;
        try {
            br = getReader(descriptorFile);
            this.config.getLogger().logInfo("Reading descriptor file: " + descriptorFile); //$NON-NLS-1$

            String line = null;
            // Walk through records, finding matches
            while(true) {
                line = br.readLine();
                if (line == null) {
                    break;
                }

                // Skip blank lines
                if (line.length() == 0) {
                    continue;
                }

                // populate the map with metadata information for the given line.  If readAll option is chosen,
                // keep trying to read until everything is tried, then throw the first exception encountered.
                try {
					getMetadata(line);
				} catch (ConnectorException e) {
					if(!startPartial) {
						throw e;
					} else if (connExcep==null) {
						connExcep=e;
					}
				}

            }
            // throw first exception if readAll was set
            if(connExcep!=null) throw connExcep;
            
        } catch (IOException e) {
        	this.config.getLogger().logError(TextPlugin.Util.getString("TextConnection.Error_while_reading_text_file__{0}_1", new Object[] {e.getMessage()}), e); //$NON-NLS-1$
            throw new ConnectorException(e, TextPlugin.Util.getString("TextConnection.Error_trying_to_establish_connection_5")); //$NON-NLS-1$
        } finally {
        	if (br != null) {
        		try {br.close();} catch (Exception ee) {}
        	}
        }
        this.config.getLogger().logDetail("Successfully read metadata information from the descriptor file " + descriptorFile); //$NON-NLS-1$
    }

    /**
     * Read the property string and populate the properties with info needed to access data files.
     */
    private void getMetadata(String propStr) throws ConnectorException {
        try {
            int index = 0;

            // Property String --> <Fully_Qualified_Group>.location=<location of actual file>
            int eqIndex = propStr.indexOf("=", index); //$NON-NLS-1$
            String propString = propStr.substring(index, eqIndex);

            if (!propString.equals(" ")) { //$NON-NLS-1$
                propString = propString.trim();
            }

            int lastIndex = propString.lastIndexOf('.');

            // group name
            String groupName = propString.substring(0, lastIndex).toUpperCase();
            // property name
            String propertyName = propString.substring(lastIndex + 1).toUpperCase();

            // Properties read from descriptor, which are properties for a given group
            Properties props = metadataProps.get(groupName);
            if (props == null) {
                props = new Properties();
            }

            // Adjust index past '='
            index = eqIndex + 1;

            // Read property value
            String propertyValue = propStr.substring(index).trim();

            if (propertyName.equals(TextPropertyNames.LOCATION)) {
            	srcFiles++;
                // Verify required items
                if (propertyValue == null || propertyValue.trim().length() == 0) {
                    srcFileErrs++;
                    throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Text_file_name_is_not_specified_for_the_group___{0}_2", new Object[] {groupName})); //$NON-NLS-1$
                }
                try {
                	checkFile(propertyValue, props, groupName);
                } catch (ConnectorException e) {
                	if (this.parentDirectory == null) {
                		throw e;
                	}
                	srcFileErrs--;
            		try {
            			checkFile(parentDirectory + "/" + propertyValue, props, groupName); //$NON-NLS-1$
            		} catch (ConnectorException e1) {
            			throw e;
            		}
                }
            } else if (propertyName.equals(TextPropertyNames.HEADER_LINES)) {
                try {
                    Integer.parseInt(propertyValue);
                } catch (NumberFormatException e) {
                    throw new ConnectorException(e, TextPlugin.Util.getString("TextConnection.The_value_for_the_property_should_be_an_integer._{0}_3", new Object[] {e.getMessage()})); //$NON-NLS-1$
                }
            } else if (propertyName.equals(TextPropertyNames.HEADER_ROW)) {
                try {
                    Integer.parseInt(propertyValue);
                } catch (NumberFormatException e) {
                    throw new ConnectorException(e, TextPlugin.Util.getString("TextConnection.The_value_for_the_property_should_be_an_integer._{0}_3", new Object[] {e.getMessage()})); //$NON-NLS-1$
                }
            } else if (!(propertyName.equals(TextPropertyNames.COLUMNS) || propertyName.equals(TextPropertyNames.TYPES) || propertyName.equals(TextPropertyNames.TYPES) || propertyName.equals(TextPropertyNames.TYPES) || propertyName.equals(TextPropertyNames.TYPES) || propertyName.equals(TextPropertyNames.DELIMITER) || propertyName.equals(TextPropertyNames.QUALIFIER))) {
                throw new ConnectorException(TextPlugin.Util.getString("TextConnection.The_property_{0}_for_the_group_{1}_is_invalid._4", new Object[] {propertyName, groupName})); //$NON-NLS-1$
            }

            // Check for tab as a delimiter and use correct string
            if (propertyValue != null && propertyValue.equals("\\t")) { //$NON-NLS-1$
                propertyValue = "\t"; //$NON-NLS-1$
            }

            if (propertyValue != null && !propertyValue.equals("")) { //$NON-NLS-1$
                // Add property
                props.put(propertyName, propertyValue);
                metadataProps.put(groupName, props);
            }
        } catch (Exception e) {
            throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Error_parsing_property_string_{0}__{1}_6", new Object[] {propStr, e.getMessage()})); //$NON-NLS-1$
        }

    }

    /**
     * Check if the file or url of the given name exists and populate properties.
     */
    private void checkFile(String fileName, Properties props, String groupName) 
        throws ConnectorException {
        
        // Construct file and make sure it exists and is readable
        File datafile = new File(fileName);
        
        File[] files= TextUtil.getFiles(fileName);
        
        
        // determine if the wild card is used to indicate all files
        // of the specified extension
        if (files == null && TextUtil.usesWildCard(fileName)) {
            srcFileErrs++;
            throw new ConnectorException(TextPlugin.Util.getString("TextConnection.fileDoesNotExistForGroup", new Object[] {fileName, groupName})); //$NON-NLS-1$                                    
        }
        if (files != null && files.length > 0) { 
            props.setProperty(TextPropertyNames.FILE_LOCATION, fileName);
        } else if (datafile.isFile()){
            if (!datafile.exists()) {
                srcFileErrs++;
                throw new ConnectorException(TextPlugin.Util.getString("TextConnection.fileDoesNotExistForGroup", new Object[] {fileName, groupName})); //$NON-NLS-1$                        
            }
            else if (!datafile.canRead()) {
                srcFileErrs++;
                throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Data_file_{0}_found_but_does_not_have_Read_permissions_8", new Object[] {fileName})); //$NON-NLS-1$                
            }
            props.setProperty(TextPropertyNames.FILE_LOCATION, fileName);
        } else {
            try {
                URL url = new URL(fileName);
                // create the connection to the URL
                URLConnection conn = url.openConnection();
                // establish the connection to the URL
                conn.connect();
                props.setProperty(TextPropertyNames.URL_LOCATION, fileName);
            } catch (IOException e) {
                srcFileErrs++;
                throw new ConnectorException(e,TextPlugin.Util.getString("TextConnection.fileDoesNotExistForGroup", new Object[] {fileName, groupName})); //$NON-NLS-1$
            }
        } 

    }

    /**
     * This method gets the reader object for the descriptorfile present either on the local file system or the web.
     * @param fileLocation String standing for the fileLocation either in file system or web.
     * @return BufferReader for the file
     */
    private BufferedReader getReader(String fileLocation) throws ConnectorException {
        BufferedReader br = null;
        // Construct file and make sure it exists and is readable
        File descfile = new File(fileLocation);
        if (descfile.isFile()) {
            if (!descfile.exists()) {
                throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Descriptor_file_does_not_exist_at_this_location__{0}_9", new Object[] {fileLocation})); //$NON-NLS-1$
            } else if (!descfile.canRead()) {
                throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Descriptor_file_{0}_found_but_does_not_have_Read_permissions_10", new Object[] {fileLocation})); //$NON-NLS-1$
            }
            this.parentDirectory = descfile.getParent();
            try {
                br = new BufferedReader(new FileReader(descfile));

            } catch (FileNotFoundException fe) {
                throw new ConnectorException(fe,TextPlugin.Util.getString("TextConnection.Couldn__t_find_the_file_of_name_{0}_11", new Object[] {descfile})); //$NON-NLS-1$
            }
        } else {
            try {
                URL url = new URL(fileLocation);
                // create the connection to the URL
                URLConnection conn = url.openConnection();
                // establish the connection to the URL
                conn.connect();
                // get the stream from the connection
                InputStreamReader inSR = new InputStreamReader(conn.getInputStream());
                // place the stream into a buffered reader
                br = new BufferedReader(inSR);
            } catch (IOException e) {
                throw new ConnectorException(e,TextPlugin.Util.getString("TextConnection.Descriptor_file_does_not_exist_at_this_location__{0}_12", new Object[] {fileLocation})); //$NON-NLS-1$
            }
        }
        return br;
    }




}
