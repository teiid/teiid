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

package org.teiid.translator.text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.BasicExecutionFactory;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.FileConnection;
import org.teiid.translator.MetadataProvider;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;


/**
 * Implementation of text connector.
 */
public class TextExecutionFactory extends BasicExecutionFactory implements MetadataProvider{

	private boolean enforceColumnCount = false;
	private String dateResultFormatsDelimiter;
	private String dateResultFormats;
	Map<String, Properties> metadataProps = new HashMap<String, Properties>();
	private String descriptorFile;
	private String encoding = Charset.defaultCharset().name();
	
	@Override
	public void start() throws ConnectorException {
		initMetaDataProps();
    }

	@TranslatorProperty(name="EnforceColumnCount", display="Enforce Column Count",description="This forces the number of columns in text file to match what was modeled", defaultValue="false")
	public boolean isEnforceColumnCount() {
		return enforceColumnCount;
	}

	public void setEnforceColumnCount(Boolean enforceColumnCount) {
		this.enforceColumnCount = enforceColumnCount.booleanValue();
	}
	
	@TranslatorProperty(name="DateResultFormatsDelimiter", display="Date Result Formats Delimiter", advanced=true)
	public String getDateResultFormatsDelimiter() {
		return dateResultFormatsDelimiter;
	}

	public void setDateResultFormatsDelimiter(String dateResultFormatsDelimiter) {
		this.dateResultFormatsDelimiter = dateResultFormatsDelimiter;
	}

	@TranslatorProperty(name="DateResultFormats", display="Date Result Formats",advanced=true)
	public String getDateResultFormats() {
		return dateResultFormats;
	}

	public void setDateResultFormats(String dateResultFormats) {
		this.dateResultFormats = dateResultFormats;
	}	
	
	@TranslatorProperty(name="DescriptorFile", display="Descriptor File",required=true)
	public String getDescriptorFile() {
		return descriptorFile;
	}

	public void setDescriptorFile(String descriptorFile) {
		this.descriptorFile = descriptorFile;
	}	
	
	@TranslatorProperty(name="Encoding", display="File Encoding",advanced=true)
	public String getEncoding() {
		return encoding;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws ConnectorException {
    	try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			TextConnectionImpl textConn = new TextConnectionImpl(this.metadataProps, (FileConnection)cf.getConnection(), encoding);
			return new TextSynchExecution(this, (Select)command, textConn);
		} catch (ResourceException e) {
			throw new ConnectorException(e);
		}
    }

	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory, Object connectionFactory) throws ConnectorException {
		for (Map.Entry<String, Properties> entry : metadataProps.entrySet()) {
			Properties p = entry.getValue();
			String columns = p.getProperty(TextDescriptorPropertyNames.COLUMNS);
			if (columns == null) {
				continue;
			}
			String types = p.getProperty(TextDescriptorPropertyNames.TYPES);
			String[] columnNames = columns.trim().split(","); //$NON-NLS-1$
			String[] typeNames = null; 
			if (types != null) {
				typeNames = types.trim().split(","); //$NON-NLS-1$
				if (typeNames.length != columnNames.length) {
					throw new ConnectorException(TextPlugin.Util.getString("TextConnector.column_mismatch", entry.getKey())); //$NON-NLS-1$
				}
			}
			Table table = metadataFactory.addTable(entry.getKey().substring(entry.getKey().indexOf('.') + 1));
			for (int i = 0; i < columnNames.length; i++) {
				String type = typeNames == null?TypeFacility.RUNTIME_NAMES.STRING:typeNames[i].trim().toLowerCase();
				Column column = metadataFactory.addColumn(columnNames[i].trim(), type, table);
				column.setNameInSource(String.valueOf(i));
				column.setNativeType(TypeFacility.RUNTIME_NAMES.STRING);
			}
		}
	} 
	    
    private void initMetaDataProps() throws ConnectorException {
        // Verify required items
        if (descriptorFile == null || descriptorFile.trim().length() == 0) {
            throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Descriptor_file_name_is_not_specified._2")); //$NON-NLS-1$
        }
        BufferedReader br = null;
        try {
            br = getReader(descriptorFile);
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Reading descriptor file: " + descriptorFile); //$NON-NLS-1$

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
				getMetadata(line);
            }
        } catch (IOException e) {
        	LogManager.logError(LogConstants.CTX_CONNECTOR, e, TextPlugin.Util.getString("TextConnection.Error_while_reading_text_file__{0}_1", new Object[] {e.getMessage()})); //$NON-NLS-1$
            throw new ConnectorException(e, TextPlugin.Util.getString("TextConnection.Error_trying_to_establish_connection_5")); //$NON-NLS-1$
        } finally {
        	if (br != null) {
        		try {br.close();} catch (Exception ee) {}
        	}
        }
        LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Successfully read metadata information from the descriptor file " + descriptorFile); //$NON-NLS-1$
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

            if (propertyName.equals(TextDescriptorPropertyNames.HEADER_LINES)) {
                try {
                    Integer.parseInt(propertyValue);
                } catch (NumberFormatException e) {
                    throw new ConnectorException(e, TextPlugin.Util.getString("TextConnection.The_value_for_the_property_should_be_an_integer._{0}_3", new Object[] {e.getMessage()})); //$NON-NLS-1$
                }
            } else if (propertyName.equals(TextDescriptorPropertyNames.HEADER_ROW)) {
                try {
                    Integer.parseInt(propertyValue);
                } catch (NumberFormatException e) {
                    throw new ConnectorException(e, TextPlugin.Util.getString("TextConnection.The_value_for_the_property_should_be_an_integer._{0}_3", new Object[] {e.getMessage()})); //$NON-NLS-1$
                }
            } else if (!(propertyName.equals(TextDescriptorPropertyNames.COLUMNS)
            		|| propertyName.equals(TextDescriptorPropertyNames.TYPES) || propertyName.equals(TextDescriptorPropertyNames.LOCATION)
            		|| propertyName.equals(TextDescriptorPropertyNames.DELIMITER) || propertyName.equals(TextDescriptorPropertyNames.QUALIFIER))) {
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
     * This method gets the reader object for the descriptorfile.
     * @param fileLocation String standing for the fileLocation either in file system or web.
     * @return BufferReader for the file
     */
    private BufferedReader getReader(String fileLocation) throws ConnectorException {
        try {
        	InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileLocation);
        	if (is != null) {
        		return new BufferedReader(new InputStreamReader(is, encoding));
        	}
        	try {
	            URL url = new URL(fileLocation);
	            // create the connection to the URL
	            URLConnection conn = url.openConnection();
	            // establish the connection to the URL
	            conn.connect();
	            // get the stream from the connection
	            InputStreamReader inSR = new InputStreamReader(conn.getInputStream(), encoding);
	            // place the stream into a buffered reader
	            return new BufferedReader(inSR);
        	} catch (MalformedURLException e) {
        		throw new ConnectorException(TextPlugin.Util.getString("TextConnection.Descriptor_file_does_not_exist_at_this_location__{0}_12", new Object[] {fileLocation})); //$NON-NLS-1$
        	}
        } catch (IOException e) {
            throw new ConnectorException(e,TextPlugin.Util.getString("TextConnection.Descriptor_file_does_not_exist_at_this_location__{0}_12", new Object[] {fileLocation})); //$NON-NLS-1$
        }
    }      

}
