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

package org.teiid.resource.cci.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.resource.ResourceException;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.text.TextConnection;
import org.teiid.resource.adapter.text.TextDescriptorPropertyNames;
import org.teiid.resource.adapter.text.TextPlugin;
import org.teiid.resource.adapter.text.TextUtil;
import org.teiid.resource.spi.BasicConnection;

import com.metamatrix.core.util.StringUtil;


/**
 * Implementation of Connection interface for text connection.
 */
public class TextConnectionImpl extends BasicConnection implements TextConnection{
	
	private Map<String, Properties> metadataProps;
    private ArrayList readerQueue = new ArrayList();
    private int readerQueueIndex = 0;    
    private String headerMsg;
    private String firstLine = null;

    // current Reader object
    private BufferedReader currentreader = null;

    // Line num in the text file
    private int lineNum = 0;    
    
    
	public TextConnectionImpl(Map<String, Properties> props) {
		this.metadataProps = props;
	}
	
	@Override
	public void close() throws ResourceException {
        if (readerQueue.size() > 0) {
            for (Iterator it=readerQueue.iterator(); it.hasNext();) {
                BufferedReader br = (BufferedReader) it.next();
                try {
                    br.close();
                } catch (IOException err) {
                }
                    
            }
        }
        readerQueue.clear();		
	}

	@Override
	public Map<String, Properties> getMetadataProperties() {
		return metadataProps;
	}
	
	private List<Reader> createReaders(String tableName) throws ConnectorException {
		try {
			createReaders(this.metadataProps.get(tableName), tableName);
		} catch (IOException e) {
            throw new ConnectorException(e, TextPlugin.Util.getString("TextSynchExecution.Unable_get_Reader", new Object[] {tableName, e.getMessage() })); //$NON-NLS-1$

		}
		return readerQueue;
	}
	
    /**
     * This method gets the reader object for the textfile present either on
     * the local file system or the web.
     * @param props Group's metadata properites string
     * @return BufferReader Object
     */
    private void createReaders(Properties props, String groupName) throws IOException, ConnectorException {
        if(readerQueue != null && readerQueue.size() > 0) {
            return ;
        }
        
        if(props == null) {
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Error_obtain_properties_for_group", groupName)); //$NON-NLS-1$
        }
        
        String fileName  = props.getProperty(TextDescriptorPropertyNames.FILE_LOCATION);
        

        if(fileName != null) {
            File datafile = new File(fileName);
            File[] files= TextUtil.getFiles(fileName);

            
            // determine if the wild card is used to indicate all files
            // of the specified extension
            if (files != null && files.length > 0) { 
                for (int i = 0; i<files.length; i++) {
                    File f = files[i];
                   addReader(f.getName(), f);  
                }
                
            } else {
                addReader(fileName, datafile);                    
            } 
        } else {
            String urlName = props.getProperty(TextDescriptorPropertyNames.URL_LOCATION);
            if(urlName==null) {
                Object[] params = new Object[] { groupName };
                throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Unable_get_Reader_for_group", params)); //$NON-NLS-1$
            }

            // create the URL object
            URL url = new URL(urlName);
            // create the connection to the URL
            URLConnection conn = url.openConnection();
            // establish the connection to the URL
            conn.connect();
            // get the stream from the connection
            InputStreamReader inSR = new InputStreamReader(conn.getInputStream());
            // place the stream into a buffered reader
            addReader(fileName, inSR);
        }
 
    }
    
    private void addReader(String fileName, File datafile) throws IOException {
        
        FileInputStream fis = new FileInputStream(datafile);
        InputStreamReader inSR = new InputStreamReader(fis);

        BufferedReader r = new BufferedReader(inSR);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Reading file: " + fileName); //$NON-NLS-1$
        readerQueue.add(r);
    }
    
    private void addReader(String fileName, InputStreamReader inSr) {
        BufferedReader r = new BufferedReader(inSr);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Reading URL: " + fileName); //$NON-NLS-1$
        readerQueue.add(r);
    } 	
    
    /**
     * Gets the current reader, looking at the next
     * one in the list if the current one is null.
	 * 
	 * If HEADER_LINES is greater than 0, an attempt is made to
     * read the column headers from the file.  If HEADER_ROW is 
	 * less than 1, row HEADER_LINES is used as the row that may 
     * contain column names.  If HEADER_ROW is greater than 1, 
     * row HEADER_ROW is used as the row that may contain column 
	 * names.
     *
     * @throws ConnectorException 
     */
    private String nextLine(String tableName) throws ConnectorException {
    	if (currentreader == null && readerQueueIndex < readerQueue.size()) {
            // reader queue index is advanced only by the nextReader()
            // method.  Don't do it here.
            currentreader = (BufferedReader)readerQueue.get(readerQueueIndex);
    	}
		/* Retrieve connector properties so that we can find a 
		 * header row if necessary.
		 */    	
        Properties groupProps = this.metadataProps.get(tableName);
		String line = null;
		String location = groupProps.getProperty(TextDescriptorPropertyNames.LOCATION);
		String qualifier = groupProps.getProperty(TextDescriptorPropertyNames.QUALIFIER);
		String topLines = groupProps.getProperty(TextDescriptorPropertyNames.HEADER_LINES);
		String headerLine = groupProps.getProperty(TextDescriptorPropertyNames.HEADER_ROW);

		int numTop = 0;
		int headerRowNum = 0;
		if (topLines != null && topLines.length() > 0)
			numTop = Integer.parseInt(topLines);
		if (headerLine != null && headerLine.length() > 0)
			headerRowNum = Integer.parseInt(headerLine);

		/* Check to see if the value for HEADER_ROW is greater than
		 * the number of lines to skip.  If it is, it is invalid and
		 * we will use HEADER_LINES instead.
		 */
		if ( headerRowNum > numTop ) {
			Object[] params = new Object[] { TextDescriptorPropertyNames.HEADER_ROW, new Integer(headerRowNum), new Integer(numTop) }; 
			String msg = TextPlugin.Util.getString("TextSynchExecution.Property_contains_an_invalid_value_Using_value", params); //$NON-NLS-1$
			// TODO: We should include the group name in the log message.
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, msg);
			
			headerRowNum = numTop;
		}
		try {
			// set hasQualifier flag
			boolean hasQualifier = false;
			if (qualifier != null && qualifier.length() > 0)
				hasQualifier = true;

			// Walk through rows looking for header row
			while (currentreader != null) {
				line = currentreader.readLine();
				// Hit the end of file or the file is empty then
				// try next reader
				if (line == null) {
					advanceToNextReader();
					lineNum = 0;
					return nextLine(tableName);
				}

				// check if we have a qualifier defined
				// if yes check that all qualifiers have been terminated
				// if not then append the next line (if available)
				if (hasQualifier) {
					while (StringUtil.occurrences(line, qualifier) % 2 != 0) {
						String nextLine = currentreader.readLine();
						if (nextLine != null)
							line = line + StringUtil.LINE_SEPARATOR	+ nextLine;
						else {
							Object[] params = new Object[] { line }; 
							String msg = TextPlugin.Util.getString("TextSynchExecution.Text_has_no_determined_ending_qualifier", params); //$NON-NLS-1$
							throw new ConnectorException(msg); 
						}
					}
				}
				lineNum++;

				// Skip blank lines
				if (line.length() == 0) continue;

				// Attempt to retrieve column names from header row
				// or last row of header lines.
				if ((headerRowNum > 0 && headerRowNum == lineNum) || (numTop == lineNum)) {
					// This is the header row; check if null to avoid the second or clause
					if (headerMsg == null) {
						headerMsg = line;
					}
					continue;
				} else if (numTop >= lineNum) {
					continue;
				}
				return line;
			}
		} catch (Throwable e) {
			throw new ConnectorException(e, TextPlugin.Util.getString("TextSynchExecution.Error_reading_text_file", new Object[] { location, e.getMessage() })); //$NON-NLS-1$
		}
    	// we are done reading..
    	return null;
	}    
    
    /**
     * Indicate that we are done with the current reader and we should
     * advance to the next reader.
     */
    private void advanceToNextReader(){
        currentreader = null;
        readerQueueIndex++;
    }    
    
    @Override
    public String getHeaderLine(String tableName) throws ConnectorException {
    	if (this.headerMsg != null) {
    		return this.headerMsg; 
    	}
    	createReaders(tableName);
    	this.firstLine =  nextLine(tableName);
    	return this.headerMsg;
    }
    
    @Override
    public String getNextLine(String tableName) throws ConnectorException {
    	// make sure the reader are created
    	createReaders(tableName);
    	if (this.firstLine != null) {
    		String copy = this.firstLine;
    		this.firstLine = null;
    		return copy;
    	}
    	return nextLine(tableName);
    }
}
