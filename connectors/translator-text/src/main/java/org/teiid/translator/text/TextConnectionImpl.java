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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.FileConnection;



/**
 * Implementation of Connection interface for text connection.
 */
public class TextConnectionImpl {
	
	private Map<String, Properties> metadataProps;
    private ArrayList<BufferedReader> readerQueue = new ArrayList<BufferedReader>();
    private int readerQueueIndex = 0;    
    private String headerMsg;
    private String firstLine = null;

    // current Reader object
    private BufferedReader currentreader = null;

    // Line num in the text file
    private int lineNum = 0;    
    private FileConnection conn;
    private String encoding;
    
	public TextConnectionImpl(Map<String, Properties> props, FileConnection conn, String encoding) {
		this.metadataProps = props;
		this.conn = conn;
		this.encoding = encoding;
	}
	
	public void close() {
        if (readerQueue.size() > 0) {
            for (Iterator<BufferedReader> it=readerQueue.iterator(); it.hasNext();) {
                BufferedReader br = it.next();
                try {
                    br.close();
                } catch (IOException err) {
                }
                    
            }
        }
        readerQueue.clear();		
	}

	public Map<String, Properties> getMetadataProperties() {
		return metadataProps;
	}
	
	private List<BufferedReader> createReaders(String tableName) throws TranslatorException {
		try {
			createReaders(this.metadataProps.get(tableName), tableName);
		} catch (IOException e) {
            throw new TranslatorException(e, TextPlugin.Util.getString("TextSynchExecution.Unable_get_Reader", new Object[] {tableName, e.getMessage() })); //$NON-NLS-1$

		}
		return readerQueue;
	}
	
    /**
     * This method gets the reader object for the textfile present either on
     * the local file system or the web.
     * @param props Group's metadata properites string
     * @return BufferReader Object
     */
    private void createReaders(Properties props, String groupName) throws IOException, TranslatorException {
        if(readerQueue != null && readerQueue.size() > 0) {
            return ;
        }
        
        if(props == null) {
            throw new TranslatorException(TextPlugin.Util.getString("TextSynchExecution.Error_obtain_properties_for_group", groupName)); //$NON-NLS-1$
        }
        
        String location = props.getProperty(TextDescriptorPropertyNames.LOCATION);
        
        File[] files = conn.getFiles(location);
        
        if (files != null) {
            // determine if the wild card is used to indicate all files
            // of the specified extension
            for (int i = 0; i<files.length; i++) {
                File f = files[i];
               addReader(f.getName(), new FileInputStream(f));  
            }
        } else {
            // create the URL object
        	try {
	            URL url = new URL(location);
	            // create the connection to the URL
	            URLConnection urlConn = url.openConnection();
	            // establish the connection to the URL
	            urlConn.connect();
	            // get the stream from the connection
	            addReader(location, urlConn.getInputStream());
        	} catch (MalformedURLException e) {
        		throw new TranslatorException(TextPlugin.Util.getString("TextConnection.fileDoesNotExistForGroup", new Object[] {location, groupName})); //$NON-NLS-1$
        	}
        }
 
    }
    
    private void addReader(String fileName, InputStream stream) throws UnsupportedEncodingException {
        
        InputStreamReader inSR = new InputStreamReader(stream, encoding);

        BufferedReader r = new BufferedReader(inSR);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Reading: " + fileName); //$NON-NLS-1$
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
     * @throws TranslatorException 
     */
    private String nextLine(String tableName) throws TranslatorException {
    	if (currentreader == null && readerQueueIndex < readerQueue.size()) {
            // reader queue index is advanced only by the nextReader()
            // method.  Don't do it here.
            currentreader = readerQueue.get(readerQueueIndex);
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
							throw new TranslatorException(msg); 
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
			throw new TranslatorException(e, TextPlugin.Util.getString("TextSynchExecution.Error_reading_text_file", new Object[] { location, e.getMessage() })); //$NON-NLS-1$
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
    
    public String getHeaderLine(String tableName) throws TranslatorException {
    	if (this.headerMsg != null) {
    		return this.headerMsg; 
    	}
    	createReaders(tableName);
    	this.firstLine =  nextLine(tableName);
    	return this.headerMsg;
    }
    
    public String getNextLine(String tableName) throws TranslatorException {
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
