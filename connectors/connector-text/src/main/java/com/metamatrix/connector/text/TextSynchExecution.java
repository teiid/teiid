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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.core.util.StringUtil;

/**
 * The essential part that executes the query. It keeps all the execution
 * states.
 */
public class TextSynchExecution extends BasicExecution implements ResultSetExecution {
    // Command to be executed
    private IQuery cmd;

    private TextConnection txtConn;

    // metadata properties
    private Map metadataProps;

    private ConnectorLogger logger;

    // runtime metadata
    private RuntimeMetadata rm;

    // metadata properties for a given group
    private Properties groupProps = null;

    // Translator for String to Data translation
    private StringToDateTranslator stringToDateTranslator;

    /**
     * Current index to readerQueue
     */   
    private int readerQueueIndex = 0;    
    /**
     * Queue of readers that have yet to be read from.
     */    
    private ArrayList readerQueue = new ArrayList();
    // current Reader object
    private BufferedReader currentreader = null;

    // Line num in the text file
    private int lineNum = 0;

    // List of column widths specified
    private List colWidths = new ArrayList();
    
    // the number of modeled columsn should match the
    // number of colums parsed from the file
    private int numModeledColumns = 0;
    private boolean useModeledColumnCntedit=false;
    
    // If a header row is to be used, this is where the 
    // column names will be saved
    private List headerRow = null;
    
    //whether this execution is canceled
    private volatile boolean canceled;
    
    private int rowsProduced = 0;

	private int[] cols;

    /**
     * Constructor.
     * @param cmd
     * @param txtConn
     */
    public TextSynchExecution(IQuery query, TextConnection txtConn, RuntimeMetadata metadata) {
        this.txtConn = txtConn;
        this.rm = metadata;
        this.logger = this.txtConn.env.getLogger();
        this.metadataProps = this.txtConn.metadataProps;
        this.cmd = query;
    }

    @Override
    public void execute() throws ConnectorException {
        //translate request
        Object translatedRequest = translateRequest(cmd);

        // submit request
        Object response = submitRequest(translatedRequest);

        // translate results
        translateResults(response, cmd);

        cols = getSelectCols(cmd.getSelect().getSelectSymbols());
    }
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
        if (canceled) {
        	throw new ConnectorException("Execution cancelled"); //$NON-NLS-1$
        }
        IQuery query = cmd;
        
        Class[] types = query.getColumnTypes();

        String location = groupProps.getProperty(TextPropertyNames.LOCATION);
        String delimiter = groupProps.getProperty(TextPropertyNames.DELIMITER);
        String qualifier = groupProps.getProperty(TextPropertyNames.QUALIFIER);
        String topLines = groupProps.getProperty(TextPropertyNames.HEADER_LINES);

        // set hasQualifier flag
        boolean hasQualifier = false;
        if (qualifier != null && qualifier.length() > 0) {
            hasQualifier = true;
        }
        
        int numTop = 0;
        if(topLines != null) {
            numTop = Integer.parseInt(topLines);
        }
        
        try {
	        while (true) {
		        BufferedReader br = getCurrentReader();
		        if (br == null) {
		        	return null;                        
		        }
		    
		        String line = br.readLine();
		        
		        // Hit the end of file or the file is empty then
		        // try next reader
		        if(line == null) {
		            advanceToNextReader();
		            lineNum = 0;
		            continue;
		        }
	
		        // check if we have a qualifier defined
		        // if yes check that all qualifiers have been terminated
		        // if not then append the next line (if available)
		        if (hasQualifier) {
		            while (StringUtil.occurrences(line, qualifier)%2!=0) {
		                String nextLine = br.readLine();
		                if (nextLine != null) {
		                    line = line + StringUtil.LINE_SEPARATOR + nextLine;
		                } else {
		                    Object[] params = new Object[] { line }; 
		                                                    
		                    String msg = TextPlugin.Util.getString("TextSynchExecution.Text_has_no_determined_ending_qualifier", params); //$NON-NLS-1$
		                    logger.logError(msg);
		                               
		                    throw new ConnectorException( msg); 
		                }
		            }
		        }
		        lineNum++;
		        
		        //Skip blank lines and any remaining header lines
		        if ( line.length() == 0 || numTop >= lineNum ) continue;
		
		        // Get record from file for one row
		        List record = getRecord(line, delimiter, qualifier, colWidths);
		
		        ++rowsProduced;
		        // Save selected columns into query results
		        
		        if (this.useModeledColumnCntedit && record.size() != numModeledColumns) {
		            Object[] params = new Object[] { new Integer(numModeledColumns), new Integer(record.size()) };
		            String msg = TextPlugin.Util.getString("TextSynchExecution.Input_column_cnt_incorrect", params); //$NON-NLS-1$
		            logger.logError(msg);
		            throw new ConnectorException( msg); 
		        } 
		            
		        return getRow(record, cols, types);
	        }
        } catch(ConnectorException ce) {
            throw ce;
        } catch(Throwable e) {
            Object[] params = new Object[] { location, e.getMessage() };
            logger.logError(TextPlugin.Util.getString("TextSynchExecution.Error_reading_text_file", params), e); //$NON-NLS-1$
            throw new ConnectorException(e, "Error while reading text file: "+location); //$NON-NLS-1$
        }
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
     *
     * @since vhalbert 5.0.1
     */
    private BufferedReader getCurrentReader() throws ConnectorException {
		if (currentreader == null && readerQueueIndex < readerQueue.size()) {
            // reader queue index is advanced only by the nextReader()
            // method.  Don't do it here.
            currentreader = (BufferedReader)readerQueue.get(readerQueueIndex);

			/* Retrieve connector properties so that we can find a 
			 * header row if necessary.
			 */
			String line = null;
			String location = groupProps.getProperty(TextPropertyNames.LOCATION);
			String delimiter = groupProps.getProperty(TextPropertyNames.DELIMITER);
			String qualifier = groupProps.getProperty(TextPropertyNames.QUALIFIER);
			String topLines = groupProps.getProperty(TextPropertyNames.HEADER_LINES);
			String headerLine = groupProps.getProperty(TextPropertyNames.HEADER_ROW);

			int numTop = 0;
			int headerRowNum = 0;
			if (topLines != null && topLines.length() > 0)
				numTop = Integer.parseInt(topLines);
			if (headerLine != null && headerLine.length() > 0)
				headerRowNum = Integer.parseInt(headerLine);

			if (numTop > 0) {
				/* Check to see if the value for HEADER_ROW is greater than
				 * the number of lines to skip.  If it is, it is invalid and
				 * we will use HEADER_LINES instead.
				 */
				if ( headerRowNum > numTop ) {
					Object[] params = new Object[] { TextPropertyNames.HEADER_ROW, new Integer(headerRowNum), new Integer(numTop) }; 
					String msg = TextPlugin.Util.getString("TextSynchExecution.Property_contains_an_invalid_value_Using_value", params); //$NON-NLS-1$
					// TODO: We should include the group name in the log message.
					logger.logWarning(msg);
					
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
							return getCurrentReader();
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
									logger.logError(msg);
									throw new ConnectorException(msg); 
								}
							}
						}
						lineNum++;

						// Skip blank lines
						if (line.length() == 0) continue;

						// Attempt to retrieve column names from header row
						// or last row of header lines.
						if ((headerRowNum > 0 && headerRowNum == lineNum)
								|| (numTop == lineNum)) {
							// This is the header row
							headerRow = getRecord(line, delimiter, qualifier, colWidths);
							break;
						} else if (numTop >= lineNum) continue;
					}
				} catch (Throwable e) {
					Object[] params = new Object[] { location, e.getMessage() };
					logger.logError(TextPlugin.Util.getString("TextSynchExecution.Error_reading_text_file", params), e); //$NON-NLS-1$
					throw new ConnectorException(e, "Error while reading text file: " + location); //$NON-NLS-1$
				}
			}

		}
		return currentreader;
	}

    /**
     * Indicate that we are done with the current reader and we should
     * advance to the next reader.
     *
     * @since vhalbert 5.0.1
     */
    private void advanceToNextReader(){
        currentreader = null;
        readerQueueIndex++;
    }    

    public void close() {
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
        logger.logInfo("TextSynchExecution is successfully closed.");              //$NON-NLS-1$
    }

    public void cancel() {
    	canceled = true;
    }

    /**
     * Translate command.
     * @param request ICommand as request
     * @return Object translated request
     * @throws ConnectorException if error occurs
     */
    protected Object translateRequest(ICommand request) throws ConnectorException {
        if (request == null) {
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Request_is_null")); //$NON-NLS-1$
        }

        if (cmd == null) {
            Object[] params = new Object[] { cmd };
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Error_translating_request", params)); //$NON-NLS-1$
        }

        // Get the columns widths for all the elements in the group.
        IQuery query = (IQuery) request;
        
        /* Defect 13371
         * Can't use the select columns to get the columns widths because we may not be selecting all the columns. Instead,
         * we need to get all the child elements of the group being queried, and get the columns widths of each one of them.
         */
        IFrom from = query.getFrom();
        IGroup group = (IGroup)from.getItems().get(0);
        try {
        	/* We need to create the reader queue before we 
			 * attempt to create the request as we may need 
			 * column names from the header row.
             */
            String groupName = group.getMetadataObject().getFullName();
            Map metadataMap = metadataProps;
            groupProps = (Properties) metadataMap.get(groupName.toUpperCase());

            if(groupProps == null) {
                throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Error_obtain_properties_for_group", groupName)); //$NON-NLS-1$
            }
            try {
                createReaders(groupProps,groupName);
            } catch (IOException ex) {
                Object[] params = new Object[] { groupName, ex.getMessage() };
                throw new ConnectorException(ex, TextPlugin.Util.getString("TextSynchExecution.Unable_get_Reader", params)); //$NON-NLS-1$
            }
            List<Element> elements = group.getMetadataObject().getChildren();
            numModeledColumns = elements.size();
            int[] colWidthArray = new int[elements.size()];
            for (int i = 0; i < colWidthArray.length; i++) {
                Element element = elements.get(i);
                colWidthArray[getColumn(element)] = element.getLength();
            }
            for (int i = 0; i < colWidthArray.length; i++) {
                colWidths.add(new Integer(colWidthArray[i]));
            }
        } catch (ConnectorException e) {
            Object[] params = new Object[] { query, e.getMessage() };
            throw new ConnectorException(e, TextPlugin.Util.getString("TextSynchExecution.Cannot_be_translated_by_the_TextTranslator.", params)); //$NON-NLS-1$
        }
        
        return request;
    }

    /**
     * Submit request and get back the metadata necessary for accessing the text file.
     * @param req
     * @return Object
     */
    protected Object submitRequest(Object req) {
        Properties connprops = txtConn.env.getProperties();
        
        String cnt_edit = (String) connprops.get(TextPropertyNames.COLUMN_CNT_MUST_MATCH_MODEL);
        if (cnt_edit != null && cnt_edit.equalsIgnoreCase(Boolean.TRUE.toString())) {
            this.useModeledColumnCntedit = true;
        }
        return metadataProps;
    }

    /**
     * Translate results.
     * @param response
     * @param cmd
     * @throws ConnectorException
     * @throws ConnectorException
     */
    protected void translateResults(Object response, ICommand cmd) throws ConnectorException, ConnectorException {
        if(!(response instanceof Map)) {
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Not_of_type_Map")); //$NON-NLS-1$
        }

        // get the group metadataID for group present on the command
        IFrom from = ((IQuery) cmd).getFrom();
        List groups = from.getItems();
        IGroup symbol = (IGroup) groups.get(0);
        Group group = symbol.getMetadataObject();

        String groupName = group.getFullName();

        Map metadataMap = (Map) response;
        Properties connProps = this.txtConn.env.getProperties();

        if(connProps.get(TextPropertyNames.DATE_RESULT_FORMATS)  != null) {
            stringToDateTranslator = new StringToDateTranslator(connProps, logger);
        }

        groupProps = (Properties) metadataMap.get(groupName.toUpperCase());
        if(groupProps == null) {
            Object[] params = new Object[] { groupName };
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Error_obtain_properties_for_group", params)); //$NON-NLS-1$
        }

        performQuery(groupProps,groupName);
    }

    /**
     * Helper method for #translateResults(ICommand).
     * @param props Metadata properties for a specific group
     * @throws ConnectorException occurs if IOException happens
     */
    private void performQuery(Properties props,String groupName) throws ConnectorException {
        try {
            createReaders(props,groupName);
        } catch (IOException ex) {
            Object[] params = new Object[] { groupName, ex.getMessage() };
            throw new ConnectorException(ex, TextPlugin.Util.getString("TextSynchExecution.Unable_get_Reader", params)); //$NON-NLS-1$
        }
    }

    /**
     * This method gets the reader object for the textfile present either on
     * the local file system or the web.
     * @param props Group's metadata properites string
     * @return BufferReader Object
     */
    private void createReaders(Properties props,String groupName) throws IOException, ConnectorException {
        if(readerQueue != null && readerQueue.size() > 0) {
            return ;
        }
        String fileName  = props.getProperty(TextPropertyNames.FILE_LOCATION);
        

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
//                new BufferedReader(new FileReader(datafile));
//                logger.logInfo("Reading file: " + fileName); //$NON-NLS-1$
                
            } 
        } else {
            String urlName = props.getProperty(TextPropertyNames.URL_LOCATION);
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
            // get the stream from the commection
            InputStreamReader inSR = new InputStreamReader(conn.getInputStream());
            // place the stream into a buffered reader
            addReader(fileName, inSR);
//            BufferedReader reader = new BufferedReader(inSR);
//            logger.logInfo("Reading URL: " + urlName); //$NON-NLS-1$
           
        }
 
    }
    
    private void addReader(String fileName, File datafile) throws IOException {
        
        FileInputStream fis = new FileInputStream(datafile);
        InputStreamReader inSR = new InputStreamReader(fis);

        BufferedReader r = new BufferedReader(inSR);
                                              //new FileReader(datafile));
        logger.logInfo("Reading file: " + fileName); //$NON-NLS-1$
        readerQueue.add(r);
    }
    
    private void addReader(String fileName, InputStreamReader inSr) throws IOException {
        BufferedReader r = new BufferedReader(inSr);
        logger.logInfo("Reading URL: " + fileName); //$NON-NLS-1$
        readerQueue.add(r);
    }    

    /**
     * Convert selected column names to columns.
     * @param vars List of DataNodeIDs
     * @return Column numbers corresponding to vars
     */
    private int[] getSelectCols(List vars) throws ConnectorException{
        int[] cols = new int[vars.size()];
        for(int i=0; i<vars.size(); i++) {
            cols[i] = getColumn((ISelectSymbol)vars.get(i));
        }
        return cols;
    }

    /**
     * Get column number in Source by ISelectSymbol
     * 
     * An Element is created from the symbol and this method
     * invokes getColumn(Element). 
     *
     * @param symbol Identifier to look up the column
     * @return int The column corresponding to that id
     * @throws ConnectorException
     */
    private int getColumn(ISelectSymbol symbol) throws ConnectorException{
        return this.getColumn(getElementFromSymbol(symbol));
    }

    /**
     * Helper method for getting runtime {@link org.teiid.connector.metadata.runtime.Element} from a
     * {@link org.teiid.connector.language.ISelectSymbol}.
     * @param symbol Input ISelectSymbol
     * @return Element returned metadata runtime Element
     */
    private Element getElementFromSymbol(ISelectSymbol symbol) throws ConnectorException {
        IElement expr = (IElement) symbol.getExpression();
        return expr.getMetadataObject();
    }

    /**
     * Get column number in Source by Element
     * 
     * An attempt is made to parse an int from the Name In Source
	 * for the Element.  If this fails it is assumed that 
     * Name In Source is blank or contains an identifier 
     * name.  If blank, the Element.getMetadataID().getName()
     * is used otherwise, Name In Source is used.
     * @param elem
     * @return int
     * @throws ConnectorException
     */
    private int getColumn(Element elem) throws ConnectorException{
        String colStr = null;
        try {
            colStr = elem.getNameInSource();
        } catch(ConnectorException e) {
            throw new ConnectorException(e);
        }
        try {
        	// If Name In Source is numeric, it is a column number
            return Integer.parseInt(colStr);
        } catch(NumberFormatException e) {
        	// Name In Source was not numeric, so look for a column with a heading matching Name In Source
        	if ( colStr == null ) {
        		colStr = elem.getName();
        	}
        	if ( headerRow == null ) getCurrentReader();
        	if ( headerRow != null ) {
        		for ( int i = 0; i < headerRow.size(); i++ ) {
        			if ( colStr.compareToIgnoreCase((String)headerRow.get(i) )==0) return i;
        		}
                throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Column_not_found_in_header_row", new Object[] {colStr, elem.getFullName() } )); //$NON-NLS-1$
        	}
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Invalid_column_number", new Object[] {colStr, elem.getFullName() } ) ); //$NON-NLS-1$
        }
    }

    /**
     * Open the file, read it, and parse it into memory.
     * @param line the line is being read
     * @param delimiter
     * @param qualifier
     * @param colWidths List of column widths
     * @return List of column values inside the line
     * @throws Exception
     */
    private List getRecord(String line, String delimiter, String qualifier, List colWidths) throws Exception {
        if(delimiter != null) {
            return parseDelimitedLine(line, delimiter, qualifier);
        }
        return parseFixedWidthLine(line, colWidths);
    }

    /**
     * @param line line's length will not be 0
     * @return List of parsed columns
     */
    private List parseDelimitedLine(String line, String delimiter, String qualifier) throws Exception {
        // the case with no qualifier
        if (qualifier == null || qualifier.trim().length()==0) {
            // parse on delimiters
            List strs = new ArrayList();

            int index = -1;
            while(true) {
                int newIndex = line.indexOf(delimiter, index);
                if(newIndex >= 0) {
                    if(index >= 0) {
                        // middle column
                        addUnqualifiedColumnToList(strs, line.substring(index, newIndex));
                    } else {
                        // first column
                        addUnqualifiedColumnToList(strs, line.substring(0, newIndex));
                    }
                    index = newIndex+1;
                } else if(index >= 0) {
                    // end of line
                    addUnqualifiedColumnToList(strs, line.substring(index));
                    break;
                } else {
                    // only one column
                    addUnqualifiedColumnToList(strs, line);
                    break;
                }
            }
            return strs;

        }
        // the case with qualifier

        char delimChar = delimiter.charAt(0);
        char qualChar = qualifier.charAt(0);
        char spaceChar = " ".charAt(0); //$NON-NLS-1$

        List columns = new ArrayList();
        int charIndex = 0;
        int totalChars = line.length();

        while(charIndex < totalChars) {
            // Read character
            char c = line.charAt(charIndex);

            if(c == delimChar) {
                addUnqualifiedColumnToList(columns, null);
                charIndex++;    // past delimiter

            } else if(c == qualChar) {
                int endQualIndex = charIndex;
                while(true) {
                    endQualIndex = line.indexOf(qualChar, endQualIndex+1);
                    if(endQualIndex < 0) {
                        Object[] params = new Object[] { ""+(columns.size()+1), line }; //$NON-NLS-1$
						// changed to Connectorexception so that the exception is thrown to the user
						// and becomes known a problem, rather than just
						// keeping it internally to the server
                        throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Text_has_no_ending_qualifier", params)); //$NON-NLS-1$
                      }
                    // skipping escaped qualifier charachters
                    if(line.length() > endQualIndex+1) {
                        if(line.charAt(endQualIndex+1) == qualChar) {
                            endQualIndex = endQualIndex+1;
                            continue;
                        }
                    }
                    // quoted column
                    columns.add(line.substring(charIndex+1, endQualIndex));
                    charIndex = endQualIndex+1;     // past quoted column

                    // Check for extra characters between quote and delimiter
                    if(charIndex < totalChars && line.charAt(charIndex) != delimChar) {
                        Object[] params = new Object[] { ""+(columns.size()+1), line }; //$NON-NLS-1$
                        String msg = TextPlugin.Util.getString("TextSynchExecution.Text_file_must_have_delimiter", params);//$NON-NLS-1$
//                        Object[] params = new Object[] { location, e.getMessage() };
                        logger.logError(msg); 
						// changed to Connectorexception so that the exception is thrown to the user
						// and becomes known a problem, rather than just
						// keeping it internally to the server
                        throw new ConnectorException(msg); 
                        
                    }

                    charIndex++;    // past delimiter
                    break;
                }

            // skip any space between the delimiter
            // and the qualifier
            } else if(c == spaceChar) {
                charIndex++;

            } else {
                int endColIndex = line.indexOf(delimChar, charIndex);
                if(endColIndex < 0) {
                    // last unquoted column
                    addUnqualifiedColumnToList(columns, line.substring(charIndex));

                    // We know the line is done so we should exit the loop here.
                    // If we didn't exit the loop and just advanced the charIndex,
                    // we would trip the "line ends in <delim>" case below, which
                    // is not valid as we have not ended in a delimiter
                    break;
                }
                    // middle unquoted column
                    addUnqualifiedColumnToList(columns, line.substring(charIndex, endColIndex));
                    charIndex = endColIndex+1;
            }

            // line ends in <delimiter>
            if(charIndex == totalChars) {
                addUnqualifiedColumnToList(columns, null);
            }

        }

        return columns;
    }

    /**
     * Add column value, if null or length is 0, then add null.
     * @param
     * @param
     */
    private static void addUnqualifiedColumnToList(List list, String col) {
        if(col == null || col.length() == 0) {
            list.add(null);
        } else {
            list.add(col);
        }
    }

    /**
     *
     * @param line
     * @param colWidths
     * @return List
     * @throws ConnectorException
     */
    private List parseFixedWidthLine(String line, List colWidths) throws ConnectorException {
        int length = line.length();
        List fields = new ArrayList(colWidths.size());
        Iterator iter = colWidths.iterator();
        int current = 0;
        while(iter.hasNext()) {
            try {
                int width = ((Integer)iter.next()).intValue();
                if(width <= 0) {
                    throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Column_length_must_be_positive")); //$NON-NLS-1$
                }

                int end = current + width;
                if(end > length) {
                    end = length;
                }

                String colValue = line.substring(current, end).trim();
                if(colValue.length() == 0) {
                    fields.add(null);
                } else {
                    fields.add(colValue);
                }
                current += width;
            } catch(Exception e) {
                // ignore and fill column with null
                fields.add(null);
            }
        }

        return fields;
    }

    /**
     * Save selected columns from record into results.
     * @param batch batch to contain the record
     * @param record the record of one row
     * @param columns Columns to save in results
     * @param types Class of all columns' types
     */
    private List getRow(List record, int[] columns, Class[] types) throws ConnectorException {
        List newRecord = new ArrayList(columns.length);
        for(int i=0; i<columns.length; i++) {
            int column = columns[i];
            String value = (String) record.get(column);
            Class type = types[i];
            newRecord.add(convertString(value, type));
        }
        return newRecord;
    }

    /**
     * Convert String to Object of correct type.
     * @param value Input Value
     * @param type Input type
     * @return Object translated Object from String
     */
    private Object convertString(String value, Class type) throws ConnectorException {
        if (value==null) {
            return null;
        }

        if (type == TypeFacility.RUNTIME_TYPES.STRING) {
            return value;
        }

        if (java.util.Date.class.isAssignableFrom(type)) {
        	//check defaults first
        	try {
        		return Timestamp.valueOf(value);
        	} catch (IllegalArgumentException e) {
        		
        	}
        	try {
        		return Date.valueOf(value);
        	} catch (IllegalArgumentException e) {
        		
        	}
        	try {
        		return Time.valueOf(value);
        	} catch (IllegalArgumentException e) {
        		
        	}
        	//check for overrides
        	if (stringToDateTranslator!=null && stringToDateTranslator.hasFormatters()) {
	            try {
	                 return new Timestamp(stringToDateTranslator.translateStringToDate(value).getTime());
	            }catch(ParseException ex) {
	                throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Unable_translate_String_to_Date", new Object[] { ex.getMessage() })); //$NON-NLS-1$
	            }
        	}
        }
        return value;
    }

}

