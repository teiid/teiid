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

package org.teiid.resource.adapter.text;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.language.ColumnReference;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.NamedTable;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.BasicExecution;
import org.teiid.resource.cci.DataNotAvailableException;
import org.teiid.resource.cci.ResultSetExecution;
import org.teiid.resource.cci.TypeFacility;

/**
 * The essential part that executes the query. It keeps all the execution
 * states.
 */
public class TextSynchExecution extends BasicExecution implements ResultSetExecution {
    // Command to be executed
    private Select cmd;

    private TextExecutionFactory config;

    // Translator for String to Data translation
    private StringToDateTranslator stringToDateTranslator;

    // List of column widths specified
    private List colWidths = new ArrayList();
    
    // the number of modeled columsn should match the
    // number of colums parsed from the file
    private int numModeledColumns = 0;
    
    // If a header row is to be used, this is where the 
    // column names will be saved
    private List headerRow = null;
    
    //whether this execution is canceled
    private volatile boolean canceled;
    
    private int rowsProduced = 0;

	private int[] cols;
	
	private TextConnectionImpl connection;

    /**
     * Constructor.
     * @param cmd
     * @param txtConn
     */
    public TextSynchExecution(TextExecutionFactory config, Select query, TextConnectionImpl connection) {
        this.config = config;
        this.cmd = query;
        this.connection = connection;
        
        if(this.config.getDateResultFormats() != null) {
            stringToDateTranslator = new StringToDateTranslator(this.config);
        }        
    }

    @Override
    public void execute() throws ConnectorException {
        //translate request
        translateRequest(cmd);

        cols = getSelectCols(cmd.getDerivedColumns());
    }
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
        if (canceled) {
        	throw new ConnectorException("Execution cancelled"); //$NON-NLS-1$
        }
        Select query = cmd;
        NamedTable group = (NamedTable)query.getFrom().get(0);
        String tableName = group.getMetadataObject().getFullName().toUpperCase();
        Properties groupProps = this.config.metadataProps.get(tableName);
        
        Class[] types = query.getColumnTypes();

        String location = groupProps.getProperty(TextDescriptorPropertyNames.LOCATION);
        String delimiter = groupProps.getProperty(TextDescriptorPropertyNames.DELIMITER);
        String qualifier = groupProps.getProperty(TextDescriptorPropertyNames.QUALIFIER);
        
        try {
	        while (true) {
		        String line = this.connection.getNextLine(tableName);
		        // Hit the end of file or the file is empty then
		        if(line == null) {
		        	return null;
		        }
		
		        // Get record from file for one row
		        List record = getRecord(line, delimiter, qualifier, colWidths);
		
		        ++rowsProduced;
		        // Save selected columns into query results
		        
		        if (this.config.isEnforceColumnCount() && record.size() != numModeledColumns) {
		            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Input_column_cnt_incorrect", new Object[] { new Integer(numModeledColumns), new Integer(record.size()) })); //$NON-NLS-1$ 
		        } 
		            
		        return getRow(record, cols, types);
	        }
        } catch(ConnectorException ce) {
            throw ce;
        } catch(Throwable e) {
            throw new ConnectorException(e, TextPlugin.Util.getString("TextSynchExecution.Error_reading_text_file", new Object[] { location, e.getMessage() })); //$NON-NLS-1$
        }
    }
        

    public void close() {
		this.connection.close();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "TextSynchExecution is successfully closed.");//$NON-NLS-1$
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
    protected Object translateRequest(Command request) throws ConnectorException {
        if (request == null) {
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Request_is_null")); //$NON-NLS-1$
        }

        if (cmd == null) {
            Object[] params = new Object[] { cmd };
            throw new ConnectorException(TextPlugin.Util.getString("TextSynchExecution.Error_translating_request", params)); //$NON-NLS-1$
        }

        // Get the columns widths for all the elements in the group.
        Select query = (Select) request;
        
        /* Defect 13371
         * Can't use the select columns to get the columns widths because we may not be selecting all the columns. Instead,
         * we need to get all the child elements of the group being queried, and get the columns widths of each one of them.
         */
        NamedTable group = (NamedTable)query.getFrom().get(0);
        try {
        	/* We need to create the reader queue before we 
			 * attempt to create the request as we may need 
			 * column names from the header row.
             */
        	String headerLine = this.connection.getHeaderLine(group.getMetadataObject().getFullName().toUpperCase());
            if (headerLine != null) {
                String tableName = group.getMetadataObject().getFullName().toUpperCase();
                Properties groupProps = this.connection.getMetadataProperties().get(tableName);            	
                String delimiter = groupProps.getProperty(TextDescriptorPropertyNames.DELIMITER);
                String qualifier = groupProps.getProperty(TextDescriptorPropertyNames.QUALIFIER);            	
                this.headerRow = getRecord(headerLine, delimiter, qualifier, colWidths);
            }
            
            List<Column> elements = group.getMetadataObject().getColumns();
            numModeledColumns = elements.size();
            int[] colWidthArray = new int[elements.size()];
            for (int i = 0; i < colWidthArray.length; i++) {
                Column element = elements.get(i);
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
     * Convert selected column names to columns.
     * @param vars List of DataNodeIDs
     * @return Column numbers corresponding to vars
     */
    private int[] getSelectCols(List vars) throws ConnectorException{
        int[] cols = new int[vars.size()];
        for(int i=0; i<vars.size(); i++) {
            cols[i] = getColumn((DerivedColumn)vars.get(i));
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
    private int getColumn(DerivedColumn symbol) throws ConnectorException{
        return this.getColumn(getElementFromSymbol(symbol));
    }

    /**
     * Helper method for getting runtime {@link org.teiid.connector.metadata.runtime.Element} from a
     * {@link org.teiid.connector.language.DerivedColumn}.
     * @param symbol Input ISelectSymbol
     * @return Element returned metadata runtime Element
     */
    private Column getElementFromSymbol(DerivedColumn symbol) {
        ColumnReference expr = (ColumnReference) symbol.getExpression();
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
    private int getColumn(Column elem) throws ConnectorException{
        String colStr = elem.getNameInSource();
        try {
        	// If Name In Source is numeric, it is a column number
            return Integer.parseInt(colStr);
        } catch(NumberFormatException e) {
        	// Name In Source was not numeric, so look for a column with a heading matching Name In Source
        	if ( colStr == null ) {
        		colStr = elem.getName();
        	}
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
    private List getRecord(String line, String delimiter, String qualifier, List colWidths) throws ConnectorException {
        if(delimiter != null) {
            return parseDelimitedLine(line, delimiter, qualifier);
        }
        return parseFixedWidthLine(line, colWidths);
    }

    /**
     * @param line line's length will not be 0
     * @return List of parsed columns
     */
    private List parseDelimitedLine(String line, String delimiter, String qualifier) throws ConnectorException {
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

