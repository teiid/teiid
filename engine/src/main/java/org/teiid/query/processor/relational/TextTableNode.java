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

package org.teiid.query.processor.relational;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.TextTable.TextColumn;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;

/**
 * Handles text file processing.
 * 
 * TODO: allow for a configurable line terminator
 */
public class TextTableNode extends SubqueryAwareRelationalNode {

	private TextTable table;
	
	//initialized state
	private int skip = 0;
	private int header = -1;
	private boolean noQuote;
	private char quote;
	private char delimiter;
	private int lineWidth;
    private int[] projectionIndexes;
    private Map<String, List<String>> parentLines;
	
    //per file state
	private BufferedReader reader;
	private int textLine = 0;
	private Map<String, Integer> nameIndexes;
	private String systemId;
	private int rowNumber;

	private boolean cr;
	private boolean eof;
	
	private boolean running;
	private TeiidProcessingException asynchException;

	private int limit = -1;
	
	public TextTableNode(int nodeID) {
		super(nodeID);
	}
	
	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);
		if (projectionIndexes != null) {
			return;
		}
		if (table.getSkip() != null) {
			skip = table.getSkip();
		}
		if (table.getHeader() != null) {
			skip = Math.max(table.getHeader(), skip);
			header = table.getHeader() - 1;
		}
		if (table.isFixedWidth()) {
			for (TextColumn col : table.getColumns()) {
				lineWidth += col.getWidth();
			}
		} else {
			if (table.getDelimiter() == null) {
				delimiter = ',';
			} else {
				delimiter = table.getDelimiter();
			}
			if (table.getQuote() == null) {
				quote = '"';
			} else {
				noQuote = table.isEscape();
				quote = table.getQuote();
			}
			for (TextColumn column : table.getColumns()) {
				if (column.getSelector() != null) {
					if (parentLines == null) {
						parentLines = new HashMap<String, List<String>>();
					}
					parentLines.put(column.getSelector(), null);
				}
	        }
			lineWidth = table.getColumns().size() * DataTypeManager.MAX_STRING_LENGTH;
		}
        Map<Expression, Integer> elementMap = createLookupMap(table.getProjectedSymbols());
        this.projectionIndexes = getProjectionIndexes(elementMap, getElements());
	}
	
	@Override
	public void closeDirect() {
		super.closeDirect();
		reset();
	}
	
	@Override
	public void reset() {
		super.reset();
		if (this.reader != null) {
			try {
				this.reader.close();
			} catch (IOException e) {
			}
			this.reader = null;
		}
		this.nameIndexes = null;
		this.textLine = 0;
		this.rowNumber = 0;
		this.cr = false;
		this.eof = false;
		if (this.parentLines != null) {
			for (Map.Entry<String, List<String>> entry : this.parentLines.entrySet()) {
				entry.setValue(null);
			}
		}
		this.running = false;
		this.asynchException = null;
		this.limit = -1;
	}
	
	public void setTable(TextTable table) {
		this.table = table;
	}

	@Override
	public TextTableNode clone() {
		TextTableNode clone = new TextTableNode(getID());
		this.copyTo(clone);
		clone.setTable(table);
		return clone;
	}
	
	@Override
	public void open() throws TeiidComponentException, TeiidProcessingException {
		super.open();
		if (getParent() instanceof LimitNode) {
			LimitNode parent = (LimitNode)getParent();
			if (parent.getLimit() > 0) {
				limit = parent.getLimit() + parent.getOffset();
			}
		}
	}

	@Override
	protected synchronized TupleBatch nextBatchDirect() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		
		if (reader == null) {
			initReader();
		}

		if (reader == null) {
			terminateBatches();
			return pullBatch();
		}
		
		if (isLastBatch()) {
			return pullBatch();
		}
		
		if (isBatchFull()) {
			TupleBatch result = pullBatch();
			processAsynch(); // read ahead
			return result;
		}
		
		if (asynchException != null) {
			throw asynchException;
		}
		
		processAsynch();
		
		if (this.getContext().getWorkItem() == null) {
			//this is for compatibility with engine tests that are below the level of using the work item
			synchronized (this) {
				while (running) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						throw new TeiidRuntimeException(e);
					}
				}
			}
		}
		
		throw BlockedException.block("Blocking on results from file processing."); //$NON-NLS-1$
	}

	private void processAsynch() {
		if (!running) {
			running = true;
			getContext().getExecutor().execute(new Runnable() {
				@Override
				public void run() {
					try {
						process();
					} catch (TeiidProcessingException e) {
						asynchException = e;
					} finally {
						running = false;
						RequestWorkItem workItem = TextTableNode.this.getContext().getWorkItem();
						if (workItem != null) {
							workItem.moreWork();
						} else {
							synchronized (TextTableNode.this) {
								TextTableNode.this.notifyAll();
							}
						}
					}
				}
			});
		}
	}

	private void process() throws TeiidProcessingException {
		while (true) {
			synchronized (this) {
				if (isBatchFull()) {
					return;
				}
				StringBuilder line = readLine(lineWidth, table.isFixedWidth());
				
				if (line == null) {
					terminateBatches();
					break;
				}
				
				String parentSelector = null;
				if (table.getSelector() != null) {
					if (line.length() < table.getSelector().length()) {
						continue;
					}
					if (!line.substring(0, table.getSelector().length()).equals(table.getSelector())) {
						if (parentLines == null) {
							continue; //doesn't match any selector
						}
						parentSelector = line.substring(0, table.getSelector().length());
						
						if (!parentLines.containsKey(parentSelector)) {
							continue; //doesn't match any selector
						} 
					}
				}
				
				List<String> vals = parseLine(line);
				
				if (parentSelector != null) {
					this.parentLines.put(parentSelector, vals);
					continue;
				} else if (table.getSelector() != null && !table.getSelector().equals(vals.get(0))) {
					continue;
				}
				
				rowNumber++;
				
				List<Object> tuple = new ArrayList<Object>(projectionIndexes.length);
				for (int output : projectionIndexes) {
					TextColumn col = table.getColumns().get(output);
					String val = null;
					int index = output;
					
					if (col.isOrdinal()) {
						tuple.add(rowNumber);
						continue;
					}
					
					if (col.getSelector() != null) {
						vals = this.parentLines.get(col.getSelector());
						index = col.getPosition() - 1;
					} else if (nameIndexes != null) {
						index = nameIndexes.get(col.getName());
					}
					if (vals == null || index >= vals.size()) {
						//throw new TeiidProcessingException(QueryPlugin.Util.getString("TextTableNode.no_value", col.getName(), textLine, systemId)); //$NON-NLS-1$
						tuple.add(null);
						continue;
					} 
					val = vals.get(index);
					try {
						tuple.add(DataTypeManager.transformValue(val, table.getColumns().get(output).getSymbol().getType()));
					} catch (TransformationException e) {
						 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30176, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30176, col.getName(), textLine, systemId));
					}
				}
				addBatchRow(tuple);
				
				if (rowNumber == limit) {
					terminateBatches();
					break;
				}
			}
		}
	}

	private StringBuilder readLine(int maxLength, boolean exact) throws TeiidProcessingException {
		if (eof) {
			return null;
		}
		StringBuilder sb = new StringBuilder(exact ? maxLength : (maxLength >> 4));
		while (true) {
			char c = readChar();
			if (c == '\n') {
				if (sb.length() == 0) {
					if (eof) {
						return null;
					}
					if (table.isUsingRowDelimiter()) {
						continue; //skip empty lines
					}
				}
				if (table.isUsingRowDelimiter()) {
					return sb;
				}
		    }
		    sb.append(c);
		    if (exact && sb.length() == maxLength && !table.isUsingRowDelimiter()) {
		    	return sb;
		    }
		    if (sb.length() > maxLength) {
		    	if (exact) {
		    		sb.deleteCharAt(sb.length() - 1);
		    		//we're not forcing them to fully specify the line, so just drop the rest
		    		//TODO: there should be a max read length
		    		while (readChar() != '\n') {
		    			
		    		}
		    		return sb;
		    	}
		    	//protects non-fixed width processing from run-away values
		    	//TODO it is possible that string values could be desired that are longer than the max and/or returned as clobs
		    	 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30178, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30178, textLine+1, systemId, maxLength));
		    }
		}
	}
	
	private char readChar() throws TeiidProcessingException {
		try {
			int c = reader.read();
		    if (cr) {
				if (c == '\n') {
				    c = reader.read();
				}
				cr = false;
		    }
		    switch (c) {
		    case '\r':
				cr = true;
				textLine++;
				return '\n';
		    case -1:
		    	eof = true;
		    	textLine++;
				return '\n';
		    case '\n':		
				textLine++;
				return '\n';
		    }
		    return (char)c;
		} catch (IOException e) {
			throw new TeiidProcessingException(QueryPlugin.Event.TEIID30179, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30179, systemId));
		}
	}

	private void initReader() throws ExpressionEvaluationException,
			BlockedException, TeiidComponentException, TeiidProcessingException {
		
		setReferenceValues(this.table);
		ClobType file = (ClobType)getEvaluator(Collections.emptyMap()).evaluate(table.getFile(), null);
		if (file == null) {
			return;
		}
		
		//get the reader
		try {
			this.systemId = "Unknown"; //$NON-NLS-1$
			if (file.getReference() instanceof ClobImpl) {
				this.systemId = ((ClobImpl)file.getReference()).getStreamFactory().getSystemId();
				if (this.systemId == null) {
					this.systemId = "Unknown"; //$NON-NLS-1$
				}
			} 
			Reader r = file.getCharacterStream();
			if (!(r instanceof BufferedReader)) {
				reader = new BufferedReader(r);
			} else {
				reader = (BufferedReader)r;
			}
		} catch (SQLException e) {
			 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30180, e);
		}
		
		//process the skip field
		if (skip <= 0) {
			return;
		}
		while (textLine < skip) {
			boolean isHeader = textLine == header;
			if (isHeader) {
				StringBuilder line = readLine(DataTypeManager.MAX_STRING_LENGTH * 16, false);
				if (line == null) { //just return an empty batch
					reset();
					return;
				}
				processHeader(parseLine(line));
			} else {
				while (readChar() != '\n') {
	    			
	    		}
			}
		}
	}

	private void processHeader(List<String> line) throws TeiidProcessingException {
		nameIndexes = new HashMap<String, Integer>();
		this.lineWidth = DataTypeManager.MAX_STRING_LENGTH * line.size();
		for (String string : line) {
			if (string == null) {
				continue;
			}
			nameIndexes.put(string.toUpperCase(), nameIndexes.size());
		}
		for (TextColumn col : table.getColumns()) {
			if (col.isOrdinal()) {
				continue;
			}
			Integer index = nameIndexes.get(col.getName().toUpperCase());
			if (index == null) {
				 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30181, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30181, col.getName(), systemId));
			}
			nameIndexes.put(col.getName(), index);
		}
	}

	private List<String> parseLine(StringBuilder line) throws TeiidProcessingException {
		if (table.isFixedWidth()) {
			return parseFixedWidth(line);
		} 
		return parseDelimitedLine(line);
	}

	private List<String> parseDelimitedLine(StringBuilder line) throws TeiidProcessingException {
		ArrayList<String> result = new ArrayList<String>();
		StringBuilder builder = new StringBuilder();
		boolean escaped = false;
		boolean wasQualified = false;
		boolean qualified = false;
		while (true) {
			if (line == null) {
				if (escaped) {
					//allow for escaped new lines
					if (cr) {
						builder.append('\r'); 
					}
					builder.append('\n'); 
					escaped = false;
					line = readLine(lineWidth, false);
					continue;
				} 
				if (!qualified) {
					//close the last entry
					addValue(result, wasQualified, builder.toString());
					return result;
				} 
				line = readLine(lineWidth, false);
				if (line == null) {
					 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30182, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30182, systemId));
				}
			}
			for (int i = 0; i < line.length(); i++) {
				char chr = line.charAt(i);
				if (chr == delimiter) {
					if (escaped || qualified) {
						builder.append(chr);
						escaped = false;
					} else {
						addValue(result, wasQualified, builder.toString());
						wasQualified = false;
						builder = new StringBuilder();  //next entry
					} 
				} else if (chr == quote) {
					if (noQuote) { 	//it's the escape char
						if (escaped) {
							builder.append(quote);
						} 
						escaped = !escaped;
					} else {
						if (qualified) {
							qualified = false;
						} else {
							if (wasQualified) {
								qualified = true;
								builder.append(chr);
							} else {
								if (builder.toString().trim().length() != 0) {
									 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30183, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30183, textLine, systemId));
								}
								qualified = true;
								builder = new StringBuilder(); //start the entry over
								wasQualified = true;
							}
						}
					}
				} else {
					if (escaped) {
						//don't understand other escape sequences yet
						 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30184, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30184, chr, textLine, systemId));
					}
					if (wasQualified && !qualified) {
						if (!Character.isWhitespace(chr)) {
							 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30183, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30183, textLine, systemId));
						}
						//else just ignore
					} else {
						builder.append(chr);
					}
				}
			}
			line = null;
		}
	}

	private void addValue(ArrayList<String> result, boolean wasQualified, String val) {
		if (!wasQualified) {
			val = val.trim();
			if (val.length() == 0) {
				val = null;
			}
		}
		result.add(val);
	}

	private List<String> parseFixedWidth(StringBuilder line) {
		ArrayList<String> result = new ArrayList<String>();
		int beginIndex = 0;
		for (TextColumn col : table.getColumns()) {
			if (beginIndex >= line.length()) {
				result.add(null);
			} else {
				String val = new String(line.substring(beginIndex, Math.min(line.length(), beginIndex + col.getWidth())));
				addValue(result, col.isNoTrim(), val);
				beginIndex += col.getWidth();
			}		
		}
		return result;
	}
	
	@Override
	protected Collection<? extends LanguageObject> getObjects() {
		return Arrays.asList(this.table.getFile());
	}
	
}
