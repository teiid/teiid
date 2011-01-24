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
package org.teiid.translator.olap;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Member;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class OlapQueryExecution implements ProcedureExecution {

	protected Command command;
    protected OlapConnection connection;
    protected ExecutionContext context;
    protected OlapExecutionFactory executionFactory;
    private OlapStatement stmt;
    private Source returnValue;

    
	public OlapQueryExecution(Command command, OlapConnection connection, ExecutionContext context, OlapExecutionFactory executionFactory) {
		this.command = command;
		this.connection = connection;
		this.context = context;
		this.executionFactory = executionFactory;
	}
	
	@Override
	public void execute() throws TranslatorException {
		try {
			Call procedure = (Call) this.command;
			List<Argument> arguments = procedure.getArguments();
			String mdxQuery = (String) arguments.get(0).getArgumentValue().getValue();
			OlapStatement stmt = this.connection.createStatement();
			
			CellSet cellSet = stmt.executeOlapQuery(mdxQuery);
			this.returnValue = new StreamSource(new MdxResultsReader(cellSet));
			

		} catch (SQLException e) {
			throw new TranslatorException(e);
		} 
	}
	
	@Override
	public void cancel() throws TranslatorException {
		try {
			if (this.stmt != null) {
				this.stmt.cancel();
			}
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}		
	}

	@Override
	public synchronized void close() {
		try {
			if (this.stmt != null) {
				this.stmt.close();
				this.stmt = null;
			}
		} catch (SQLException e) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
		}
	}
	
    @Override
    public List<?> next() throws TranslatorException {
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return Arrays.asList(this.returnValue);
    }
    
    static class MdxResultsReader extends Reader {
    	private CellSet cellSet;
    	private ListIterator<Position> rows;
    	private Position nextRow;
    	private boolean closed = false; 
    	private char[] buffer;
    	private int index = 0;
    	
    	public MdxResultsReader(CellSet cellSet) {
    		this.cellSet = cellSet;
			CellSetAxis rowAxis = cellSet.getAxes().get(Axis.ROWS.axisOrdinal());
			this.rows = rowAxis.iterator();
			if (this.rows.hasNext()) {
				this.nextRow = this.rows.next();
				this.buffer = "<resultset>".toCharArray();
			}
    	}
    	
    	private String readNextRow() {
    		if (this.nextRow == null) {
    			return null;
    		}
    		
    		StringBuilder sb = new StringBuilder();
			CellSetAxis cols = cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal());
			sb.append("<row>");

			// add in rows axis
			List<Member> members = nextRow.getMembers();
			for (Member member:members) {
				String columnName = member.getHierarchy().getName();
				columnName = columnName.replace(' ', '_');
				sb.append('<').append(columnName).append('>');
				sb.append(member.getName());
				sb.append("</").append(columnName).append('>');
			}

			// add col axis
			for (Position colPos : cols) {
				Cell cell = cellSet.getCell(colPos, nextRow);
				String columnName = colPos.getMembers().get(0).getName();
				columnName = columnName.replace(' ', '_');
				sb.append('<').append(columnName).append('>');
				sb.append(cell.getValue());
				sb.append("</").append(columnName).append('>');
			}				
			sb.append("</row>");
			
			// advance the cursor to next row.
			if (this.rows.hasNext()) {
				this.nextRow = this.rows.next();
			}
			else {
				this.nextRow = null;
			}
			return sb.toString();
    	}
    	
		@Override
		public void close() throws IOException {
		}

		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
			int availble = this.buffer.length - this.index;
			if (availble == 0) {
				String next = readNextRow();
				if (next == null) {
					if (!this.closed) {
						this.buffer = "</resultset>".toCharArray();
						this.closed = true;
					}
					else {
						return -1;
					}
				}
				else {
					this.buffer = next.toCharArray();
				}
				this.index = 0;
				availble = this.buffer.length;
			}
			len = (availble > len) ? len : availble;
			System.arraycopy(this.buffer, this.index, cbuf, off, len);
			this.index = this.index + len;
			return len;
		}
    }
}
