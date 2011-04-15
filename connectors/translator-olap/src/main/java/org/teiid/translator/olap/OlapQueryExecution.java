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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

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

/**
 * Executes the given MDX and packs the results into an array
 */
public class OlapQueryExecution implements ProcedureExecution {

	protected Command command;
    protected OlapConnection connection;
    protected ExecutionContext context;
    protected OlapExecutionFactory executionFactory;
    private OlapStatement stmt;
    private CellSet cellSet;
    private CellSetAxis cols;
    private int colWidth;
    private ListIterator<Position> iterator;
    
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
			stmt = this.connection.createStatement();
			
			cellSet = stmt.executeOlapQuery(mdxQuery);
			CellSetAxis rows = this.cellSet.getAxes().get(Axis.ROWS.axisOrdinal());
			iterator = rows.iterator();
			cols = cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal());
	    	colWidth = rows.getAxisMetaData().getHierarchies().size() + this.cols.getPositions().size();
		} catch (SQLException e) {
			throw new TranslatorException(e);
		} 
	}
	
	@Override
	public void cancel() throws TranslatorException {
		try {
			OlapStatement olapStatement = this.stmt;
			if (olapStatement != null) {
				olapStatement.cancel();
			}
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}		
	}

	@Override
	public void close() {
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
    	if (!iterator.hasNext()) {
    		return null;
    	}
    	Position nextRow = iterator.next();
    	Object[] result = new Object[colWidth];
    	int i = 0;
    	// add in rows axis
		List<Member> members = nextRow.getMembers();
		for (Member member:members) {
			String columnName = member.getHierarchy().getName();
			result[i++] = columnName;
		}

		// add col axis
		for (Position colPos : cols) {
			Cell cell = cellSet.getCell(colPos, nextRow);
			result[i++] = cell.getValue();
		}	
		ArrayList<Object[]> results = new ArrayList<Object[]>();
		results.add(result);
		return results;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }
    
}
