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
package org.teiid.translator.hbase;

import static org.teiid.language.SQLConstants.Reserved.AS;
import static org.teiid.language.SQLConstants.Reserved.DISTINCT;
import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.HAVING;
import static org.teiid.language.SQLConstants.Reserved.INTO;
import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.language.SQLConstants.Reserved.WHERE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.XMLType;
import org.teiid.language.Argument;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.hbase.phoenix.PColumnTeiidImpl;
import org.teiid.translator.hbase.phoenix.PNameTeiidImpl;
import org.teiid.translator.hbase.phoenix.PTableTeiidImpl;
import org.teiid.translator.hbase.phoenix.PhoenixUtils;

public class SQLConversionVisitor extends SQLStringVisitor implements SQLStringVisitor.Substitutor {
	
	public static final String INSERT = "UPSERT";
	
	private HBaseExecutionFactory executionFactory ;
	private ExecutionContext context ;
	
	private boolean replaceWithBinding = false;
	
	// used to map hbase table to phoenix
//	private PTable ptable;
	
	private boolean prepared;
	
	private List preparedValues = new ArrayList();
	
	private Map<Column, PColumn> columnsMap = new HashMap<Column, PColumn> ();
	private Map<Table, PTable> tablesMap = new HashMap<Table, PTable> ();
	private List<String> mappingDDLlist = new ArrayList<String>();
	
//	private String mappingDDL; 
	
	public SQLConversionVisitor(HBaseExecutionFactory ef) {
		this.executionFactory = ef;
		this.prepared = executionFactory.usePreparedStatements();
	}

	public void setExecutionContext(ExecutionContext context) {
		this.context = context;
	}
	
	List getPreparedValues() {
        return this.preparedValues;
    }
	
	public boolean isPrepared() {
		return prepared;
	}
    
    public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}
	
	public List<String> getMappingDDLList() {
		return mappingDDLlist;
	}

	@Override
	public void visit(Select obj) {
				
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
			phoenixTableMapping(obj.getFrom());
		}
		
    	if (obj.getWith() != null) {
    		append(obj.getWith());
    	}
		buffer.append(SELECT).append(Tokens.SPACE);
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }
        if (useSelectLimit() && obj.getLimit() != null) {
            append(obj.getLimit());
            buffer.append(Tokens.SPACE);
        }
        append(obj.getDerivedColumns());
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
        	buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);      
            append(obj.getFrom());
//        	buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);    
//        	buffer.append(ptable.getTableName().getString()).append(Tokens.SPACE).append(AS).append(Tokens.SPACE).append(ptable.getName().getString());
        }
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }
        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE)
                  .append(HAVING)
                  .append(Tokens.SPACE);
            append(obj.getHaving());
        }
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
        if (!useSelectLimit() && obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }
    }
	
	@Override
	public void visit(Insert obj) {
		
		phoenixTableMapping(obj.getTable());
		
		buffer.append(INSERT).append(Tokens.SPACE);
		buffer.append(INTO).append(Tokens.SPACE);
		
		PTable ptable = tablesMap.get(obj.getTable().getMetadataObject());
    	buffer.append(ptable.getTableName().getString());
    	
		buffer.append(Tokens.SPACE).append(Tokens.LPAREN);

		this.shortNameOnly = true;
		append(obj.getColumns());
		this.shortNameOnly = false;

		buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        append(obj.getValueSource());
	}
	
	private void phoenixTableMapping(NamedTable namedtable) {
		
		Table table = namedtable.getMetadataObject();
		
		String tname = table.getProperty(HBaseMetadataProcessor.TABLE, false);
		PName tableName = PNameTeiidImpl.makePName(tname);
		
		List<PColumn> columns = new ArrayList<PColumn>();
		for(Column column : table.getColumns()) {
			PColumn pcolumn;
			String cell = column.getProperty(HBaseMetadataProcessor.CELL, false);
			String[] qua =  cell.split(":");
			if(qua.length != 2) {
				pcolumn = new PColumnTeiidImpl(PNameTeiidImpl.makePName(cell), null, convertType(column));
			} else {
				pcolumn = new PColumnTeiidImpl(PNameTeiidImpl.makePName(qua[1]), PNameTeiidImpl.makePName(qua[0]), convertType(column));
			}	
			columns.add(pcolumn);
			columnsMap.put(column, pcolumn);
		}
		
		PTable ptable = PTableTeiidImpl.makeTable(tableName, columns);
		
		tablesMap.put(table, ptable);
		
		mappingDDLlist.add(PhoenixUtils.hbaseTableMappingDDL(ptable));	
	}

	
	private void phoenixTableMapping(List<TableReference> list) {

		for(TableReference reference : list) {
			if(reference instanceof NamedTable) {
				NamedTable namedtable = (NamedTable) reference;
				phoenixTableMapping(namedtable);
			} 
		}
	
		
	}

	/*
	 * Convert teiid type to phoenix type, the following types not support by phoenix
	 *    object -> Any 
	 *    blob   -> java.sql.Blob
	 *    clob   -> java.sql.Clob
	 *    xml    -> java.sql.SQLXML
	 */
	private PDataType convertType(Column column) {
		
		Class<?> clas = column.getJavaType();
		
		if(clas.equals(String.class)){
			return PDataType.VARCHAR;
		} else if (clas.equals(BinaryType.class)){
			return PDataType.VARBINARY;
		} else if (clas.equals(Character.class)){
			return PDataType.VARCHAR;
		} else if (clas.equals(Boolean.class)){
			return PDataType.BOOLEAN;
		} else if (clas.equals(Byte.class)){
			return PDataType.TINYINT;
		} else if (clas.equals(Short.class)){
			return PDataType.SMALLINT;
		} else if (clas.equals(Integer.class)){
			return PDataType.INTEGER;
		} else if (clas.equals(Long.class)){
			return PDataType.LONG;
		} else if (clas.equals(BigInteger.class)){
			return PDataType.LONG;
		} else if (clas.equals(Float.class)){
			return PDataType.FLOAT;
		} else if (clas.equals(Double.class)){
			return PDataType.DOUBLE;
		} else if (clas.equals(BigDecimal.class)){
			return PDataType.DECIMAL;
		} else if (clas.equals(Date.class)){
			return PDataType.DATE;
		} else if (clas.equals(Time.class)){
			return PDataType.TIME;
		} else if (clas.equals(Timestamp.class)){
			return PDataType.TIMESTAMP;
		} else if (clas.equals(BlobType.class)){
			return PDataType.UNSIGNED_LONG_ARRAY;
		} else if (clas.equals(ClobType.class)){
			return PDataType.UNSIGNED_LONG_ARRAY;
		} else if (clas.equals(XMLType.class)){
			return PDataType.UNSIGNED_LONG_ARRAY;
		} else if (clas.equals(Object.class)){
			return PDataType.UNSIGNED_LONG_ARRAY;
		} 
		
		return null;
	}
	
	@Override
	public void visit(Literal obj) {
		
		if(isPrepared() && ((replaceWithBinding && obj.isBindEligible()) || executionFactory.isBindEligible(obj))){
			buffer.append(UNDEFINED_PARAM);
			preparedValues.add(obj);
		} else {
			super.visit(obj);
		}
	}

	@Override
    public void visit(ExpressionValueSource obj) {
        replaceWithBinding = true;
        super.visit(obj);
    }

	@Override
	public void visit(DerivedColumn obj) {
		replaceWithBinding = false;
		append(obj.getExpression());
	}

	@Override
	public void visit(ColumnReference obj) {
		String groupName = getGroupName(obj, !shortNameOnly);
		Column column = obj.getMetadataObject();
		PColumn pcolumn = columnsMap.get(column);
		if(null != groupName){
			buffer.append(groupName + Tokens.DOT + pcolumn.getName().getString());
		} else {
			buffer.append(pcolumn.getName().getString());
		}
	}
	
	@Override
	public void visit(NamedTable obj) {
		
		PTable ptable = tablesMap.get(obj.getMetadataObject());
		buffer.append(ptable.getTableName().getString());
		String groupName = getGroupName(obj, !shortNameOnly);
        if (groupName != null) {
            buffer.append(Tokens.SPACE);
            if (useAsInGroupAlias()){
                buffer.append(AS).append(Tokens.SPACE);
            }
        	buffer.append(groupName);
        }
	}
	
	private String getGroupName(NamedTable group, boolean qualify) {
		
		String groupName = null;
		
		if (group != null && qualify) {
            if(group.getCorrelationName() != null) { 
                groupName = group.getCorrelationName();
            } else {  
                AbstractMetadataRecord groupID = group.getMetadataObject();
                if(groupID != null) {              
                    groupName = getName(groupID);
                } else {
                    groupName = group.getName();
                }
            }
        }
		
		return groupName;
	}
	

	private String getGroupName(ColumnReference obj, boolean qualify){
		
        return getGroupName(obj.getTable(), qualify);
	}

	public String getSQL(){
		return buffer.toString();
	}

	@Override
	public void substitute(Argument arg, StringBuilder builder, int index) {
		// TODO Auto-generated method stub
		
	}

}
