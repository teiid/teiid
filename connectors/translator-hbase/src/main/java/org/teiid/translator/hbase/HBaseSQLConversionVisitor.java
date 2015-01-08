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
import static org.teiid.language.SQLConstants.Reserved.INTO;

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
import org.teiid.language.ColumnReference;
import org.teiid.language.Insert;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.hbase.phoenix.PColumnTeiidImpl;
import org.teiid.translator.hbase.phoenix.PNameTeiidImpl;
import org.teiid.translator.hbase.phoenix.PTableTeiidImpl;
import org.teiid.translator.hbase.phoenix.PhoenixUtils;

public class HBaseSQLConversionVisitor extends org.teiid.translator.jdbc.SQLConversionVisitor {
	
	public static final String INSERT = "UPSERT";
	
	private HBaseExecutionFactory executionFactory ;
	
	
	private Map<Column, PColumn> columnsMap = new HashMap<Column, PColumn> ();
	private Map<Table, PTable> tablesMap = new HashMap<Table, PTable> ();
	
	public HBaseSQLConversionVisitor(HBaseExecutionFactory ef) {
		super(ef);
		this.executionFactory = ef;
	}


	@Override
	public void visit(Select obj) {
				
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
			phoenixTableMapping(obj.getFrom());
		}
		
		super.visit(obj);

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
		
		executionFactory.getMappingDDLList().add(PhoenixUtils.hbaseTableMappingDDL(ptable));
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
	public void visit(ColumnReference obj) {
		String groupName = getGroupName(obj.getTable(), !shortNameOnly);
		String elementName = getElementName(obj, !shortNameOnly);
		
		if(null != groupName){
			buffer.append(groupName + Tokens.DOT + elementName);
		} else {
			buffer.append(elementName);
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
	

	private String getElementName(ColumnReference obj, boolean qualify){
		
		Column column = obj.getMetadataObject();
		if(null != column) {
			PColumn pcolumn = columnsMap.get(column);
			return pcolumn.getName().getString();
		} else {
			return obj.getName();
		}

	}

	public String getSQL(){
		return buffer.toString();
	}

}
