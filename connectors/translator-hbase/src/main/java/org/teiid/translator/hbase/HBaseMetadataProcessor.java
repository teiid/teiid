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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.HBaseConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.hbase.phoenix.PColumnTeiidImpl;
import org.teiid.translator.hbase.phoenix.PNameTeiidImpl;
import org.teiid.translator.hbase.phoenix.PTableTeiidImpl;
import org.teiid.translator.hbase.phoenix.PhoenixUtils;

public class HBaseMetadataProcessor implements MetadataProcessor<HBaseConnection> {
	
	@ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="HBase Table Name", description="HBase Table Name", required=true)
	public static final String TABLE = MetadataFactory.HBASE_URI + "TABLE";

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Family and Qualifier", description="Column Family and Column Qualifier, seperated by a colon, for eample, 'customer:city' means cell's family is 'customer', qualifier is 'city'", required=true)    
	public static final String CELL = MetadataFactory.HBASE_URI + "CELL";

    private String hbaseTableName;
    private String[] columnQualifiers;
    private String[] columnTypes;

	@Override
	public void process(MetadataFactory metadataFactory, HBaseConnection connection) throws TranslatorException {
		
		if(hbaseTableName == null) {
			throw new TranslatorException(HBasePlugin.Event.TEIID27005, HBasePlugin.Util.gs(HBasePlugin.Event.TEIID27014, "importer.hbaseTableName"));
		}
		
		if(columnQualifiers == null || columnQualifiers.length == 0) {
			throw new TranslatorException(HBasePlugin.Event.TEIID27005, HBasePlugin.Util.gs(HBasePlugin.Event.TEIID27014, "importer.columnQualifiers"));
		}
		
		if(columnTypes == null || columnTypes.length != columnQualifiers.length) {
			throw new TranslatorException(HBasePlugin.Event.TEIID27005, HBasePlugin.Util.gs(HBasePlugin.Event.TEIID27014, "importer.columnTypes"));
		}
		
		Connection conn = connection.getConnection();
		
		addTable(metadataFactory, conn, hbaseTableName, columnQualifiers);
		
	}

	private void addTable(MetadataFactory mf, Connection conn, String hbaseTableName, String[] columnQualifiers) throws TranslatorException {

		Table table = mf.addTable(hbaseTableName);
		table.setProperty(HBaseMetadataProcessor.TABLE, hbaseTableName);
		
		List<PColumn> columns = new ArrayList<PColumn>();
		for(int i = 0 ; i < columnQualifiers.length; i ++) {
			PColumn pcolumn;
			String cell = columnQualifiers[i];
			String[] qua =  cell.split(":");
			if(qua.length != 2) {
				Column column = mf.addColumn(cell, columnTypes[i], table);
				column.setProperty(HBaseMetadataProcessor.CELL, cell);
				mf.addPrimaryKey(cell, Arrays.asList(cell), table);
				pcolumn = new PColumnTeiidImpl(PNameTeiidImpl.makePName(cell), null, convertType(columnTypes[i]));
			} else {
				Column column = mf.addColumn(qua[1], columnTypes[i], table);
				column.setProperty(HBaseMetadataProcessor.CELL, cell);
				pcolumn = new PColumnTeiidImpl(PNameTeiidImpl.makePName(qua[1]), PNameTeiidImpl.makePName(qua[0]), convertType(columnTypes[i]));
			}
			columns.add(pcolumn);
		}
		
		PName tableName = PNameTeiidImpl.makePName(hbaseTableName);
		PTable ptable = PTableTeiidImpl.makeTable(tableName, columns);
		
		try {
			PhoenixUtils.executeUpdate(conn, PhoenixUtils.hbaseTableMappingDDL(ptable));
		} catch (SQLException e) {
			throw new TranslatorException(HBasePlugin.Event.TEIID27005, HBasePlugin.Util.gs(HBasePlugin.Event.TEIID27015, e.getMessage()));
		}

	}

	private PDataType convertType(String type) {
		
		if(type.equals(TypeFacility.RUNTIME_NAMES.STRING)){
			return PDataType.VARCHAR ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.BOOLEAN)){
			return PDataType.BOOLEAN ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.BYTE)){
			return PDataType.TINYINT ;
		}  else if(type.equals(TypeFacility.RUNTIME_NAMES.SHORT)){
			return PDataType.SMALLINT ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.CHAR)){
			return PDataType.VARCHAR ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.INTEGER)){
			return PDataType.INTEGER;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.LONG)){
			return PDataType.LONG ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.BIG_INTEGER)){
			return PDataType.LONG ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.FLOAT)){
			return PDataType.FLOAT ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.DOUBLE)){
			return PDataType.DOUBLE ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.BIG_DECIMAL)){
			return PDataType.DECIMAL ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.DATE)){
			return PDataType.DATE ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.TIME)){
			return PDataType.TIME ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.TIMESTAMP)){
			return PDataType.TIMESTAMP ;
		} else if(type.equals(TypeFacility.RUNTIME_NAMES.VARBINARY)){
			return PDataType.VARBINARY ;
		} 
		
		return null;
	}

	@TranslatorProperty(display="HBase Table", category=PropertyType.IMPORT, description="Name of the HBase Table to read metadata from", required=true)
	public String getHbaseTableName() {
		return hbaseTableName;
	}

	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	@TranslatorProperty (display="Column Qualifiers", category=PropertyType.IMPORT, description="Comma separated list(ROW_ID,f1:q1,f1:q2,f2:q1,f2:q2) - without spaces - of imported Column Qualifiers")
	public String[] getColumnQualifiers() {
		return columnQualifiers;
	}

	public void setColumnQualifiers(String[] columnQualifiers) {
		this.columnQualifiers = columnQualifiers;
	}

	@TranslatorProperty (display="Column Types", category=PropertyType.IMPORT, description="Comma separated list - without spaces - of imported column types.")
	public String[] getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(String[] columnTypes) {
		this.columnTypes = columnTypes;
	}

}
