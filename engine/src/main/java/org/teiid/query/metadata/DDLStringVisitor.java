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
package org.teiid.query.metadata;

import static org.teiid.language.SQLConstants.Tokens.COMMA;
import static org.teiid.language.SQLConstants.Tokens.LPAREN;
import static org.teiid.language.SQLConstants.Tokens.RPAREN;
import static org.teiid.language.SQLConstants.Tokens.SPACE;
import static org.teiid.language.SQLConstants.Tokens.TICK;
import static org.teiid.query.metadata.DDLConstants.*;

import java.util.EnumSet;
import java.util.List;

import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;

public class DDLStringVisitor {
	private static final String TAB = "\t"; //$NON-NLS-1$
	private static final String NEWLINE = "\n";//$NON-NLS-1$

	protected StringBuilder buffer = new StringBuilder();
	private boolean includeTables = true;
	private boolean includeProcedures = true;
	private boolean includeFunctions = true;
	private String filter;
	
    public static String getDDLString(Schema schema, EnumSet<SchemaObjectType> types, String regexPattern) {
    	DDLStringVisitor visitor = new DDLStringVisitor(types, regexPattern);
        visitor.visit(schema);
        return visitor.toString();
    }
	
    public DDLStringVisitor(EnumSet<SchemaObjectType> types, String regexPattern) {
    	if (types != null) {
    		this.includeTables = types.contains(SchemaObjectType.TABLES);
    		this.includeProcedures = types.contains(SchemaObjectType.PROCEDURES);
    		this.includeFunctions = types.contains(SchemaObjectType.FUNCTIONS);
    	}
    	this.filter = regexPattern;
    }
    
	private void visit(Schema schema) {
		boolean first = true; 
		
		if (this.includeTables) {
			for (Table t: schema.getTables().values()) {
				if (first) {
					first = false;
				}
				else {
					buffer.append(NEWLINE);
					buffer.append(NEWLINE);
				}			
				visit(t);
			}
		}
		
		if (this.includeProcedures) {
			for (Procedure p:schema.getProcedures().values()) {
				if (first) {
					first = false;
				}
				else {
					buffer.append(NEWLINE);
					buffer.append(NEWLINE);
				}				
				visit(p);
			}
		}
		
		if (this.includeFunctions) {
			for (FunctionMethod f:schema.getFunctions().values()) {
				if (first) {
					first = false;
				}
				else {
					buffer.append(NEWLINE);
					buffer.append(NEWLINE);
				}				
				visit(f);
			}
		}
	}

	private void visit(Table table) {
		if (this.filter != null && !table.getName().matches(this.filter)) {
			return;
		}
		
		buffer.append(CREATE).append(SPACE);
		if (table.isPhysical()) {
			buffer.append(FOREIGN_TABLE);
		}
		else {
			buffer.append(VIEW);
		}
		buffer.append(SPACE);
		buffer.append(table.getName());
		
		if (table.getColumns() != null) {
			buffer.append(SPACE);
			buffer.append(LPAREN);
			boolean first = true; 
			for (Column c:table.getColumns()) {
				if (first) {
					first = false;
				}
				else {
					buffer.append(COMMA);
				}
				visit(c, table);
			}
			
			// constraints
			String contraints = buildContraints(table);
			if (!contraints.isEmpty()) {
				buffer.append(NEWLINE).append(TAB);
				buffer.append(CONSTRAINT);
				buffer.append(contraints);
			}
			buffer.append(NEWLINE);
			buffer.append(RPAREN);			
		}
		
		// options
		String options = buildTableOptions(table);		
		if (!options.isEmpty()) {
			buffer.append(SPACE).append(OPTIONS2).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
		
		if (table.isVirtual()) {
			buffer.append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE).append(table.getSelectTransformation());
		}
		buffer.append(SQLConstants.Tokens.SEMICOLON);
		
		if (table.isInsertPlanEnabled()) {
			buildTrigger(table.getName(), INSERT, table.getInsertPlan());
		}
		
		if (table.isUpdatePlanEnabled()) {
			buildTrigger(table.getName(), UPDATE, table.getUpdatePlan());
		}	
		
		if (table.isDeletePlanEnabled()) {
			buildTrigger(table.getName(), DELETE, table.getDeletePlan());
		}			
	}

	private void buildTrigger(String name, String type, String plan) {
		buffer.append(NEWLINE);
		buffer.append(NEWLINE);
		buffer.append(CREATE_TRIGGER_ON).append(SPACE);
		buffer.append(name).append(SPACE).append(INSTEAD_OF).append(SPACE).append(type).append(SPACE).append(SQLConstants.Reserved.AS).append(NEWLINE);
		buffer.append(plan);
		buffer.append(SQLConstants.Tokens.SEMICOLON);
	}

	private String buildTableOptions(Table table) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, table);
		
		if (table.isMaterialized()) {
			addOption(options, MATERIALIZED, String.valueOf(table.isMaterialized()));
			if (table.getMaterializedTable() != null) {
				addOption(options, MATERIALIZED_TABLE, table.getMaterializedTable().getName());
			}
		}
		if (table.supportsUpdate()) {
			addOption(options, UPDATABLE, String.valueOf(table.supportsUpdate()));
		}
		if (table.getCardinality() != -1) {
			addOption(options, CARDINALITY, table.getCardinality());
		}
		if (!table.getProperties().isEmpty()) {
			for (String key:table.getProperties().keySet()) {
				addOption(options, key, table.getProperty(key, false));
			}
		}
		return options.toString();
	}

	private void addCommonOptions(StringBuilder sb, AbstractMetadataRecord record) {
		if (record.getUUID() != null && !record.getUUID().startsWith("mmuuid:")) { //$NON-NLS-1$
			addOption(sb, UUID, record.getUUID());
		}
		if (record.getAnnotation() != null) {
			addOption(sb, ANNOTATION, record.getAnnotation());
		}
		if (record.getNameInSource() != null && !record.getNameInSource().equals(record.getName())) {
			addOption(sb, NAMEINSOURCE, record.getNameInSource());
		}
	}
	
	private String buildContraints(Table table) {
		StringBuilder options = new StringBuilder();
		
		boolean first = true;
		for (KeyRecord key:table.getAccessPatterns()) {
			if (first) {
				first = false;
			}
			else {
				options.append(COMMA);
			}			
			options.append(SPACE).append(ACCESSPATTERN);
			addColumns(options, key.getColumns(), false);
		}
		
		
		KeyRecord pk = table.getPrimaryKey();
		if (pk != null && pk.getColumns().size() > 1) {
			if (first) {
				first = false;
			}
			else {
				options.append(COMMA);
			}
			options.append(SPACE).append(PRIMARY_KEY);
			addColumns(options, pk.getColumns(), false);
		}
		
		for (KeyRecord key:table.getUniqueKeys()) {
			if (key != null && key.getColumns().size() > 1) {
				if (first) {
					first = false;
				}
				else {
					options.append(COMMA);
				}
				options.append(SPACE).append(UNIQUE);
				addColumns(options, key.getColumns(), false);
			}
		}
		
		for (KeyRecord key:table.getIndexes()) {
			if (key != null && key.getColumns().size() > 1) {
				if (first) {
					first = false;
				}
				else {
					options.append(COMMA);
				}				
				options.append(SPACE).append(INDEX);
				addColumns(options, key.getColumns(), false);
			}
		}		

		for (ForeignKey key:table.getForeignKeys()) {
			if (first) {
				first = false;
			}
			else {
				options.append(COMMA);
			}			
			options.append(SPACE).append(FOREIGN_KEY);
			addColumns(options, key.getColumns(), false);
			options.append(SPACE).append(REFERENCES);
			if (key.getReferenceTableName() != null) {
				options.append(SPACE).append(key.getReferenceTableName());
			}
			options.append(SPACE);
			addNames(options, key.getReferenceColumns());
		}
		
		return options.toString();
	}

	private void addColumns(StringBuilder options, List<Column> columns, boolean includeType) {
		options.append(LPAREN);
		boolean first = true;
		for (Column c:columns) {
			if (first) {
				first = false;
			}
			else {
				options.append(COMMA).append(SPACE);
			}
			options.append(c.getName());
			if (includeType) {
				options.append(SPACE).append(c.getDatatype().getName());
			}
		}
		options.append(RPAREN);
	}
	
	private void addNames(StringBuilder options, List<String> columns) {
		if (columns != null) {
			options.append(LPAREN);
			boolean first = true;
			for (String c:columns) {
				if (first) {
					first = false;
				}
				else {
					options.append(COMMA).append(SPACE);
				}
				options.append(c);
			}
			options.append(RPAREN);
		}
	}	
	
	private void visit(Column column, Table table) {
		buffer.append(NEWLINE).append(TAB).append(column.getName()).append(SPACE).append(column.getDatatype().getName());
		if (column.getLength() != 0) {
			buffer.append(LPAREN).append(column.getLength()).append(RPAREN);
		}
		else if (column.getPrecision() != 0){
			buffer.append(LPAREN).append(column.getPrecision());
			if (column.getScale() != 0) {
				buffer.append(COMMA).append(column.getScale());
			}
			buffer.append(RPAREN);
		}
		if (column.getNullType() != null) {
			if (column.getNullType() == NullType.No_Nulls) {
				buffer.append(SPACE).append(NOT_NULL);
			}
		}
		
		if (column.isAutoIncremented()) {
			buffer.append(SPACE).append(AUTO_INCREMENT);
		}
		
		KeyRecord pk = table.getPrimaryKey();
		if (pk != null && pk.getColumns().size() == 1) {
			Column c = pk.getColumns().get(0);
			if (column.equals(c)) {
				buffer.append(SPACE).append(PRIMARY_KEY);
			}
		}
		
		for (KeyRecord key:table.getUniqueKeys()) {
			if (key != null && key.getColumns().size() == 1) {
				Column c = key.getColumns().get(0);
				if (column.equals(c)) {
					buffer.append(SPACE).append(UNIQUE);
				}
			}
		}
		
		for (KeyRecord key:table.getIndexes()) {
			if (key != null && key.getColumns().size() == 1) {
				Column c = key.getColumns().get(0);
				if (column.equals(c)) {
					buffer.append(SPACE).append(INDEX);
				}
			}
		}		
		
		if (column.getDefaultValue() != null) {
			buffer.append(SPACE).append(DEFAULT).append(SPACE).append(TICK).append(column.getDefaultValue()).append(TICK);
		}
		
		// options
		String options = buildColumnOptions(column, table);		
		if (!options.isEmpty()) {
			buffer.append(SPACE).append(OPTIONS2).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
	}	
	
	private String buildColumnOptions(Column column, Table table) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, column);
		
		if (column.isCaseSensitive()) {
			addOption(options, CASE_SENSITIVE, String.valueOf(column.isCaseSensitive()));
		}
		
		if (!column.isSelectable()) {
			addOption(options, SELECTABLE, String.valueOf(column.isSelectable()));
		}		

		// if table is already updatable, then columns are implicitly updatable.
		if (table.supportsUpdate() && !column.isUpdatable()) {
			addOption(options, UPDATABLE, String.valueOf(column.isUpdatable()));
		}
		
		if (column.isSigned()) {
			addOption(options, SIGNED, String.valueOf(column.isSigned()));
		}
		
		if (column.isCurrency()) {
			addOption(options, CURRENCY, String.valueOf(column.isCurrency()));
		}
			
		if (column.isFixedLength()) {
			addOption(options, FIXED_LENGTH, String.valueOf(column.isFixedLength()));
		}
		
		if (column.getSearchType() != null) {
			addOption(options, SEARCHABLE, column.getSearchType().name());
		}
		
		if (column.getMinimumValue() != null) {
			addOption(options, MIN_VALUE, column.getMinimumValue());
		}
		
		if (column.getMaximumValue() != null) {
			addOption(options, MAX_VALUE, column.getMaximumValue());
		}
		
		if (column.getCharOctetLength() != 0) {
			addOption(options, CHAR_OCTET_LENGTH, column.getCharOctetLength());
		}	
		
		// only set native type on the foreign tables, on view the data type is type
		if (table.isPhysical() && column.getNativeType() != null) {
			addOption(options, NATIVE_TYPE, column.getNativeType());
		}
		
		if (column.getRadix() != 0) {
			addOption(options, RADIX, column.getRadix());
		}
		if (column.getNullValues() != -1) {
			addOption(options, NULL_VALUE_COUNT, column.getNullValues());
		}
		if (column.getDistinctValues() != -1) {
			addOption(options, DISTINCT_VALUES, column.getDistinctValues());
		}		
		
		if (!column.getProperties().isEmpty()) {
			for (String key:column.getProperties().keySet()) {
				addOption(options, key, column.getProperty(key, false));
			}
		}
		return options.toString();
	}	
	
	private void addOption(StringBuilder sb, String key, String value) {
		if (sb.length() != 0) {
			sb.append(COMMA).append(SPACE);
		}
		sb.append(key).append(SPACE).append(TICK).append(value).append(TICK);
	}
	
	private void addOption(StringBuilder sb, String key, int value) {
		if (sb.length() != 0) {
			sb.append(COMMA).append(SPACE);
		}		
		sb.append(key).append(SPACE).append(value);
	}	

	private void visit(Procedure procedure) {
		if (this.filter != null && !procedure.getName().matches(this.filter)) {
			return;
		}
		
		buffer.append(CREATE).append(SPACE);
		if (procedure.isVirtual()) {
			buffer.append(VIRTUAL);
		}
		else {
			buffer.append(FOREIGN);
		}
		buffer.append(SPACE).append(PROCEDURE2).append(SPACE).append(procedure.getName());
		buffer.append(LPAREN);
		
		boolean first = true;
		for (ProcedureParameter pp:procedure.getParameters()) {
			Type type = pp.getType();
			if (type == Type.In || type == Type.InOut || type == Type.Out) {
				if (first) {
					first = false;
				}
				else {
					buffer.append(COMMA).append(SPACE);
				}
				visit(pp);
			}
		}
		buffer.append(RPAREN);
		
		buffer.append(SPACE).append(RETURNS).append(SPACE);
		buildProcedureReturn(procedure.getResultSet(), procedure.getParameters());
		
		//options
		String options = buildProcedureOptions(procedure);		
		if (!options.isEmpty()) {
			buffer.append(NEWLINE).append(OPTIONS2).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}		
		//block
		if (procedure.isVirtual()) {
			buffer.append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE);
			String plan = procedure.getQueryPlan();
			buffer.append(plan.substring("CREATE VIRTUAL PROCEDURE BEGIN ".length(), plan.length()-"END".length())); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
	

	private String buildProcedureOptions(Procedure procedure) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, procedure);
		
		if (procedure.getUpdateCount() != 1) {
			addOption(options, UPDATECOUNT, procedure.getUpdateCount());
		}	
		
		if (!procedure.getProperties().isEmpty()) {
			for (String key:procedure.getProperties().keySet()) {
				addOption(options, key, procedure.getProperty(key, false));
			}
		}		
		
		return options.toString();
	}

	private void buildProcedureReturn(ColumnSet<Procedure> resultSet, List<ProcedureParameter> parameters) {
		if (resultSet != null) {
			addColumns(buffer, resultSet.getColumns(), true);
		}
		else {
			if (parameters != null) {
				for (ProcedureParameter pp: parameters) {
					if (pp.getType().equals(Type.ReturnValue)) {
						buffer.append(pp.getDatatype().getName());
					}
				}
			}
		}
		
	}
	
	private void visit(ProcedureParameter param) {
		Type type = param.getType();
		if (type == Type.In || type == Type.InOut || type == Type.Out) {
			String typeStr = null;
			if (type == Type.In) typeStr = IN;
			if (type == Type.InOut) typeStr = INOUT;
			if (type == Type.Out) typeStr = OUT;
			buffer.append(typeStr).append(SPACE).append(param.getName()).append(SPACE).append(param.getDatatype().getName());
		}
	}	

	private void visit(FunctionMethod function) {
		if (this.filter != null && !function.getName().matches(this.filter)) {
			return;
		}		
		buffer.append(CREATE).append(SPACE);
		if (function.getPushdown().equals(FunctionMethod.PushDown.MUST_PUSHDOWN)) {
			buffer.append(FOREIGN);
		}
		else {
			buffer.append(VIRTUAL);
		}
		buffer.append(SPACE).append(FUNCTION2).append(SPACE).append(function.getName());
		buffer.append(LPAREN);
		
		boolean first = true;
		for (FunctionParameter fp:function.getInputParameters()) {
			if (first) {
				first = false;
			}
			else {
				buffer.append(COMMA).append(SPACE);
			}
			visit(fp);
		}
		buffer.append(RPAREN);
		
		buffer.append(SPACE).append(RETURNS).append(SPACE);
		buffer.append(function.getOutputParameter().getType());
		
		//options
		String options = buildFunctionOptions(function);		
		if (!options.isEmpty()) {
			buffer.append(NEWLINE).append(OPTIONS2).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}		
		buffer.append(SQLConstants.Tokens.SEMICOLON);		
	}

	private String buildFunctionOptions(FunctionMethod function) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, function);
		
		if (function.getCategory() != null) {
			addOption(options, CATEGORY, function.getCategory());
		}	
		
		if (!function.getDeterminism().equals(Determinism.DETERMINISTIC)) {
			addOption(options, DETERMINISM, function.getDeterminism().name());
		}		
		
		if (function.getInvocationClass() != null) {
			addOption(options, JAVA_CLASS, function.getInvocationClass());
		}

		if (function.getInvocationMethod() != null) {
			addOption(options, JAVA_METHOD, function.getInvocationMethod());
		}
		
		if (!function.getProperties().isEmpty()) {
			for (String key:function.getProperties().keySet()) {
				addOption(options, key, function.getProperty(key, false));
			}
		}		
		
		return options.toString();
	}

	private void visit(FunctionParameter param) {
		buffer.append(param.getName()).append(SPACE).append(param.getType());
	}

    public String toString() {
        return buffer.toString();
    }
}
