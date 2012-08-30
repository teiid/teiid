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

import static org.teiid.language.SQLConstants.NonReserved.*;
import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.query.metadata.DDLConstants.*;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class DDLStringVisitor {
	private static final String TAB = "\t"; //$NON-NLS-1$
	private static final String NEWLINE = "\n";//$NON-NLS-1$
	
	private static final HashSet<String> LENGTH_DATATYPES = new HashSet<String>(
			Arrays.asList(
					DataTypeManager.DefaultDataTypes.CHAR,
					DataTypeManager.DefaultDataTypes.CLOB,
					DataTypeManager.DefaultDataTypes.BLOB,
					DataTypeManager.DefaultDataTypes.OBJECT,
					DataTypeManager.DefaultDataTypes.XML,
					DataTypeManager.DefaultDataTypes.STRING,
					DataTypeManager.DefaultDataTypes.VARBINARY,
					DataTypeManager.DefaultDataTypes.BIG_INTEGER));
	
	private static final HashSet<String> PRECISION_DATATYPES = new HashSet<String>(
			Arrays.asList(DataTypeManager.DefaultDataTypes.BIG_DECIMAL));

	protected StringBuilder buffer = new StringBuilder();
	private boolean includeTables = true;
	private boolean includeProcedures = true;
	private boolean includeFunctions = true;
	private Pattern filter;
	
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
    	if (regexPattern != null) {
    		this.filter = Pattern.compile(regexPattern);
    	}
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
		if (this.filter != null && !filter.matcher(table.getName()).matches()) {
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
		String name = SQLStringVisitor.escapeSinglePart(table.getName());
		buffer.append(name);
		
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
			buffer.append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
		
		if (table.isVirtual()) {
			buffer.append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE).append(table.getSelectTransformation());
		}
		buffer.append(SQLConstants.Tokens.SEMICOLON);
		
		if (table.isInsertPlanEnabled()) {
			buildTrigger(name, INSERT, table.getInsertPlan());
		}
		
		if (table.isUpdatePlanEnabled()) {
			buildTrigger(name, UPDATE, table.getUpdatePlan());
		}	
		
		if (table.isDeletePlanEnabled()) {
			buildTrigger(name, DELETE, table.getDeletePlan());
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
			addOption(options, MATERIALIZED, table.isMaterialized());
			if (table.getMaterializedTable() != null) {
				addOption(options, MATERIALIZED_TABLE, table.getMaterializedTable().getName());
			}
		}
		if (table.supportsUpdate()) {
			addOption(options, UPDATABLE, table.supportsUpdate());
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
		if (record.getUUID() != null && !record.getUUID().startsWith("tid:")) { //$NON-NLS-1$
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

	private void addColumns(StringBuilder builder, List<Column> columns, boolean includeType) {
		builder.append(LPAREN);
		boolean first = true;
		for (Column c:columns) {
			if (first) {
				first = false;
			}
			else {
				builder.append(COMMA).append(SPACE);
			}
			appendColumn(builder, c, true, includeType);
			if (includeType) {
				appendColumnOptions(builder, c);
			}
		}
		builder.append(RPAREN);
	}

	private void addNames(StringBuilder builder, List<String> columns) {
		if (columns != null) {
			builder.append(LPAREN);
			boolean first = true;
			for (String c:columns) {
				if (first) {
					first = false;
				}
				else {
					builder.append(COMMA).append(SPACE);
				}
				builder.append(c);
			}
			builder.append(RPAREN);
		}
	}	
	
	private void visit(Column column, Table table) {
		buffer.append(NEWLINE).append(TAB);
		appendColumn(buffer, column, true, true);
		
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
		
		appendDefault(column);
		
		// options
		appendColumnOptions(buffer, column);
	}

	private void appendDefault(BaseColumn column) {
		if (column.getDefaultValue() != null) {
			buffer.append(SPACE).append(DEFAULT).append(SPACE).append(TICK).append(StringUtil.replaceAll(column.getDefaultValue(), TICK, TICK + TICK)).append(TICK);
		}
	}

	private void appendColumn(StringBuilder builder, BaseColumn column, boolean includeName, boolean includeType) {
		if (includeName) {
			builder.append(SQLStringVisitor.escapeSinglePart(column.getName()));
		}
		if (includeType) {
			String runtimeTypeName = column.getDatatype().getRuntimeTypeName();
			if (includeName) {
				builder.append(SPACE);
			}
			builder.append(runtimeTypeName);
			if (LENGTH_DATATYPES.contains(runtimeTypeName)) {
				if (column.getLength() != 0 && column.getLength() != column.getDatatype().getLength()) {
					builder.append(LPAREN).append(column.getLength()).append(RPAREN);
				}
			} else if (PRECISION_DATATYPES.contains(runtimeTypeName) 
					&& (column.getPrecision() != column.getDatatype().getPrecision() || column.getScale() != column.getDatatype().getScale())) {
				builder.append(LPAREN).append(column.getPrecision());
				if (column.getScale() != 0) {
					builder.append(COMMA).append(column.getScale());
				}
				builder.append(RPAREN);
			}
			if (column.getNullType() == NullType.No_Nulls) {
				builder.append(SPACE).append(NOT_NULL);
			}
		}
	}	
	
	private void appendColumnOptions(StringBuilder builder, BaseColumn column) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, column);
		
		if (!column.getDatatype().isBuiltin()) {
			addOption(options, UDT, column.getDatatype().getName() + "("+column.getLength()+ ", " +column.getPrecision()+", " + column.getScale()+ ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		}
		
		if (column.getDatatype().getRadix() != 0 && column.getRadix() != column.getDatatype().getRadix()) {
			addOption(options, RADIX, column.getRadix());
		}
		
		if (column instanceof Column) {
			buildColumnOptions((Column)column, options);
		}
		if (options.length() != 0) {
			builder.append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
	}

	private void buildColumnOptions(Column column, 
			StringBuilder options) {
		if (!column.isSelectable()) {
			addOption(options, SELECTABLE, column.isSelectable());
		}		

		// if table is already updatable, then columns are implicitly updatable.
		if (!column.isUpdatable() && column.getParent() instanceof Table && ((Table)column.getParent()).supportsUpdate()) {
			addOption(options, UPDATABLE, column.isUpdatable());
		}
		
		if (column.isCurrency()) {
			addOption(options, CURRENCY, column.isCurrency());
		}
			
		// only record if not default
		if (!column.isCaseSensitive() && column.getDatatype().isCaseSensitive()) {
			addOption(options, CASE_SENSITIVE, column.isCaseSensitive());
		}
		
		if (!column.isSigned() && column.getDatatype().isSigned()) {
			addOption(options, SIGNED, column.isSigned());
		}		  
		if (column.isFixedLength()) {
			addOption(options, FIXED_LENGTH, column.isFixedLength());
		}
		// length and octet length should be same. so this should be never be true.
		//TODO - this is not quite valid since we are dealing with length representing chars in UTF-16, then there should be twice the bytes
		if (column.getCharOctetLength() != 0 && column.getLength() != column.getCharOctetLength()) {
			addOption(options, CHAR_OCTET_LENGTH, column.getCharOctetLength());
		}	
		
		// by default the search type is default data type search, so avoid it.
		if (column.getSearchType() != null && !column.getSearchType().equals(column.getDatatype().getSearchType())) {
			addOption(options, SEARCHABLE, column.getSearchType().name());
		}
		
		if (column.getMinimumValue() != null) {
			addOption(options, MIN_VALUE, column.getMinimumValue());
		}
		
		if (column.getMaximumValue() != null) {
			addOption(options, MAX_VALUE, column.getMaximumValue());
		}
		
		if (column.getNativeType() != null) {
			addOption(options, NATIVE_TYPE, column.getNativeType());
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
	}	
	
	private void addOption(StringBuilder sb, String key, Object value) {
		if (sb.length() != 0) {
			sb.append(COMMA).append(SPACE);
		}
		if (value instanceof String) {
			value = TICK + StringUtil.replaceAll((String)value, TICK, TICK + TICK) + TICK;
		}
		sb.append(SQLStringVisitor.escapeSinglePart(key)).append(SPACE).append(value);
	}
	
	private void visit(Procedure procedure) {
		if (this.filter != null && !filter.matcher(procedure.getName()).matches()) {
			return;
		}
		
		buffer.append(CREATE).append(SPACE);
		if (procedure.isVirtual()) {
			buffer.append(VIRTUAL);
		}
		else {
			buffer.append(FOREIGN);
		}
		buffer.append(SPACE).append(PROCEDURE).append(SPACE).append(SQLStringVisitor.escapeSinglePart(procedure.getName()));
		buffer.append(LPAREN);
		
		boolean first = true;
		for (ProcedureParameter pp:procedure.getParameters()) {
			if (first) {
				first = false;
			}
			else {
				buffer.append(COMMA).append(SPACE);
			}
			visit(pp);
		}
		buffer.append(RPAREN);
		
		if (procedure.getResultSet() != null) {
			buffer.append(SPACE).append(RETURNS).append(SPACE).append(TABLE).append(SPACE);
			addColumns(buffer, procedure.getResultSet().getColumns(), true);
		}
		/* The parser treats the RETURN clause as optional for a procedure if using the RESULT param
		  for (ProcedureParameter pp: procedure.getParameters()) {
			if (pp.getType().equals(Type.ReturnValue)) {
				buffer.append(SPACE).append(RETURNS).append(SPACE);
				appendColumn(buffer, pp, false, true);
				break;
			}
		}*/
		
		//options
		String options = buildProcedureOptions(procedure);		
		if (!options.isEmpty()) {
			buffer.append(NEWLINE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}		
		//block
		if (procedure.isVirtual()) {
			buffer.append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE);
			String plan = procedure.getQueryPlan();
			buffer.append(plan);
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

	private void visit(ProcedureParameter param) {
		Type type = param.getType();
		String typeStr = null;
		switch (type) {
		case InOut:
			typeStr = INOUT;
			break;
		case ReturnValue:
		case Out:
			typeStr = OUT;
			break;
		case In:
			if (param.isVarArg()) {
				typeStr = NonReserved.VARIADIC;
			} else {
				typeStr = IN;
			}
			break;
		}
		buffer.append(typeStr).append(SPACE);
		appendColumn(buffer, param, true, true);
		if (type == Type.ReturnValue) {
			buffer.append(SPACE).append(NonReserved.RESULT);
		}
		appendDefault(param);
		appendColumnOptions(buffer, param);
	}	

	private void visit(FunctionMethod function) {
		if (this.filter != null && !filter.matcher(function.getName()).matches()) {
			return;
		}		
		buffer.append(CREATE).append(SPACE);
		if (function.getPushdown().equals(FunctionMethod.PushDown.MUST_PUSHDOWN)) {
			buffer.append(FOREIGN);
		}
		else {
			buffer.append(VIRTUAL);
		}
		buffer.append(SPACE).append(FUNCTION).append(SPACE).append(SQLStringVisitor.escapeSinglePart(function.getName()));
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
			buffer.append(NEWLINE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
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
		if (param.isVarArg()) {
			buffer.append(NonReserved.VARIADIC).append(SPACE);
		}
		buffer.append(SQLStringVisitor.escapeSinglePart(param.getName())).append(SPACE).append(param.getType());
	}

    public String toString() {
        return buffer.toString();
    }
}
