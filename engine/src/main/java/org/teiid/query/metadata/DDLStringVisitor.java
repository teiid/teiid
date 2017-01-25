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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.Grant.Permission;
import org.teiid.metadata.Grant.Permission.Privilege;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
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
	private Map<String, String> prefixMap;
	protected boolean usePrefixes = true;
	protected boolean createNS = true;
	
	private final static Map<String, String> BUILTIN_PREFIXES = new HashMap<String, String>();
	static {
		for (Map.Entry<String, String> entry : MetadataFactory.BUILTIN_NAMESPACES.entrySet()) {
			BUILTIN_PREFIXES.put(entry.getValue(), entry.getKey());
		}
	}
	
    public static String getDDLString(Schema schema, EnumSet<SchemaObjectType> types, String regexPattern) {
    	DDLStringVisitor visitor = new DDLStringVisitor(types, regexPattern);
        visitor.visit(schema);
        return visitor.toString();
    }

    public static String getDDLString(Database database) {
        DDLStringVisitor visitor = new DDLStringVisitor(null, null);
        visitor.visit(database);
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
    
    private void visit(Database database) {
        append(NEWLINE);
        append("/*").append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("# START DATABASE ").append(database.getName()).append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("*/").append(NEWLINE);
        
        append(CREATE).append(SPACE).append(DATABASE).append(SPACE)
                .append(SQLStringVisitor.escapeSinglePart(database.getName())).append(SPACE).append(VERSION)
                .append(SPACE).append(TICK).append(database.getVersion()).append(TICK);
        appendOptions(database);
        append(SEMICOLON);
        append(NEWLINE);
        append(NEWLINE);

        if (!database.getDataWrappers().isEmpty()){
        	append(NEWLINE);
        	append("--############ Translators ############");
        	append(NEWLINE);        	
        }
        
        for (DataWrapper dw : database.getDataWrappers()) {
            visit(dw);
            append(NEWLINE);
            append(NEWLINE);
        }

        if (!database.getServers().isEmpty()){
        	append(NEWLINE);
        	append("--############ Servers ############");
        	append(NEWLINE);        	
        }
        
        for (Server server : database.getServers()) {
            visit(server);
            append(NEWLINE);
            append(NEWLINE);
        }

        for (Schema schema : database.getSchemas()) {
        	append(NEWLINE);
        	append("--############ Schema:").append(schema.getName()).append(" ############");
        	append(NEWLINE);
            append(CREATE).append(SPACE);
            if (!schema.isPhysical()) {
                append(VIRTUAL);
            }
            append(SPACE).append(SCHEMA).append(SPACE).append(SQLStringVisitor.escapeSinglePart(schema.getName()));
            if (!schema.getServers().isEmpty()) {
                append(SPACE).append(SERVER);
                boolean first = true;
                for (Server s:schema.getServers()) {
                    if (first) {
                        first = true;
                    } else {
                        append(COMMA);
                    }
                    append(SPACE).append(SQLStringVisitor.escapeSinglePart(s.getName()));
                }
            }
            
            appendOptions(schema);
            append(SEMICOLON);
            append(NEWLINE);
            append(SET).append(SPACE).append(SCHEMA).append(SPACE);
            append(SQLStringVisitor.escapeSinglePart(schema.getName())).append(SEMICOLON);
            append(NEWLINE);
            append(NEWLINE);
            
            visit(schema);  
        }
        
        if (!database.getRoles().isEmpty()){
        	append(NEWLINE);
        	append("--############ Roles & Grants ############");
        	append(NEWLINE);        	
        }
        
        for (Role role:database.getRoles()) {
            visit(role);
            append(NEWLINE);
        }
        
        for (Grant grant:database.getGrants()) {
            visit(grant);
            append(NEWLINE);
        }
        
        append(NEWLINE);
        append("/*").append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("# END DATABASE ").append(database.getName()).append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("*/").append(NEWLINE);
        append(NEWLINE);
    }
    
    private void visit(Grant grant) {
        
        for (Permission permission : grant.getPermissions()) {
            append(GRANT);
            boolean first = true;
            for (Privilege allowance:permission.getPrivileges()) {
                if (first) {
                    first = false;
                    append(SPACE);
                } else {
                    append(COMMA);
                }
                append(allowance.name());
            }
            append(SPACE).append(ON).append(SPACE).append(permission.getResourceType().name());
            if (permission.getResourceName() != null) {
                append(SPACE).append(SQLStringVisitor.escapeSinglePart(permission.getResourceName()));
            }
            
            if (permission.getResourceType() == ResourceType.COLUMN) {
                
                if (permission.getMask() != null) {
                    append(SPACE).append(MASK);
                    if (permission.getMaskOrder() != null && permission.getMaskOrder() != -1) {
                        append(SPACE).append(ORDER).append(SPACE).append(permission.getMaskOrder());
                    }
                    append(SPACE).append(TICK).append(SQLStringVisitor.escapeSinglePart(permission.getMask())).append(TICK);
                }
                
                if (permission.getCondition() != null) {
                    append(SPACE).append(CONDITION);
                    if (permission.isConditionAConstraint() != null && permission.isConditionAConstraint()) {
                        append(SPACE).append(CONSTRAINT);
                    }
                    append(SPACE).append(TICK).append(SQLStringVisitor.escapeSinglePart(permission.getCondition())).append(TICK);
                }
            }
            append(SPACE).append(TO).append(SPACE).append(grant.getRole());
            append(SEMICOLON).append(NEWLINE);
        }
    }

    private void visit(Role role) {
        append(CREATE).append(SPACE).append(ROLE.toUpperCase()).append(SPACE)
                .append(SQLStringVisitor.escapeSinglePart(role.getName()));
        if (role.getJassRoles() != null && !role.getJassRoles().isEmpty()) {
            append(SPACE).append(WITH).append(SPACE).append(JAAS).append(SPACE).append(ROLE);
            for (String str:role.getJassRoles()) {
                append(SPACE).append(SQLStringVisitor.escapeSinglePart(str));
            }
        }
        
        if (role.isAnyAuthenticated()) {
            append(SPACE).append(WITH).append(SPACE).append(ANY).append(SPACE).append(AUTHENTICATED);
        }
        append(SEMICOLON);
    }
    
    private void visit(DataWrapper dw) {
        append(CREATE).append(SPACE).append(FOREIGN).append(SPACE).append(DATA).append(SPACE).append(WRAPPER)
                .append(SPACE);
        append(SQLStringVisitor.escapeSinglePart(dw.getName()));
        if (dw.getType() != null) {
            append(SPACE).append(TYPE).append(SPACE).append(SQLStringVisitor.escapeSinglePart(dw.getType()));
        }
        appendOptions(dw);
        append(SEMICOLON);
    }

    private void visit(Server server) {
        if (server.getName().equalsIgnoreCase("none")) {
            return;
        }
        append(CREATE).append(SPACE).append(SERVER).append(SPACE)
                .append(SQLStringVisitor.escapeSinglePart(server.getName())).append(SPACE).append(TYPE).append(SPACE)
                .append(TICK).append(server.getType()).append(TICK);
        if (server.getVersion() != null) {
            append(SPACE).append(VERSION).append(SPACE).append(TICK).append(server.getVersion()).append(TICK);
        }
        append(SPACE).append(FOREIGN).append(SPACE).append(DATA).append(SPACE).append(WRAPPER).append(SPACE);
        append(SQLStringVisitor.escapeSinglePart(server.getDataWrapper()));
        appendOptions(server);
        append(SEMICOLON);
    }

    private void visit(Schema schema) {
		boolean first = true; 
		
		if (this.includeTables) {
			for (Table t: schema.getTables().values()) {
				if (first) {
					first = false;
				}
				else {
					append(NEWLINE);
					append(NEWLINE);
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
					append(NEWLINE);
					append(NEWLINE);
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
					append(NEWLINE);
					append(NEWLINE);
				}				
				visit(f);
			}
		}
	}

	private void visit(Table table) {
		if (this.filter != null && !filter.matcher(table.getName()).matches()) {
			return;
		}
		
		append(CREATE).append(SPACE);
		if (table.isPhysical()) {
			append(FOREIGN_TABLE);
		}
		else {
			if (table.getTableType() == Table.Type.TemporaryTable) {
				append(GLOBAL).append(SPACE).append(TEMPORARY).append(SPACE).append(TABLE);
			} else {
				append(VIEW);
			}
		}
		append(SPACE);
		String name = addTableBody(table);
		
		if (table.getTableType() != Table.Type.TemporaryTable) {
			if (table.isVirtual()) {
				append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE).append(table.getSelectTransformation());
			}
			append(SQLConstants.Tokens.SEMICOLON);
			
			if (table.isInsertPlanEnabled()) {
				buildTrigger(name, null, INSERT, table.getInsertPlan());
			}
			
			if (table.isUpdatePlanEnabled()) {
				buildTrigger(name, null, UPDATE, table.getUpdatePlan());
			}	
			
			if (table.isDeletePlanEnabled()) {
				buildTrigger(name, null, DELETE, table.getDeletePlan());
			}	
			
			for (Trigger tr : table.getTriggers().values()) {
			    buildTrigger(name, tr.getName(), tr.getEvent().name(), tr.getPlan());   
			}
		} else {
		    append(SQLConstants.Tokens.SEMICOLON);
		}
	}

	public String addTableBody(Table table) {
		String name = SQLStringVisitor.escapeSinglePart(table.getName());
		append(name);
		
		if (table.getColumns() != null) {
			append(SPACE);
			append(LPAREN);
			boolean first = true; 
			for (Column c:table.getColumns()) {
				if (first) {
					first = false;
				}
				else {
					append(COMMA);
				}
				visit(table, c);
			}
			buildContraints(table);
			append(NEWLINE);
			append(RPAREN);			
		}
		
		// options
		String options = buildTableOptions(table);		
		if (!options.isEmpty()) {
			append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
		return name;
	}
	
	protected DDLStringVisitor append(Object o) {
		buffer.append(o);
		return this;
	}

	private void buildTrigger(String name, String trigger_name, String type, String plan) {
		append(NEWLINE);
		append(NEWLINE);
		append(SQLConstants.Reserved.CREATE).append(SPACE).append(TRIGGER).append(SPACE);
		if (trigger_name != null) {
		    append(SQLStringVisitor.escapeSinglePart(trigger_name)).append(SPACE);
		}
		append(SQLConstants.Reserved.ON).append(SPACE).append(name).append(SPACE).append(INSTEAD_OF).append(SPACE).append(type).append(SPACE).append(SQLConstants.Reserved.AS).append(NEWLINE);
		append(plan);
		append(SQLConstants.Tokens.SEMICOLON);
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
			if (table.getCardinality() != table.getCardinalityAsFloat()) {
				addOption(options, CARDINALITY, (long)table.getCardinalityAsFloat());
			} else {
				addOption(options, CARDINALITY, table.getCardinality());
			}
		}
		if (!table.getProperties().isEmpty()) {
			for (String key:table.getProperties().keySet()) {
				addOption(options, key, table.getProperty(key, false));
			}
		}
		return options.toString();
	}

	private void addCommonOptions(StringBuilder sb, AbstractMetadataRecord record) {
		if (record.isUUIDSet() && record.getUUID() != null && !record.getUUID().startsWith("tid:")) { //$NON-NLS-1$
			addOption(sb, UUID, record.getUUID());
		}
		if (record.getAnnotation() != null) {
			addOption(sb, ANNOTATION, record.getAnnotation());
		}
		if (record.getNameInSource() != null) {
			addOption(sb, NAMEINSOURCE, record.getNameInSource());
		}
	}
	
	private void buildContraints(Table table) {
		addConstraints(table.getAccessPatterns(), "AP", ACCESSPATTERN); //$NON-NLS-1$
		
		KeyRecord pk = table.getPrimaryKey();
		if (pk != null) {
			addConstraint("PK", PRIMARY_KEY, pk, true); //$NON-NLS-1$
		}

		addConstraints(table.getUniqueKeys(), UNIQUE, UNIQUE);
		addConstraints(table.getIndexes(), INDEX, INDEX);
		addConstraints(table.getFunctionBasedIndexes(), INDEX, INDEX);

		for (int i = 0; i < table.getForeignKeys().size(); i++) {
			ForeignKey key = table.getForeignKeys().get(i);
			addConstraint("FK" + i, FOREIGN_KEY, key, false); //$NON-NLS-1$
			append(SPACE).append(REFERENCES);
			if (key.getReferenceKey() != null) {
				if (key.getReferenceKey().getParent().getParent().equals(key.getParent().getParent())) {
					append(SPACE).append(new GroupSymbol(key.getReferenceKey().getParent().getName()));
				} else {
					append(SPACE).append(new GroupSymbol(key.getReferenceKey().getParent().getFullName()));
				}
			} else if (key.getReferenceTableName() != null) {
				append(SPACE).append(new GroupSymbol(key.getReferenceTableName()));
			}
			append(SPACE);
			addNames(key.getReferenceColumns());
			appendOptions(key);
		}
	}

	private void addConstraints(List<KeyRecord> constraints, String defaultName, String type) {
		for (int i = 0; i < constraints.size(); i++) {
			KeyRecord constraint = constraints.get(i);
			addConstraint(defaultName + i, type, constraint, true);
		}
	}

	private void addConstraint(String defaultName, String type,
			KeyRecord constraint, boolean addOptions) {
		append(COMMA).append(NEWLINE).append(TAB);
		boolean nameMatches = defaultName.equals(constraint.getName());
		if (!nameMatches) {
			append(CONSTRAINT).append(SPACE).append(SQLStringVisitor.escapeSinglePart(constraint.getName())).append(SPACE);	
		}
		append(type);
		addColumns(constraint.getColumns(), false);
		if (addOptions) {
			appendOptions(constraint);
		}
	}

	private void addColumns(List<Column> columns, boolean includeType) {
		append(LPAREN);
		boolean first = true;
		for (Column c:columns) {
			if (first) {
				first = false;
			}
			else {
				append(COMMA).append(SPACE);
			}
			if (includeType) {
				appendColumn(c, true, includeType);
				appendColumnOptions(c);
			} else if (c.getParent() instanceof KeyRecord) {
				//function based column
				append(c.getNameInSource());
			} else {
				append(SQLStringVisitor.escapeSinglePart(c.getName()));
			}
		}
		append(RPAREN);
	}

	private void addNames(List<String> columns) {
		if (columns != null) {
			append(LPAREN);
			boolean first = true;
			for (String c:columns) {
				if (first) {
					first = false;
				}
				else {
					append(COMMA).append(SPACE);
				}
				append(SQLStringVisitor.escapeSinglePart(c));
			}
			append(RPAREN);
		}
	}	
	
	private void visit(Table table, Column column) {
		append(NEWLINE).append(TAB);
		if (table.getTableType() == Table.Type.TemporaryTable && column.isAutoIncremented() && column.getNullType() == NullType.No_Nulls && column.getJavaType() == DataTypeManager.DefaultDataClasses.INTEGER) {
			append(SQLStringVisitor.escapeSinglePart(column.getName()));
			append(SPACE);
			append(SERIAL);
		} else {
			appendColumn(column, true, true);
			
			if (column.isAutoIncremented()) {
				append(SPACE).append(AUTO_INCREMENT);
			}
		}
		
		appendDefault(column);
		
		// options
		appendColumnOptions(column);
	}

	private void appendDefault(BaseColumn column) {
		if (column.getDefaultValue() != null) {
			append(SPACE).append(DEFAULT).append(SPACE);
			if (BaseColumn.EXPRESSION_DEFAULT.equalsIgnoreCase(column.getProperty(BaseColumn.DEFAULT_HANDLING, false))) {
				append(column.getDefaultValue());
			} else {
				append(TICK).append(StringUtil.replaceAll(column.getDefaultValue(), TICK, TICK + TICK)).append(TICK);
			}
		}
	}

	private void appendColumn(BaseColumn column, boolean includeName, boolean includeType) {
		if (includeName) {
			append(SQLStringVisitor.escapeSinglePart(column.getName()));
		}
		if (includeType) {
			Datatype datatype = column.getDatatype();
			String runtimeTypeName = column.getRuntimeType();
			if (datatype != null) {
				runtimeTypeName = datatype.getRuntimeTypeName();
			}
			if (includeName) {
				append(SPACE);
			}
			append(runtimeTypeName);
			if (LENGTH_DATATYPES.contains(runtimeTypeName)) {
				if (column.getLength() != 0 && (datatype == null || column.getLength() != datatype.getLength())) {
					append(LPAREN).append(column.getLength()).append(RPAREN);
				}
			} else if (PRECISION_DATATYPES.contains(runtimeTypeName) 
					&& !column.isDefaultPrecisionScale()) {
				append(LPAREN).append(column.getPrecision());
				if (column.getScale() != 0) {
					append(COMMA).append(column.getScale());
				}
				append(RPAREN);
			}
			if (datatype != null) {
				for (int dims = column.getArrayDimensions(); dims > 0; dims--) {
					append(Tokens.LSBRACE).append(Tokens.RSBRACE);
				}
			}
			if (column.getNullType() == NullType.No_Nulls) {
				append(SPACE).append(NOT_NULL);
			}
		}
	}	
	
	private void appendColumnOptions(BaseColumn column) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, column);
		
		if (!column.getDatatype().isBuiltin()) {
			addOption(options, UDT, column.getDatatype().getName() + "("+column.getLength()+ ", " +column.getPrecision()+", " + column.getScale()+ ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		}
		
		if (column.getDatatype().getRadix() != 0 && column.getRadix() != column.getDatatype().getRadix()) {
			addOption(options, RADIX, column.getRadix());
		}
		
		buildColumnOptions(column, options);
		
		if (options.length() != 0) {
			append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
	}

	private void buildColumnOptions(BaseColumn baseColumn, 
			StringBuilder options) {
		if (baseColumn instanceof Column) {
			Column column = (Column)baseColumn;
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
			if (column.getSearchType() != null && (!column.getSearchType().equals(column.getDatatype().getSearchType()) || column.isSearchTypeSet())) {
				addOption(options, SEARCHABLE, column.getSearchType().name());
			}
			
			if (column.getMinimumValue() != null) {
				addOption(options, MIN_VALUE, column.getMinimumValue());
			}
			
			if (column.getMaximumValue() != null) {
				addOption(options, MAX_VALUE, column.getMaximumValue());
			}
			
			if (column.getNullValues() != -1) {
				addOption(options, NULL_VALUE_COUNT, column.getNullValues());
			}
			
			if (column.getDistinctValues() != -1) {
				addOption(options, DISTINCT_VALUES, column.getDistinctValues());
			}		
		}
		
		if (baseColumn.getNativeType() != null) {
			addOption(options, NATIVE_TYPE, baseColumn.getNativeType());
		}
		
		buildOptions(baseColumn, options);
	}
	
	private void appendOptions(AbstractMetadataRecord record) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, record);
		buildOptions(record, options);
		if (options.length() != 0) {
			append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}
	}

	private void buildOptions(AbstractMetadataRecord record, StringBuilder options) {
		if (!record.getProperties().isEmpty()) {
			for (Map.Entry<String, String> entry:record.getProperties().entrySet()) {
				addOption(options, entry.getKey(), entry.getValue());
			}
		}
	}	
	
	private void addOption(StringBuilder sb, String key, Object value) {
		if (sb.length() != 0) {
			sb.append(COMMA).append(SPACE);
		}
		if (value != null) {
			value = new Constant(value);
		} else {
			value = Constant.NULL_CONSTANT;
		}
		if (key != null && key.length() > 2 && key.charAt(0) == '{') { 
			String origKey = key;
			int index = key.indexOf('}');
			if (index > 1) {
				String uri = key.substring(1, index);
				key = key.substring(index + 1, key.length());
				String prefix = BUILTIN_PREFIXES.get(uri);
				if ((prefix == null && usePrefixes) || createNS) {
					if (prefixMap == null) {
						prefixMap = new LinkedHashMap<String, String>();
					} else {
						prefix = this.prefixMap.get(uri);
					}
					if (prefix == null) {
						prefix = "n"+this.prefixMap.size(); //$NON-NLS-1$						
					}
					this.prefixMap.put(uri, prefix);
				} 
				if (prefix != null) {
					key = prefix + ":" + key; //$NON-NLS-1$
				} else {
					key = origKey;
				}
			}
		}
		sb.append(SQLStringVisitor.escapeSinglePart(key)).append(SPACE).append(value);
	}
	
	private void visit(Procedure procedure) {
		if (this.filter != null && !filter.matcher(procedure.getName()).matches()) {
			return;
		}
		
		append(CREATE).append(SPACE);
		if (procedure.isVirtual()) {
			append(VIRTUAL);
		}
		else {
			append(FOREIGN);
		}
		append(SPACE).append(procedure.isFunction()?FUNCTION:PROCEDURE).append(SPACE).append(SQLStringVisitor.escapeSinglePart(procedure.getName()));
		append(LPAREN);
		
		boolean first = true;
		for (ProcedureParameter pp:procedure.getParameters()) {
			if (first) {
				first = false;
			}
			else {
				append(COMMA).append(SPACE);
			}
			visit(pp);
		}
		append(RPAREN);
		
		if (procedure.getResultSet() != null) {
			append(SPACE).append(RETURNS);
			appendOptions(procedure.getResultSet());
			append(SPACE).append(TABLE).append(SPACE);
			addColumns(procedure.getResultSet().getColumns(), true);
		}
		/* The parser treats the RETURN clause as optional for a procedure if using the RESULT param
		  for (ProcedureParameter pp: procedure.getParameters()) {
			if (pp.getType().equals(Type.ReturnValue)) {
				append(SPACE).append(RETURNS).append(SPACE);
				appendColumn(buffer, pp, false, true);
				break;
			}
		}*/
		
		//options
		String options = buildProcedureOptions(procedure);		
		if (!options.isEmpty()) {
			append(NEWLINE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}		
		//block
		if (procedure.isVirtual()) {
			append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE);
			String plan = procedure.getQueryPlan();
			append(plan);
		}
		
		append(SEMICOLON);
	}

	private String buildProcedureOptions(Procedure procedure) {
		StringBuilder options = new StringBuilder();
		addCommonOptions(options, procedure);
		
		if (procedure.getUpdateCount() != Procedure.AUTO_UPDATECOUNT) {
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
		append(typeStr).append(SPACE);
		appendColumn(param, true, true);
		if (type == Type.ReturnValue) {
			append(SPACE).append(NonReserved.RESULT);
		}
		appendDefault(param);
		appendColumnOptions(param);
	}	

	private void visit(FunctionMethod function) {
		if (this.filter != null && !filter.matcher(function.getName()).matches()) {
			return;
		}		
		append(CREATE).append(SPACE);
		if (function.getPushdown().equals(FunctionMethod.PushDown.MUST_PUSHDOWN)) {
			append(FOREIGN);
		}
		else {
			append(VIRTUAL);
		}
		append(SPACE).append(FUNCTION).append(SPACE).append(SQLStringVisitor.escapeSinglePart(function.getName()));
		append(LPAREN);
		
		boolean first = true;
		for (FunctionParameter fp:function.getInputParameters()) {
			if (first) {
				first = false;
			}
			else {
				append(COMMA).append(SPACE);
			}
			visit(fp);
		}
		append(RPAREN);
		
		append(SPACE).append(RETURNS);
		appendOptions(function.getOutputParameter());
		append(SPACE);
		append(function.getOutputParameter().getType());
		
		//options
		String options = buildFunctionOptions(function);		
		if (!options.isEmpty()) {
			append(NEWLINE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
		}		
		
		/*if (function.getDefinition() != null) {
			append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE);
			append(function.getDefinition());
		}*/
		
		append(SQLConstants.Tokens.SEMICOLON);		
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
			append(NonReserved.VARIADIC).append(SPACE);
		}
		appendColumn(param, true, true);
	}

    @Override
    public String toString() {
    	if (this.prefixMap != null) {
    		StringBuilder sb = new StringBuilder();
    		for (Map.Entry<String, String> entry : this.prefixMap.entrySet()) {
    			sb.append("SET NAMESPACE '").append(StringUtil.replaceAll(entry.getKey(), "'", "''")).append('\'') //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    			.append(" AS " ).append(SQLStringVisitor.escapeSinglePart(entry.getValue())).append(";\n"); //$NON-NLS-1$  //$NON-NLS-2$
    		}
    		return sb.append("\n").toString() + buffer.toString(); //$NON-NLS-1$
    	}
        return buffer.toString();
    }
}
