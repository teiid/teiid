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

package org.teiid.query.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.DDLConstants;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.TranslatorException;

public class SQLParserUtil {
	
    static Pattern udtPattern = Pattern.compile("(\\w+)\\s*\\(\\s*(\\d+),\\s*(\\d+),\\s*(\\d+)\\)"); //$NON-NLS-1$
	
	public static final boolean DECIMAL_AS_DOUBLE = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.decimalAsDouble", false); //$NON-NLS-1$
	
	String matchesAny(String arg, String ... expected) {
		for (String string : expected) {
			if (string.equalsIgnoreCase(arg)) {
				return arg;
			}
		}
		return null;
	}
	
	String normalizeStringLiteral(String s) {
		int start = 1;
		boolean unescape = false;
  		if (s.charAt(0) == 'N') {
  			start++;
  		} else if (s.charAt(0) == 'E') {
  			start++;
  			unescape = true;
  		}
  		char tickChar = s.charAt(start - 1);
  		s = s.substring(start, s.length() - 1);
  		String result = removeEscapeChars(s, String.valueOf(tickChar));
  		if (unescape) {
  			result = FunctionMethods.unescape(result);
  		}
  		return result;
	}
	
	public static String normalizeId(String s) {
		if (s.indexOf('"') == -1) {
			return s;
		}
		List<String> nameParts = new LinkedList<String>();
		while (s.length() > 0) {
			if (s.charAt(0) == '"') {
				boolean escape = false;
				for (int i = 1; i < s.length(); i++) {
					if (s.charAt(i) != '"') {
						continue;
					}
					escape = !escape;
					boolean end = i == s.length() - 1;
					if (end || (escape && s.charAt(i + 1) == '.')) {
				  		String part = s.substring(1, i);
				  		s = s.substring(i + (end?1:2));
				  		nameParts.add(removeEscapeChars(part, "\"")); //$NON-NLS-1$
				  		break;
					}
				}
			} else {
				int index = s.indexOf('.');
				if (index == -1) {
					nameParts.add(s);
					break;
				} 
				nameParts.add(s.substring(0, index));
				s = s.substring(index + 1);
			}
		}
		StringBuilder sb = new StringBuilder();
		for (Iterator<String> i = nameParts.iterator(); i.hasNext();) {
			sb.append(i.next());
			if (i.hasNext()) {
				sb.append('.');
			}
		}
		return sb.toString();
	}
	
    /**
     * Check if this is a valid string literal
     * @param id Possible string literal
     */
    boolean isStringLiteral(String str, ParseInfo info) {
    	if (info.useAnsiQuotedIdentifiers() || str.charAt(0) != '"' || str.charAt(str.length() - 1) != '"') {
    		return false;
    	}
    	int index = 1;
    	while (index < str.length() - 1) {
    		index = str.indexOf('"', index);
    		if (index == -1 || index + 1 == str.length()) {
    			return true;
    		}
    		if (str.charAt(index + 1) != '"') {
    			return false;
    		}
    		index += 2;
    	}
    	return true;
    }    

    /**
     * Check that this is a valid alias, remove quotes, and return updated
     * alias string.
     * @param id Metadata alias
     */
    String validateAlias(String id) throws ParseException {
        return validateName(id, false);
    }

    private String validateName(String id, boolean element) throws ParseException {
        if(id.indexOf('.') != -1) { 
            String key = "SQLParser.Invalid_alias"; //$NON-NLS-1$
            if (element) {
                key = "SQLParser.Invalid_short_name"; //$NON-NLS-1$
            }
            throw new ParseException(QueryPlugin.Util.getString(key, id)); 
        }
        return id;
    }
    
    /**
     * Check that this is a valid element name, remove quotes
     * @param id Metadata alias
     */
    String validateElementName(String id) throws ParseException {
        return validateName(id, true);
    }
    
    static String removeEscapeChars(String str, String tickChar) {
        return StringUtil.replaceAll(str, tickChar + tickChar, tickChar);
    }
    
    void setFromClauseOptions(Token groupID, FromClause fromClause){
        String[] parts = getComment(groupID).split("\\s"); //$NON-NLS-1$

        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase(Option.OPTIONAL)) {
                fromClause.setOptional(true);
            } else if (parts[i].equalsIgnoreCase(Option.MAKEDEP)) {
                fromClause.setMakeDep(true);
            } else if (parts[i].equalsIgnoreCase(Option.MAKENOTDEP)) {
                fromClause.setMakeNotDep(true);
            } else if (parts[i].equalsIgnoreCase(FromClause.MAKEIND)) {
                fromClause.setMakeInd(true);
            } else if (parts[i].equalsIgnoreCase(SubqueryHint.NOUNNEST)) {
            	fromClause.setNoUnnest(true);
            }
        }
    }
    
    SubqueryHint getSubqueryHint(Token t) {
    	SubqueryHint hint = new SubqueryHint();
    	String[] parts = getComment(t).split("\\s"); //$NON-NLS-1$
    	for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase(SubqueryHint.MJ)) {
                hint.setMergeJoin(true);
            } else if (parts[i].equalsIgnoreCase(SubqueryHint.NOUNNEST)) {
            	hint.setNoUnnest(true);
            } else if (parts[i].equalsIgnoreCase(SubqueryHint.DJ)) {
                hint.setDepJoin(true);
            }
        }
    	return hint;
    }
    
	String getComment(Token t) {
		Token optToken = t.specialToken;
        if (optToken == null) { 
            return ""; //$NON-NLS-1$
        }
        //handle nested comments
        String image = optToken.image;
        while (optToken.specialToken != null) {
        	optToken = optToken.specialToken;
        	image = optToken.image + image;
        }
        String hint = image.substring(2, image.length() - 2);
        if (hint.startsWith("+")) { //$NON-NLS-1$
        	hint = hint.substring(1);
        }
        return hint;
	}
	
	private static Pattern SOURCE_HINT = Pattern.compile("\\s*sh(?::((?:'[^']*')+))?\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
	private static Pattern SOURCE_HINT_ARG = Pattern.compile("\\s*([^:]+):((?:'[^']*')+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
	
	SourceHint getSourceHint(Token t) {
		String comment = getComment(t);
		Matcher matcher = SOURCE_HINT.matcher(comment);
		if (!matcher.find()) {
			return null;
		}
		SourceHint sourceHint = new SourceHint();
		String generalHint = matcher.group(1);
		if (generalHint != null) {
			sourceHint.setGeneralHint(normalizeStringLiteral(generalHint));
		}
		int end = matcher.end();
		matcher = SOURCE_HINT_ARG.matcher(comment);
		while (matcher.find(end)) {
			end = matcher.end();
			sourceHint.setSourceHint(matcher.group(1), normalizeStringLiteral(matcher.group(2)));
		}
		return sourceHint;
	}
	
	boolean isNonStrictHint(Token t) {
		String[] parts = getComment(t).split("\\s"); //$NON-NLS-1$
    	for (int i = 0; i < parts.length; i++) {
    		if (parts[i].equalsIgnoreCase(Limit.NON_STRICT)) {
    			return true;
    		}
    	}
    	return false;
	}
	
	private static Pattern CACHE_HINT = Pattern.compile("/\\*\\+?\\s*cache(\\(\\s*(pref_mem)?\\s*(ttl:\\d{1,19})?\\s*(updatable)?\\s*(scope:(session|vdb|user))?[^\\)]*\\))?[^\\*]*\\*\\/.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
    
	static CacheHint getQueryCacheOption(String query) {
    	Matcher match = CACHE_HINT.matcher(query);
    	if (match.matches()) {
    		CacheHint hint = new CacheHint();
    		if (match.group(2) !=null) {
    			hint.setPrefersMemory(true);
    		}
    		String ttl = match.group(3);
    		if (ttl != null) {
    			hint.setTtl(Long.valueOf(ttl.substring(4)));
    		}
    		if (match.group(4) != null) {
    			hint.setUpdatable(true);
    		}
    		String scope =  match.group(5);
    		if (scope != null) {
    			scope = scope.substring(6);
    			hint.setScope(scope);
    		}    		
    		return hint;
    	}
    	return null;
    }

    /**
     * Helper for the FROM clause that takes the join type string and adds
     * the join type to the From clause based on that type.
     * @param groupID Left group ID
     * @param rid Right group ID
     * @param joinType Join type word from query
     * @param from From clause to update
     * @throws ParseException if parsing failed
     */
    JoinType getJoinType(Token joinTypeToken) throws ParseException {
        if(joinTypeToken == null) { 
            return JoinType.JOIN_INNER;
        }   
        String joinType = joinTypeToken.image;
        if(joinType.equalsIgnoreCase(Reserved.INNER)) {
            return JoinType.JOIN_INNER;
        } else if(joinType.equalsIgnoreCase(Reserved.CROSS)) {
            return JoinType.JOIN_CROSS;         
        } else if(joinType.equalsIgnoreCase(Reserved.LEFT)) {
            return JoinType.JOIN_LEFT_OUTER;
        } else if(joinType.equalsIgnoreCase(Reserved.RIGHT)) {
            return JoinType.JOIN_RIGHT_OUTER;
        } else if(joinType.equalsIgnoreCase(Reserved.FULL)) {
            return JoinType.JOIN_FULL_OUTER;
        } else if(joinType.equalsIgnoreCase(Reserved.UNION)) {
            return JoinType.JOIN_UNION;
        } else {
            Object[] params = new Object[] { joinType };
            throw new ParseException(QueryPlugin.Util.getString("SQLParser.Unknown_join_type", params)); //$NON-NLS-1$
        }
    }
    
    int getOperator(String opString) {
        if (opString.equals("=")) { //$NON-NLS-1$
            return CompareCriteria.EQ;
        } else if (opString.equals("<>") || opString.equals("!=")) { //$NON-NLS-1$ //$NON-NLS-2$
            return CompareCriteria.NE;
        } else if (opString.equals("<")) { //$NON-NLS-1$
            return CompareCriteria.LT;
        } else if (opString.equals(">")) { //$NON-NLS-1$
            return CompareCriteria.GT;
        } else if (opString.equals("<=")) { //$NON-NLS-1$
            return CompareCriteria.LE;
        } else if (opString.equals(">=")) { //$NON-NLS-1$
            return CompareCriteria.GE;
        }
        
        Assertion.failed("unknown operator"); //$NON-NLS-1$
        return 0;
    }
    
    SetQuery addQueryToSetOperation(QueryCommand query, QueryCommand rightQuery, SetQuery.Operation type, boolean all) {
        SetQuery setQuery = new SetQuery(type, all, query, rightQuery);
        return setQuery;
    }
    
    static Block asBlock(Statement stmt) {
    	if (stmt == null) {
    		return null;
    	}
    	if (stmt instanceof Block) {
    		return (Block)stmt;
    	}
    	return new Block(stmt);
    }
    
    void setColumnOptions(BaseColumn c)  throws ParseException {
    	Map<String, String> props = c.getProperties();
		setCommonProperties(c, props);
		
    	String v = props.remove(DDLConstants.RADIX); 
    	if (v != null) {
    		c.setRadix(Integer.parseInt(v));
    	}
    	
    	if (c instanceof Column) {
    		setColumnOptions((Column)c, props);
    	}
    }

	private void setColumnOptions(Column c, Map<String, String> props) throws ParseException {
		String v = props.remove(DDLConstants.CASE_SENSITIVE); 
        if (v != null) {
        	c.setCaseSensitive(isTrue(v));
        }
    	
    	v = props.remove(DDLConstants.SELECTABLE);
    	if (v != null) {
    		c.setSelectable(isTrue(v));
    	}
    	
    	v = props.remove(DDLConstants.UPDATABLE); 
    	if (v != null) {
    		c.setUpdatable(isTrue(v));
    	}
    	
    	v = props.remove(DDLConstants.SIGNED);
    	if (v != null) {
    		c.setSigned(isTrue(v));
    	}
    	
    	v = props.remove(DDLConstants.CURRENCY);
    	if (v != null) {
    		c.setSigned(isTrue(v));
    	}

    	v = props.remove(DDLConstants.FIXED_LENGTH);
    	if (v != null) {
    		c.setFixedLength(isTrue(v));
    	}
    	
    	v = props.remove(DDLConstants.SEARCHABLE);
    	if (v != null) {
    		c.setSearchType(SearchType.valueOf(v.toUpperCase()));
    	}
    	
    	v = props.remove(DDLConstants.MIN_VALUE);
    	if (v != null) {
    		c.setMinimumValue(v);
    	}
    	
    	v = props.remove(DDLConstants.MAX_VALUE);
    	if (v != null) {
    		c.setMaximumValue(v);
    	}
    	
    	v = props.remove(DDLConstants.CHAR_OCTET_LENGTH);
    	if (v != null) {
    		c.setCharOctetLength(Integer.parseInt(v));
    	}
        
    	v = props.remove(DDLConstants.NATIVE_TYPE);
    	if (v != null) {
    		c.setNativeType(v);
    	}

    	v = props.remove(DDLConstants.NULL_VALUE_COUNT); 
    	if (v != null) {
    		c.setNullValues(Integer.parseInt(v));
    	}
    	
    	v = props.remove(DDLConstants.DISTINCT_VALUES); 
    	if (v != null) {
    		c.setDistinctValues(Integer.parseInt(v));
    	}

    	v = props.remove(DDLConstants.UDT); 
    	if (v != null) {
    		Matcher matcher = udtPattern.matcher(v);
    		Map<String, Datatype> datatypes = SystemMetadata.getInstance().getSystemStore().getDatatypes();
    		if (matcher.matches() && datatypes.get(matcher.group(1)) != null) {
    			c.setDatatype(datatypes.get(matcher.group(1)));
    			c.setLength(Integer.parseInt(matcher.group(2)));
    			c.setPrecision(Integer.parseInt(matcher.group(3)));
    			c.setScale(Integer.parseInt(matcher.group(4)));
    		}
    		else {
    			throw new ParseException(QueryPlugin.Util.getString("udt_format_wrong", c.getName())); //$NON-NLS-1$
    		}
    	}
    }

	void setCommonProperties(AbstractMetadataRecord c, Map<String, String> props) {
		String v = props.remove(DDLConstants.UUID); 
		if (v != null) {
			c.setUUID(v);
		}
		
    	v = props.remove(DDLConstants.ANNOTATION); 
    	if (v != null) {
    		c.setAnnotation(v);
    	}
		
		v = props.remove(DDLConstants.NAMEINSOURCE); 
		if (v != null) {
			c.setNameInSource(v);
		}
	}
    
    void setTableOptions(Table table) {
    	Map<String, String> props = table.getProperties();
    	setCommonProperties(table, props);
    	
    	String value = props.remove(DDLConstants.MATERIALIZED); 
    	if (value != null) {
    		table.setMaterialized(isTrue(value));
    	}
		
		value = props.remove(DDLConstants.MATERIALIZED_TABLE); 
		if (value != null) {
    		Table mattable = new Table();
    		mattable.setName(value);
    		table.setMaterializedTable(mattable);
    	}
		
		value = props.remove(DDLConstants.UPDATABLE); 
		if (value != null) {
			table.setSupportsUpdate(isTrue(value));
		}
		
    	value = props.remove(DDLConstants.CARDINALITY); 
    	if (value != null) {
    		table.setCardinality(Integer.parseInt(value));
    	}
    }     
    
	static void replaceProcedureWithFunction(MetadataFactory factory,
			Procedure proc) throws ParseException {
		FunctionMethod method = new FunctionMethod();
		method.setName(proc.getName());
		method.setPushdown(proc.isVirtual()?FunctionMethod.PushDown.CAN_PUSHDOWN:FunctionMethod.PushDown.MUST_PUSHDOWN);
		
		ArrayList<FunctionParameter> ins = new ArrayList<FunctionParameter>();
		for (ProcedureParameter pp:proc.getParameters()) {
			if (pp.getType() == ProcedureParameter.Type.InOut || pp.getType() == ProcedureParameter.Type.Out) {
				throw new ParseException(QueryPlugin.Util.getString("SQLParser.function_in", proc.getName())); //$NON-NLS-1$
			}
			
			FunctionParameter fp = new FunctionParameter(pp.getName(), pp.getRuntimeType(), pp.getAnnotation());
			if (pp.getType() == ProcedureParameter.Type.In) {
				fp.setVarArg(pp.isVarArg());
				ins.add(fp);
			} else {
				method.setOutputParameter(fp);
			}
		}
		method.setInputParameters(ins);
		
		if (proc.getResultSet() != null || method.getOutputParameter() == null) {
			throw new ParseException(QueryPlugin.Util.getString("SQLParser.function_return", proc.getName())); //$NON-NLS-1$
		}
		
		method.setAnnotation(proc.getAnnotation());
		method.setNameInSource(proc.getNameInSource());
		method.setUUID(proc.getUUID());
		
		Map<String, String> props = proc.getProperties();

		String value = props.remove(DDLConstants.CATEGORY); 
		method.setCategory(value);
		
		value = props.remove(DDLConstants.DETERMINISM); 
		if (value != null) {
			method.setDeterminism(FunctionMethod.Determinism.valueOf(value.toUpperCase()));
		}
		
		value = props.remove(DDLConstants.JAVA_CLASS); 
		method.setInvocationClass(value);
		
		value = props.remove(DDLConstants.JAVA_METHOD); 
		method.setInvocationMethod(value);
		
		for (String key:props.keySet()) {
			value = props.get(key);
			method.setProperty(key, value);
		}
		
		FunctionMethod.convertExtensionMetadata(proc, method);
		factory.getSchema().addFunction(method);
		factory.getSchema().getProcedures().remove(proc.getName());
	}
    
    void setProcedureOptions(Procedure proc) {
    	Map<String, String> props = proc.getProperties();
    	setCommonProperties(proc, props);
    	
    	String value = props.remove("UPDATECOUNT"); //$NON-NLS-1$
    	if (value != null) {
    		proc.setUpdateCount(Integer.parseInt(value));
    	}
    }

    public static boolean isTrue(final String text) {
        return Boolean.valueOf(text);
    }    
	
	Column getColumn(String columnName, Table table) throws ParseException {
		Column c = table.getColumnByName(columnName);
		if (c != null) {
			return c;
		}
		throw new ParseException(QueryPlugin.Util.getString("SQLParser.no_column", columnName, table.getName())); //$NON-NLS-1$
	}
	
	void createDDLTrigger(MetadataFactory schema, AlterTrigger trigger) {
		GroupSymbol group = trigger.getTarget();
		
		Table table = schema.getSchema().getTable(group.getName());
		if (trigger.getEvent().equals(Table.TriggerEvent.INSERT)) {
			table.setInsertPlan(trigger.getDefinition().toString());
		}
		else if (trigger.getEvent().equals(Table.TriggerEvent.UPDATE)) {
			table.setUpdatePlan(trigger.getDefinition().toString());
		}
		else if (trigger.getEvent().equals(Table.TriggerEvent.DELETE)) {
			table.setDeletePlan(trigger.getDefinition().toString());
		}
	}
	
	BaseColumn addProcColumn(MetadataFactory factory, Procedure proc, String name, ParsedDataType type, boolean rs) throws ParseException{
		try {
			name = validateElementName(name);
			BaseColumn column = null;
			if (rs) {
				column = factory.addProcedureResultSetColumn(name, type.type, proc);
			} else {
				boolean added = false;
				for (ProcedureParameter pp : proc.getParameters()) {
					if (pp.getType() == Type.ReturnValue) {
						added = true;
						if (pp.getDatatype() != factory.getDataTypes().get(type.type)) {
							throw new ParseException(QueryPlugin.Util.getString("SQLParser.proc_type_conflict", proc.getName(), pp.getDatatype(), type.type)); //$NON-NLS-1$
						}
					}
				}
				if (!added) {
					column = factory.addProcedureParameter(name, type.type, ProcedureParameter.Type.ReturnValue, proc);
				}
			}
			setTypeInfo(type, column);
			return column;
		} catch (TranslatorException e){
			throw new ParseException(e.getMessage());
		}	
	}

	void setTypeInfo(ParsedDataType type, BaseColumn column) {
		if (type.length != null){
			column.setLength(type.length);
		}
		if (type.scale != null){
			column.setScale(type.scale);
		}	
		if (type.precision != null){
			column.setPrecision(type.precision);
		}
	}	
	
	static String resolvePropertyKey(MetadataFactory factory, String key) {
	 	int index = key.indexOf(':');
	 	if (index > 0 && index < key.length() - 1) {
	 		String prefix = key.substring(0, index);
	 		String uri = factory.getNamespaces().get(prefix);
	 		if (uri != null) {
	 			key = '{' +uri + '}' + key.substring(index + 1, key.length());
	 		}
	 		//TODO warnings or errors if not resolvable 
	 	}
	 	return key;
	}
	
	static class  ParsedDataType{
		String type;
		Integer length;
		Integer scale;
		Integer precision;
		
		public ParsedDataType(String type) {
			this.type = type;
		}
		
		public ParsedDataType(String type, int length, boolean precision) {
			this.type = type;
			
			if (precision) {
				this.precision = length;
			}
			else {
				this.length = length;
			}
		}
		
		public ParsedDataType(String type, int length, int scale, boolean precision) {
			this.type = type;
			this.scale = scale;
			if (precision) {
				this.precision = length;
			}
			else {
				this.length = length;
			}			
		}	
	}
}
