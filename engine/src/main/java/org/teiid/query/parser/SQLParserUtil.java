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

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.DDLConstants;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;

public class SQLParserUtil {
	
    static final Pattern udtPattern = Pattern.compile("(\\w+)\\s*\\(\\s*(\\d+),\\s*(\\d+),\\s*(\\d+)\\)"); //$NON-NLS-1$
    
    static final Pattern hintPattern = Pattern.compile("\\s*(\\w+(?:\\(\\s*(max:\\d+)?\\s*((?:no)?\\s*join)\\s*\\))?)\\s*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
	
	public static final boolean DECIMAL_AS_DOUBLE = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.decimalAsDouble", false); //$NON-NLS-1$
	
	String prependSign(String sign, String literal) {
		if (sign != null && sign.charAt(0) == '-') {
			return sign + literal;
		}
		return literal;
	}
	
	void convertToParameters(List<Expression> values, StoredProcedure storedProcedure, int paramIndex) {
		for (Expression value : values) {
			SPParameter parameter = new SPParameter(paramIndex++, value);
			parameter.setParameterType(SPParameter.IN);
			storedProcedure.setParameter(parameter);
		}
	}
	
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

    String validateName(String id, boolean nonAlias) throws ParseException {
        if(id.indexOf('.') != -1) { 
            String key = "SQLParser.Invalid_alias"; //$NON-NLS-1$
            if (nonAlias) {
                key = "SQLParser.Invalid_short_name"; //$NON-NLS-1$
            }
            throw new ParseException(QueryPlugin.Util.getString(key, id)); 
        }
        return id;
    }
    
    static String removeEscapeChars(String str, String tickChar) {
        return StringUtil.replaceAll(str, tickChar + tickChar, tickChar);
    }
    
    void setFromClauseOptions(Token groupID, FromClause fromClause){
    	String comment = getComment(groupID);
    	if (comment == null || comment.isEmpty()) {
    		return;
    	}
    	Matcher m = hintPattern.matcher(comment);
    	int start = 0;
    	boolean makedep = false;
    	while (m.find(start)) {
    		String hint = m.group(1);
    		start = m.end();
    		if (StringUtil.startsWithIgnoreCase(hint, "make")) { //$NON-NLS-1$
        		if (hint.equalsIgnoreCase(Option.MAKENOTDEP)) {
                    fromClause.setMakeNotDep(true);
        		} else if (StringUtil.startsWithIgnoreCase(hint, Option.MAKEDEP)) {
        			Option.MakeDep option = new Option.MakeDep();
                    fromClause.setMakeDep(option);
                    parseOptions(m, option);
        		} else if (StringUtil.startsWithIgnoreCase(hint, SQLConstants.Reserved.MAKEIND)) {
        			Option.MakeDep option = new Option.MakeDep();
                    fromClause.setMakeInd(option);
                    parseOptions(m, option);
                }
        	} else if (hint.equalsIgnoreCase(SubqueryHint.NOUNNEST)) {
            	fromClause.setNoUnnest(true);
            } else if (hint.equalsIgnoreCase(FromClause.PRESERVE)) {
            	fromClause.setPreserve(true);
            } else if (hint.equalsIgnoreCase(Option.OPTIONAL)) {
                fromClause.setOptional(true);
            }
    	}
    }

	//([max:val] [[no] join])
	private void parseOptions(Matcher m, Option.MakeDep option) {
		if (m.group(3) != null) {
			if (StringUtil.startsWithIgnoreCase(m.group(3), "no")) { //$NON-NLS-1$
				option.setJoin(false);
			} else {
				option.setJoin(true);
			}
		}
		if (m.group(2) != null) {
			option.setMax(Integer.valueOf(m.group(2).trim().substring(4)));
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
                hint.setDepJoin();
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
	
	private static Pattern SOURCE_HINT = Pattern.compile("\\s*sh(\\s+KEEP ALIASES)?\\s*(?::((?:'[^']*')+))?\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
	private static Pattern SOURCE_HINT_ARG = Pattern.compile("\\s*([^: ]+)(\\s+KEEP ALIASES)?\\s*:((?:'[^']*')+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
	
	SourceHint getSourceHint(SQLParser parser) {
		int index = 1; 
		//scan for the first keyword
	    Token t = null;
	    do {
	    	t = parser.getToken(index++);
	    } while (t != null && t.kind == SQLParserConstants.LPAREN);
	    t = parser.getToken(index);
	    if (t == null) {
	    	return null;
	    }
		String comment = getComment(t);
		Matcher matcher = SOURCE_HINT.matcher(comment);
		if (!matcher.find()) {
			return null;
		}
		SourceHint sourceHint = new SourceHint();
		if (matcher.group(1) != null) {
			sourceHint.setUseAliases(true);
		}
		String generalHint = matcher.group(2);
		if (generalHint != null) {
			sourceHint.setGeneralHint(normalizeStringLiteral(generalHint));
		}
		int end = matcher.end();
		matcher = SOURCE_HINT_ARG.matcher(comment);
		while (matcher.find(end)) {
			end = matcher.end();
			sourceHint.setSourceHint(matcher.group(1), normalizeStringLiteral(matcher.group(3)), matcher.group(2) != null);
		}
		return sourceHint;
	}
	
	void setSourceHint(SourceHint sourceHint, Command command) {
	    if (sourceHint != null) {
	        if (command instanceof SetQuery) {
	        	((SetQuery)command).getProjectedQuery().setSourceHint(sourceHint);
	        } else {
	    		command.setSourceHint(sourceHint);
	    	}
	    }
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
	
	private static Pattern HINT = Pattern.compile("\\s*/\\*([^/]*)\\*/", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
	private static Pattern CACHE_HINT = Pattern.compile("\\+?\\s*cache(\\(\\s*(pref_mem)?\\s*(ttl:\\d{1,19})?\\s*(updatable)?\\s*(scope:(session|vdb|user))?\\s*(min:\\d{1,19})?[^\\)]*\\))?[^\\*]*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
    
	static CacheHint getQueryCacheOption(String query) {
    	Matcher hintMatch = HINT.matcher(query);
    	int start = 0;
    	while (hintMatch.find()) {
    		if (start != hintMatch.start()) {
    			break;
    		}
    		start = hintMatch.end();
    		Matcher match = CACHE_HINT.matcher(hintMatch.group(1));
    		if (!match.matches()) {
    			continue;
    		}
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
    		String min = match.group(7);
    		if (min != null) {
    			hint.setMinRows(Long.valueOf(min.substring(4)));
    		}
    		return hint;
    	}
    	return null;
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
    
    void setColumnOptions(BaseColumn c)  throws MetadataException {
    	Map<String, String> props = c.getProperties();
		setCommonProperties(c, props);
		
    	String v = props.remove(DDLConstants.RADIX); 
    	if (v != null) {
    		c.setRadix(Integer.parseInt(v));
    	}
    	
    	v = props.remove(DDLConstants.NATIVE_TYPE);
    	if (v != null) {
    		c.setNativeType(v);
    	}
    	
    	if (c instanceof Column) {
    		setColumnOptions((Column)c, props);
    	}
    }
    
    void removeColumnOption(String key, BaseColumn c)  throws MetadataException {
    	if (c.getProperty(key, false) != null) {
    		c.setProperty(key, null);
    	}    	
		removeCommonProperty(key, c);
		
    	if (key.equals(DDLConstants.RADIX)) {
    		c.setRadix(0);
    	} else if (key.equals(DDLConstants.NATIVE_TYPE)) {
    		c.setNativeType(null);
    	} else if (c instanceof Column) {
    		removeColumnOption(key, (Column)c);
    	}
    }    
    
    private void removeColumnOption(String key, Column c) {
        if (key.equals(DDLConstants.CASE_SENSITIVE)) {
        	c.setCaseSensitive(false);
        } else if (key.equals(DDLConstants.SELECTABLE)) {
    		c.setSelectable(true);
    	} else if (key.equals(DDLConstants.UPDATABLE)) {
    		c.setUpdatable(false);
    	} else if (key.equals(DDLConstants.SIGNED)) {
    		c.setSigned(false);
    	} else if (key.equals(DDLConstants.CURRENCY)) {
    		c.setSigned(false);
    	} else if (key.equals(DDLConstants.FIXED_LENGTH)) {
    		c.setFixedLength(false);
    	} else if (key.equals(DDLConstants.SEARCHABLE)) {
    		c.setSearchType(null);
    	} else if (key.equals(DDLConstants.MIN_VALUE)) {
    		c.setMinimumValue(null);
    	} else if (key.equals(DDLConstants.MAX_VALUE)) {
    		c.setMaximumValue(null);
    	} else if (key.equals(DDLConstants.CHAR_OCTET_LENGTH)) {
    		c.setCharOctetLength(0);
    	} else if (key.equals(DDLConstants.NULL_VALUE_COUNT)) {
    		c.setNullValues(-1);
    	} else if (key.equals(DDLConstants.DISTINCT_VALUES)) {
    		c.setDistinctValues(-1);
    	} else if (key.equals(DDLConstants.UDT)) {
			c.setDatatype(null);
			c.setLength(0);
			c.setPrecision(0);
			c.setScale(0);
    	}    	
    }

	private void setColumnOptions(Column c, Map<String, String> props) throws MetadataException {
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
    		c.setSearchType(StringUtil.caseInsensitiveValueOf(SearchType.class, v));
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
    			throw new MetadataException(QueryPlugin.Util.getString("udt_format_wrong", c.getName())); //$NON-NLS-1$
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
	
	void removeCommonProperty(String key, AbstractMetadataRecord c) {
		if (key.equals(DDLConstants.UUID)) {
			c.setUUID(null);
		} else if (key.equals(DDLConstants.ANNOTATION)) {
    		c.setAnnotation(null);
    	} else if (key.equals(DDLConstants.NAMEINSOURCE)) {
			c.setNameInSource(null);
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
			table.setCardinality(Long.valueOf(value));
    	}
    }     
    
    void removeTableOption(String key, Table table) {
    	if (table.getProperty(key, false) != null) {
    		table.setProperty(key, null);
    	}
    	removeCommonProperty(key, table);
    	
    	if (key.equals(DDLConstants.MATERIALIZED)) {
    		table.setMaterialized(false);
    	}
    	
    	if (key.equals(DDLConstants.MATERIALIZED_TABLE)) {
    		table.setMaterializedTable(null);
    	}
    	
    	if (key.equals(DDLConstants.UPDATABLE)) {
    		table.setSupportsUpdate(false);
    	}
    	
    	if (key.equals(DDLConstants.CARDINALITY)) {
    		table.setCardinality(-1);
    	}    	
    }
    
	static void replaceProcedureWithFunction(MetadataFactory factory,
			Procedure proc) throws MetadataException {
		if (proc.isFunction() && proc.getQueryPlan() != null) {
			return;
		}
		FunctionMethod method = createFunctionMethod(proc);

		//remove the old proc
		factory.getSchema().getResolvingOrder().remove(factory.getSchema().getResolvingOrder().size() - 1);
		factory.getSchema().getProcedures().remove(proc.getName());
		
		factory.getSchema().addFunction(method);
	}

	public static FunctionMethod createFunctionMethod(Procedure proc) {
		FunctionMethod method = new FunctionMethod();
		method.setName(proc.getName());
		method.setPushdown(proc.isVirtual()?FunctionMethod.PushDown.CAN_PUSHDOWN:FunctionMethod.PushDown.MUST_PUSHDOWN);
		
		ArrayList<FunctionParameter> ins = new ArrayList<FunctionParameter>();
		for (ProcedureParameter pp:proc.getParameters()) {
			if (pp.getType() == ProcedureParameter.Type.InOut || pp.getType() == ProcedureParameter.Type.Out) {
				throw new MetadataException(QueryPlugin.Util.getString("SQLParser.function_in", proc.getName())); //$NON-NLS-1$
			}
			//copy the metadata
			FunctionParameter fp = new FunctionParameter(pp.getName(), pp.getRuntimeType(), pp.getAnnotation());
			fp.setDatatype(pp.getDatatype(), true, pp.getArrayDimensions());
			fp.setLength(pp.getLength());
			fp.setNameInSource(pp.getNameInSource());
			fp.setNativeType(pp.getNativeType());
			fp.setNullType(pp.getNullType());
			fp.setProperties(pp.getProperties());
			fp.setRadix(pp.getRadix());
			fp.setScale(pp.getScale());
			fp.setUUID(pp.getUUID());
			if (pp.getType() == ProcedureParameter.Type.In) {
				fp.setVarArg(pp.isVarArg());
				ins.add(fp);
				fp.setPosition(ins.size());
			} else {
				method.setOutputParameter(fp);
				fp.setPosition(0);
			}
		}
		method.setInputParameters(ins);
		
		if (proc.getResultSet() != null || method.getOutputParameter() == null) {
			throw new MetadataException(QueryPlugin.Util.getString("SQLParser.function_return", proc.getName())); //$NON-NLS-1$
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
		if (method.getInvocationMethod() != null) {
    		method.setPushdown(PushDown.CAN_PUSHDOWN);
    	}
		return method;
	}
    
    void setProcedureOptions(Procedure proc) {
    	Map<String, String> props = proc.getProperties();
    	setCommonProperties(proc, props);
    	
    	String value = props.remove("UPDATECOUNT"); //$NON-NLS-1$
    	if (value != null) {
    		proc.setUpdateCount(Integer.parseInt(value));
    	}
    }
    
    void removeOption(String option, AbstractMetadataRecord record) {
    	if (record instanceof Table) {
    		removeTableOption(option, (Table)record);
    	}
    	if (record instanceof Procedure) {
    		removeProcedureOption(option, (Procedure)record);
    	}
    	if (record instanceof BaseColumn) {
    		removeColumnOption(option, (BaseColumn)record);
    	}
    }
    
    void setOptions(AbstractMetadataRecord record) {
    	if (record instanceof Table) {
    		setTableOptions((Table)record);
    	}
    	if (record instanceof Procedure) {
    		setProcedureOptions((Procedure)record);
    	}
    	if (record instanceof BaseColumn) {
    		setColumnOptions((BaseColumn)record);
    	}
    }
    
    void removeProcedureOption(String key, Procedure proc) {
    	if (proc.getProperty(key, false) != null) {
    		proc.setProperty(key, null);
    	}    	
    	removeCommonProperty(key, proc);
    	
    	if (key.equals("UPDATECOUNT")) { //$NON-NLS-1$
    		proc.setUpdateCount(1);
    	}
    }    

    public static boolean isTrue(final String text) {
        return Boolean.valueOf(text);
    }    
    
    AbstractMetadataRecord getChild(String name, AbstractMetadataRecord record, boolean parameter) {
    	if (record instanceof Table) {
    		if (parameter) {
    			throw new MetadataException(QueryPlugin.Util.getString("SQLParser.alter_table_param", name, record.getName())); //$NON-NLS-1$
    		}
    		return getColumn(name, (Table)record);
    	}
		return getColumn(name, (Procedure)record, parameter);
    	//TODO: function is not supported yet because we store by uid, which should instead be a more friendly "unique name"
    }
	
	Column getColumn(String columnName, Table table) throws MetadataException {
		Column c = table.getColumnByName(columnName);
		if (c != null) {
			return c;
		}
		throw new MetadataException(QueryPlugin.Util.getString("SQLParser.no_column", columnName, table.getName())); //$NON-NLS-1$
	}
	
	AbstractMetadataRecord getColumn(String paramName, Procedure proc, boolean parameter) throws MetadataException {
		if (proc.getResultSet() != null) {
			Column result = proc.getResultSet().getColumnByName(paramName);
			if (result != null) {
				return result;
			}
		}
		if (parameter) {
			List<ProcedureParameter> params = proc.getParameters();
			for (ProcedureParameter param:params) {
				if (param.getName().equalsIgnoreCase(paramName)) {
					return param;
				}
			}
		}
		throw new MetadataException(QueryPlugin.Util.getString("SQLParser.alter_procedure_param_doesnot_exist", paramName, proc.getName())); //$NON-NLS-1$
	}
	
	FunctionParameter getParameter(String paramName, FunctionMethod func) throws MetadataException {
		List<FunctionParameter> params = func.getInputParameters();
		for (FunctionParameter param:params) {
			if (param.getName().equalsIgnoreCase(paramName)) {
				return param;
			}
		}
		throw new MetadataException(QueryPlugin.Util.getString("SQLParser.alter_function_param_doesnot_exist", paramName, func.getName())); //$NON-NLS-1$
	}	
	
	void createDDLTrigger(MetadataFactory schema, AlterTrigger trigger) {
		GroupSymbol group = trigger.getTarget();
		
		Table table = schema.getSchema().getTable(group.getName());
		if (table == null || !table.isVirtual()) {
			throw new MetadataException(QueryPlugin.Util.getString("SQLParser.view_doesnot_exist", group.getName())); //$NON-NLS-1$
		}
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
	
	BaseColumn addProcColumn(MetadataFactory factory, Procedure proc, String name, ParsedDataType type, boolean rs) throws MetadataException {
		BaseColumn column = null;
		if (rs) {
			column = factory.addProcedureResultSetColumn(name, type.type, proc);
		} else {
			boolean added = false;
			for (ProcedureParameter pp : proc.getParameters()) {
				if (pp.getType() == Type.ReturnValue) {
					added = true;
					if (pp.getDatatype() != factory.getDataTypes().get(type.type)) {
						throw new MetadataException(QueryPlugin.Util.getString("SQLParser.proc_type_conflict", proc.getName(), pp.getDatatype(), type.type)); //$NON-NLS-1$
					}
				}
			}
			if (!added) {
				column = factory.addProcedureParameter(name, type.type, ProcedureParameter.Type.ReturnValue, proc);
			}
		}
		setTypeInfo(type, column);
		return column;
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
	
	KeyRecord addFBI(MetadataFactory factory, List<Expression> expressions, Table table, String name) throws MetadataException {
		List<String> columnNames = new ArrayList<String>(expressions.size());
		List<Boolean> nonColumnExpressions = new ArrayList<Boolean>(expressions.size());
		boolean fbi = false;
		for (int i = 0; i < expressions.size(); i++) {
			Expression ex = expressions.get(i);
			if (ex instanceof ElementSymbol) {
	 			columnNames.add(((ElementSymbol)ex).getName());
	 			nonColumnExpressions.add(Boolean.FALSE);
			} else {
				columnNames.add(ex.toString());
				nonColumnExpressions.add(Boolean.TRUE);
				fbi = true;
			}
		}
    	return factory.addFunctionBasedIndex(name != null?name:(SQLConstants.NonReserved.INDEX+(fbi?table.getFunctionBasedIndexes().size():table.getIndexes().size())), columnNames, nonColumnExpressions, table);
	}
	
	MetadataFactory getTempMetadataFactory() {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		return workContext.getTempMetadataFactory();
	}
	
	List<Expression> arrayExpressions(List<Expression> expressions, Expression expr) {
		if (expressions == null) {
			expressions = new ArrayList<Expression>();
		}
		if (expr != null) {
			expressions.add(expr);
		}
		return expressions;
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
	
	public static void setDefault(BaseColumn column, Expression value) {
		if ((value instanceof Constant) && value.getType() == DataTypeManager.DefaultDataClasses.STRING) {
			column.setDefaultValue(((Constant)value).getValue().toString());
		} else {
			//it's an expression
			column.setProperty(BaseColumn.DEFAULT_HANDLING, BaseColumn.EXPRESSION_DEFAULT);
			column.setDefaultValue(value.toString());
		}
	}
}
