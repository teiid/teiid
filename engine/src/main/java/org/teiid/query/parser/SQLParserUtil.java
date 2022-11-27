/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.DDLConstants;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.Symbol;

public class SQLParserUtil {

    static final Pattern hintPattern = Pattern.compile("\\s*(\\w+(?:\\(\\s*(max:\\d+)?\\s*((?:no)?\\s*join)\\s*\\))?)\\s*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    public static final boolean DECIMAL_AS_DOUBLE = PropertiesUtils.getHierarchicalProperty("org.teiid.decimalAsDouble", false, Boolean.class); //$NON-NLS-1$

    public static final boolean RESULT_ANY_POSITION = PropertiesUtils.getHierarchicalProperty("org.teiid.resultAnyPosition", false, Boolean.class); //$NON-NLS-1$

    public static final boolean CONDITION_CONSTRAINT_DEFAULT = PropertiesUtils.getHierarchicalProperty("org.teiid.conditionConstraintDefault", true, Boolean.class); //$NON-NLS-1$

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

    public static String normalizeId(String s) throws ParseException {
        return normalizeId(s, false);
    }

    public static String normalizeId(String s, boolean singlePart) throws ParseException {
        if (s.indexOf('"') == -1 && !singlePart) {
            return s;
        }
        String orig = s;
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
        if (nameParts.size() == 1) {
            return nameParts.get(0);
        }
        if (singlePart) {
            throw new ParseException(QueryPlugin.Util.getString("SQLParser.ddl_id_unqualified", orig)); //$NON-NLS-1$
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
     * @param str Possible string literal
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

    void validateQuotedName(String aliasID, String rawAlias) throws ParseException {
        String adjustedAlias = aliasID;
        if (rawAlias.charAt(0) == '"') {
          adjustedAlias = '"' + StringUtil.replaceAll(aliasID, "\"", "\"\"") + '"'; //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (!rawAlias.equals(adjustedAlias)) {
          throw new ParseException(QueryPlugin.Util.getString("SQLParser.ddl_id_unqualified", rawAlias)); //$NON-NLS-1$
        }
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

    void parseWithHints(Token paren, WithQueryCommand with){
        String comment = getComment(paren);
        if (comment == null || comment.isEmpty()) {
            return;
        }
        String[] parts = comment.split("\\s"); //$NON-NLS-1$
        for (String part : parts) {
            if (WithQueryCommand.NO_INLINE.equalsIgnoreCase(part)) {
                with.setNoInline(true);
            } else if (WithQueryCommand.MATERIALIZE.equalsIgnoreCase(part)) {
                with.setMaterialize(true);
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
        String comment = getFullComment(t, false);
        if (comment.length() == 0) {
            return comment;
        }
        if (comment.startsWith("--")) { //$NON-NLS-1$
            return comment.substring(2);
        }
        String hint = comment.substring(2, comment.length() - 2);
        if (hint.startsWith("+")) { //$NON-NLS-1$
            hint = hint.substring(1);
        }
        return hint;
    }

    /**
     * Get the full comment including the nesting characters -- or \* *\/
     * Or return the empty string if there is no comment.
     * @param includeEnding if true include a space or newline after the comment
     */
    String getFullComment(Token t, boolean includeEnding) {
        if (t == null) {
            return ""; //$NON-NLS-1$
        }
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

        if (includeEnding && image.startsWith("--")) { //$NON-NLS-1$
            return image + "\n"; //$NON-NLS-1$
        }
        if (includeEnding) {
            return image + " "; //$NON-NLS-1$
        }
        return image;
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
        int start = matcher.start();
        if (start > 0 && !Character.isWhitespace(comment.charAt(start -1))) {
            return null;
        }
        int end = matcher.end();
        if (end < comment.length() - 1 && !Character.isWhitespace(comment.charAt(end-1))) {
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

    static FunctionMethod replaceProcedureWithFunction(MetadataFactory factory,
            Procedure proc) throws MetadataException {
        if (proc.isFunction() && proc.getQueryPlan() != null) {
            return null;
        }
        FunctionMethod method = createFunctionMethod(proc);

        //remove the old proc
        factory.getSchema().getResolvingOrder().remove(factory.getSchema().getResolvingOrder().size() - 1);
        factory.getSchema().getProcedures().remove(proc.getName());

        factory.getSchema().addFunction(method);
        return method;
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
            if (pp.getDefaultValue() != null) {
                throw new MetadataException(QueryPlugin.Util.getString("SQLParser.function_default", proc.getName())); //$NON-NLS-1$
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

    void createDDLTrigger(DatabaseStore events, AlterTrigger trigger) {
        GroupSymbol group = trigger.getTarget();
        events.setTableTriggerPlan(trigger.getName(), group.getName(), trigger.getEvent(), trigger.getDefinition().toString(), trigger.isAfter());
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

    public static void setTypeInfo(ParsedDataType type, BaseColumn column) {
        if (type.length != null){
            column.setLength(type.length);
        }
        if (type.precision != null){
            if (type.precision == 0) {
                throw new MetadataException(QueryPlugin.Util.getString("SQLParser.zero_precision")); //$NON-NLS-1$
            }
            column.setPrecision(type.precision);
            if (type.scale != null){
                if (Math.abs(type.scale) > type.precision) {
                    throw new MetadataException(QueryPlugin.Util.getString("SQLParser.invalid_scale", type.scale, type.precision)); //$NON-NLS-1$
                }
                column.setScale(type.scale);
            } else {
                column.setScale(0);
            }
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

    public static class  ParsedDataType{
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

        public String getType() {
            return type;
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

    public static Expression arrayFromQuery(QueryCommand subquery) throws ParseException {
        List<Expression> projected = subquery.getProjectedSymbols();
        if (projected.size() != 1) {
            if (projected.size() > 1) {
                throw new ParseException(QueryPlugin.Util.getString("ERR.015.008.0032", subquery)); //$NON-NLS-1$
            }
            throw new ParseException(QueryPlugin.Util.getString("SQLParser.array_query", subquery)); //$NON-NLS-1$
        }
        Expression ex = projected.get(0);
        String name = Symbol.getName(ex);
        Query query = new Query();
        query.setSelect(new Select(Arrays.asList(
                new AggregateSymbol(AggregateSymbol.Type.ARRAY_AGG.name(),
                        false, new ElementSymbol(name)))));
        query.setFrom(new From(Arrays.asList(
                new SubqueryFromClause(new GroupSymbol("x"), subquery)))); //$NON-NLS-1$
        return new ScalarSubquery(query);
    }
}
