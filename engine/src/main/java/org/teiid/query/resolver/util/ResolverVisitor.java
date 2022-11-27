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

package org.teiid.query.resolver.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.InvalidFunctionException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.UnresolvedSymbolDescription;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.util.StringUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionLibrary.ConversionResult;
import org.teiid.query.metadata.GroupInfo;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.BinaryComparison;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.IsDistinctCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.proc.ExceptionExpression;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ElementSymbol.DisplayMode;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLExists;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;


public class ResolverVisitor extends LanguageVisitor {

    public static final String TEIID_PASS_THROUGH_TYPE = "teiid:pass-through-type"; //$NON-NLS-1$
    private static final String SYS_PREFIX = CoreConstants.SYSTEM_MODEL + '.';

    private Collection<GroupSymbol> groups;
    private GroupContext externalContext;
    protected QueryMetadataInterface metadata;
    private TeiidComponentException componentException;
    private QueryResolverException resolverException;
    private Map<Function, QueryResolverException> unresolvedFunctions;
    private boolean findShortName;
    private List<ElementSymbol> matches = new ArrayList<ElementSymbol>(2);
    private List<GroupSymbol> groupMatches = new ArrayList<GroupSymbol>(2);
    private boolean hasUserDefinedAggregate;

    /**
     * Constructor for ResolveElementsVisitor.
     *
     * External groups are ordered from inner to outer most
     */
    public ResolverVisitor(QueryMetadataInterface metadata, Collection<GroupSymbol> internalGroups, GroupContext externalContext) {
        this.groups = internalGroups;
        this.externalContext = externalContext;
        this.metadata = metadata;
        this.findShortName = metadata.findShortName();
    }

    public void setGroups(Collection<GroupSymbol> groups) {
        this.groups = groups;
    }

    public void visit(ElementSymbol obj) {
        try {
            resolveElementSymbol(obj);
        } catch(QueryMetadataException e) {
            handleException(handleUnresolvedElement(obj, e.getMessage()));
        } catch(TeiidComponentException e) {
            handleException(e);
        } catch (QueryResolverException e) {
            handleException(e);
        }
    }

    private QueryResolverException handleUnresolvedElement(ElementSymbol symbol, String description) {
        UnresolvedSymbolDescription usd = new UnresolvedSymbolDescription(symbol, description);
        QueryResolverException e = new QueryResolverException(usd.getDescription());
        e.setUnresolvedSymbols(Arrays.asList(usd));
        return e;
    }

    private void resolveElementSymbol(ElementSymbol elementSymbol)
        throws TeiidComponentException, QueryResolverException {

        // already resolved
        if(elementSymbol.getMetadataID() != null) {
            return;
        }

        // look up group and element parts of the potentialID
        String groupContext = null;
        if (elementSymbol.getGroupSymbol() != null) {
            groupContext = elementSymbol.getGroupSymbol().getName();
        }
        String elementShortName = elementSymbol.getShortName();
        if (groupContext != null) {
            groupContext = elementSymbol.getGroupSymbol().getName();
            try {
                if (findShortName && internalResolveElementSymbol(elementSymbol, null, elementShortName, groupContext)) {
                    elementSymbol.setDisplayMode(DisplayMode.SHORT_OUTPUT_NAME);
                    return;
                }
            } catch (QueryResolverException e) {
                //ignore
            } catch (QueryMetadataException e) {
                //ignore
            }
        }

        internalResolveElementSymbol(elementSymbol, groupContext, elementShortName, null);
   }

    private boolean internalResolveElementSymbol(ElementSymbol elementSymbol,
            String groupContext, String shortCanonicalName, String expectedGroupContext)
            throws TeiidComponentException, QueryResolverException {
        boolean isExternal = false;
        boolean groupMatched = false;

        GroupContext root = null;

        if (groups != null || externalContext != null) {
            if (groups != null) {
                root = new GroupContext(externalContext, groups);
            }
            if (root == null) {
                isExternal = true;
                root = externalContext;
            }
        } else {
            try {
                LinkedList<GroupSymbol> matchedGroups = new LinkedList<GroupSymbol>();

                if (groupContext != null) {
                    //assume that this is fully qualified
                    Object groupID = this.metadata.getGroupID(groupContext);
                    // No groups specified, so any group is valid
                    GroupSymbol groupSymbol = new GroupSymbol(groupContext);
                    groupSymbol.setMetadataID(groupID);
                    matchedGroups.add(groupSymbol);
                }

                root = new GroupContext(null, matchedGroups);
            } catch(QueryMetadataException e) {
                // ignore
            }
        }

        matches.clear();
        groupMatches.clear();
        while (root != null) {
            Collection<GroupSymbol> matchedGroups = ResolverUtil.findMatchingGroups(groupContext, root.getGroups(), metadata);
            if (matchedGroups != null && !matchedGroups.isEmpty()) {
                groupMatched = true;

                resolveAgainstGroups(shortCanonicalName, matchedGroups);

                if (matches.size() > 1) {
                    throw handleUnresolvedElement(elementSymbol, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31117, elementSymbol, groupMatches));
                }

                if (matches.size() == 1) {
                    break;
                }
            }

            root = root.getParent();
            isExternal = true;
        }

        if (matches.isEmpty()) {
            if (groupMatched) {
                throw handleUnresolvedElement(elementSymbol, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31118, elementSymbol));
            }
            throw handleUnresolvedElement(elementSymbol, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31119, elementSymbol));
        }
        //copy the match information
        ElementSymbol resolvedSymbol = matches.get(0);
        GroupSymbol resolvedGroup = groupMatches.get(0);
        String oldName = elementSymbol.getOutputName();
        if (expectedGroupContext != null && !ResolverUtil.nameMatchesGroup(expectedGroupContext, resolvedGroup.getName())) {
            return false;
        }
        elementSymbol.setIsExternalReference(isExternal);
        elementSymbol.setType(resolvedSymbol.getType());
        elementSymbol.setMetadataID(resolvedSymbol.getMetadataID());
        elementSymbol.setGroupSymbol(resolvedGroup);
        elementSymbol.setShortName(resolvedSymbol.getShortName());
        if (metadata.useOutputName()) {
            elementSymbol.setOutputName(oldName);
        }
        return true;
    }

    private void resolveAgainstGroups(String elementShortName,
                                      Collection<GroupSymbol> matchedGroups) throws QueryMetadataException,
                                                         TeiidComponentException {
        for (GroupSymbol group : matchedGroups) {
            GroupInfo groupInfo = ResolverUtil.getGroupInfo(group, metadata);

            ElementSymbol result = groupInfo.getSymbol(elementShortName);
            if (result != null) {
                matches.add(result);
                groupMatches.add(group);
            }
        }
    }

    public void visit(BetweenCriteria obj) {
        try {
            resolveBetweenCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    public void visit(CompareCriteria obj) {
        try {
            resolveCompareCriteria(obj, obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(IsDistinctCriteria obj) {
        try {
            if (!(obj.getLeftRowValue() instanceof GroupSymbol) && !(obj.getRightRowValue() instanceof GroupSymbol)) {
                resolveCompareCriteria(new BinaryComparison() {

                    @Override
                    public void setRightExpression(Expression ex) {
                        obj.setRightRowValue(ex);
                    }

                    @Override
                    public void setLeftExpression(Expression ex) {
                        obj.setLeftRowValue(ex);
                    }

                    @Override
                    public Expression getRightExpression() {
                        return (Expression) obj.getRightRowValue();
                    }

                    @Override
                    public Expression getLeftExpression() {
                        return (Expression) obj.getLeftRowValue();
                    }

                }, obj);
            }
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(MatchCriteria obj) {
        try {
            resolveMatchCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SetCriteria obj) {
        try {
            resolveSetCriteria(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SubqueryCompareCriteria obj) {
        if (obj.getCommand() != null) {
            try {
                obj.setLeftExpression(ResolverUtil.resolveSubqueryPredicateCriteria(obj.getLeftExpression(), obj, metadata));
            } catch(QueryResolverException e) {
                handleException(e);
            } catch (TeiidComponentException e) {
                handleException(e);
            }
        } else {
            try {
                resolveQuantifiedCompareArray(obj);
            } catch (QueryResolverException e) {
                handleException(e);
            }
        }
    }

    private void resolveQuantifiedCompareArray(SubqueryCompareCriteria obj)
            throws QueryResolverException, AssertionError {
        Class<?> expressionType = obj.getArrayExpression().getType();

        if (expressionType == null || !expressionType.isArray()) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31175, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31175, new Object[] { obj.getArrayExpression(), expressionType }));
        }

        Class<?> rightType = expressionType.getComponentType();

        Expression leftExpression = obj.getLeftExpression();

        setDesiredType(leftExpression, rightType, obj);

        if(leftExpression.getType().equals(rightType) ) {
            return;
        }

        // Try to apply an implicit conversion from one side to the other
        String leftTypeName = DataTypeManager.getDataTypeName(leftExpression.getType());
        String rightTypeName = DataTypeManager.getDataTypeName(rightType);

        if (leftExpression.getType() == DataTypeManager.DefaultDataClasses.NULL) {
            obj.setLeftExpression(ResolverUtil.convertExpression(leftExpression, rightTypeName, metadata) );
            return;
        }

        boolean leftChar = isCharacter(leftExpression, true);
        boolean rightChar = isCharacter(rightType, true);

        // Special cases when left expression is a constant
        if(leftExpression instanceof Constant && (!rightChar || leftChar)) {
            // Auto-convert constant string on left to expected type on right
            try {
                obj.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName, metadata, true));
                return;
            } catch (QueryResolverException qre) {
                if (leftChar && !metadata.widenComparisonToString()) {
                    throw qre;
                }
            }
        }

        // Try to apply a conversion generically
        if ((rightChar ^ leftChar) && !metadata.widenComparisonToString()) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, obj));
        }
        if(ResolverUtil.canImplicitlyConvert(leftTypeName, rightTypeName)) {
            obj.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName, metadata, true) );
            return;
        }
        throw new QueryResolverException(QueryPlugin.Event.TEIID30072, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30072, new Object[] { leftTypeName, rightTypeName, obj }));
    }

    public void visit(SubquerySetCriteria obj) {
        try {
            obj.setExpression(ResolverUtil.resolveSubqueryPredicateCriteria(obj.getExpression(), obj, metadata));
        } catch(QueryResolverException e) {
            handleException(e);
        } catch (TeiidComponentException e) {
            handleException(e);
        }
    }

    public void visit(IsNullCriteria obj) {
        try {
            setDesiredType(obj.getExpression(), DefaultDataClasses.OBJECT, obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(Function obj) {
        try {
            resolveFunction(obj, this.metadata.getFunctionLibrary());
            if (obj.isAggregate()) {
                hasUserDefinedAggregate = true;
            }
        } catch(QueryResolverException e) {
            if (QueryPlugin.Event.TEIID30069.name().equals(e.getCode()) || QueryPlugin.Event.TEIID30067.name().equals(e.getCode())) {
                if (unresolvedFunctions == null) {
                    unresolvedFunctions = new LinkedHashMap<Function, QueryResolverException>();
                }
                unresolvedFunctions.put(obj, e);
            } else {
                handleException(e);
            }
        } catch(TeiidComponentException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(Array array) {
        try {
            if (array.getComponentType() != null) {
                String type = DataTypeManager.getDataTypeName(array.getComponentType());
                for (int i = 0; i < array.getExpressions().size(); i++) {
                    Expression expr = array.getExpressions().get(i);
                    setDesiredType(expr, array.getComponentType(), array);
                    if (array.getComponentType() != DefaultDataClasses.OBJECT) {
                        array.getExpressions().set(i, ResolverUtil.convertExpression(expr, type, metadata));
                    }
                }
            } else {
                resolveComponentType(array);
            }
        } catch (QueryResolverException e) {
            handleException(e);
        }
    }

    public static void resolveComponentType(Array array) {
        Class<?> type = null;
        for (int i = 0; i < array.getExpressions().size(); i++) {
            Expression expr = array.getExpressions().get(i);
            Class<?> baseType = expr.getType();
            while (baseType != null && baseType.isArray()) {
                baseType = baseType.getComponentType();
            }
            if (baseType != DefaultDataClasses.NULL) {
                if (type == null) {
                    type = expr.getType();
                } else if (type != expr.getType()) {
                    type = DataTypeManager.DefaultDataClasses.OBJECT;
                }
            }
        }
        if (type == null) {
            type = DataTypeManager.DefaultDataClasses.NULL;
        }
        array.setComponentType(type);
    }

    public void visit(CaseExpression obj) {
        try {
            resolveCaseExpression(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SearchedCaseExpression obj) {
        try {
            resolveSearchedCaseExpression(obj);
        } catch(QueryResolverException e) {
            handleException(e);
        }
    }

    public void visit(SetClause obj) {
        String type = DataTypeManager.getDataTypeName(obj.getSymbol().getType());
        try {
            setDesiredType(obj.getValue(), obj.getSymbol().getType(), obj);
            obj.setValue(ResolverUtil.convertExpression(obj.getValue(), type, metadata));
        } catch(QueryResolverException e) {
            handleException(new QueryResolverException(e, QueryPlugin.Util.getString("SetClause.resolvingError", new Object[] {obj.getValue(), obj.getSymbol(), type}))); //$NON-NLS-1$
        }
    }

    @Override
    public void visit(XMLSerialize obj) {
        try {
            obj.setExpression(ResolverUtil.convertExpression(obj.getExpression(), DataTypeManager.DefaultDataTypes.XML, metadata));
        } catch (QueryResolverException e) {
            handleException(new QueryResolverException(e, QueryPlugin.Util.getString("XMLSerialize.resolvingError", obj))); //$NON-NLS-1$
        }
    }

    @Override
    public void visit(XMLQuery obj) {
        try {
            ResolverUtil.setDesiredType(obj.getPassing(), obj);
            obj.compileXqueryExpression();
        } catch (QueryResolverException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(XMLExists obj) {
        visit(obj.getXmlQuery());
    }

    @Override
    public void visit(XMLCast xmlCast) {
        String typeName = xmlCast.getTypeName();
        try {
            xmlCast.setType(this.metadata.getDataTypeClass(typeName));
        } catch (QueryMetadataException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(QueryString obj) {
        try {
            obj.setPath(ResolverUtil.convertExpression(obj.getPath(), DataTypeManager.DefaultDataTypes.STRING, metadata));
            for (DerivedColumn col : obj.getArgs()) {
                col.setExpression(ResolverUtil.convertExpression(col.getExpression(), DataTypeManager.DefaultDataTypes.STRING, metadata));
            }
        } catch (QueryResolverException e) {
            handleException(new QueryResolverException(e, QueryPlugin.Util.getString("XMLQuery.resolvingError", obj))); //$NON-NLS-1$
        }
    }

    @Override
    public void visit(ExpressionCriteria obj) {
        try {
            obj.setExpression(ResolverUtil.convertExpression(obj.getExpression(), DataTypeManager.DefaultDataTypes.BOOLEAN, metadata));
        } catch (QueryResolverException e) {
            handleException(e);
        }
    }

    @Override
    public void visit(ExceptionExpression obj) {
        try {
            if (obj.getErrorCode() != null) {
                obj.setErrorCode(ResolverUtil.convertExpression(obj.getErrorCode(), DataTypeManager.DefaultDataTypes.INTEGER, metadata));
            }
            obj.setMessage(ResolverUtil.convertExpression(obj.getMessage(), DataTypeManager.DefaultDataTypes.STRING, metadata));
            if (obj.getSqlState() != null) {
                obj.setSqlState(ResolverUtil.convertExpression(obj.getSqlState(), DataTypeManager.DefaultDataTypes.STRING, metadata));
            }
            checkException(obj.getParent());
        } catch (QueryResolverException e) {
            handleException(e);
        }
    }

    public static void checkException(Expression obj)
            throws QueryResolverException {
        if (obj == null || obj instanceof ExceptionExpression) {
            return;
        }
        if (obj instanceof ElementSymbol) {
            ElementSymbol es = (ElementSymbol)obj;
            if (!(es.getMetadataID() instanceof TempMetadataID)) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31120, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31120, obj));
            }
            TempMetadataID tid = (TempMetadataID)es.getMetadataID();
            if (tid.getType() != Exception.class) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31120, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31120, obj));
            }
        } else if (obj instanceof Constant) {
            Constant c = (Constant)obj;
            if (!(c.getValue() instanceof Exception)) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31120, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31120, obj));
            }
        } else {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31120, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31120, obj));
        }
    }

    @Override
    public void visit(AggregateSymbol obj) {
        if (obj.getCondition() != null) {
            try {
                obj.setCondition(ResolverUtil.convertExpression(obj.getCondition(), DataTypeManager.DefaultDataTypes.BOOLEAN, metadata));
            } catch (QueryResolverException e) {
                handleException(e);
            }
        }
        switch (obj.getAggregateFunction()) {
        case USER_DEFINED:
            visit((Function)obj);
            break;
        case LEAD:
        case LAG:
            if (obj.getArgs().length > 1) {
                //second arg must be an integer
                try {
                    obj.getArgs()[1] = ResolverUtil.convertExpression(obj.getArg(1), DataTypeManager.DefaultDataTypes.INTEGER, metadata);
                } catch (QueryResolverException e) {
                    handleException(e);
                }
                //first and third args must match type
                if (obj.getArgs().length > 2) {
                    try {
                        if (obj.getArgs()[0].getType() == DataTypeManager.DefaultDataClasses.NULL) {
                            obj.getArgs()[0] = ResolverUtil.convertExpression(obj.getArg(0), DataTypeManager.getDataTypeName(obj.getArg(2).getType()), metadata);
                        } else {
                            obj.getArgs()[2] = ResolverUtil.convertExpression(obj.getArg(2), DataTypeManager.getDataTypeName(obj.getArg(0).getType()), metadata);
                        }
                    } catch (QueryResolverException e) {
                        handleException(e);
                    }
                }
            }
            break;
        case NTILE:
            //arg must be an integer
            try {
                if (obj.getArgs().length != 1) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31278, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31278, obj));
                }
                obj.getArgs()[0] = ResolverUtil.convertExpression(obj.getArg(0), DataTypeManager.DefaultDataTypes.INTEGER, metadata);
            } catch (QueryResolverException e) {
                handleException(e);
            }
            break;
        case NTH_VALUE:
            try {
                if (obj.getArgs().length != 2) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31280, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31280, obj));
                }
                obj.getArgs()[1] = ResolverUtil.convertExpression(obj.getArg(1), DataTypeManager.DefaultDataTypes.INTEGER, metadata);
            } catch (QueryResolverException e) {
                handleException(e);
            }
            break;
        case STRING_AGG:
            try {
                if (obj.getArgs().length != 2) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31140, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31140, obj));
                }
                if (obj.getType() == null) {
                    Expression arg = obj.getArg(0);
                    Expression arg1 = obj.getArg(1);
                    Class<?> type = null;
                    if (isBinary(arg)) {
                        setDesiredType(arg1, DataTypeManager.DefaultDataClasses.BLOB, obj);
                        if (isBinary(arg1)) {
                            type = DataTypeManager.DefaultDataClasses.BLOB;
                        }
                    } else if (isCharacter(arg, false)) {
                        setDesiredType(arg1, DataTypeManager.DefaultDataClasses.CLOB, obj);
                        if (isCharacter(arg1, false)) {
                            type = DataTypeManager.DefaultDataClasses.CLOB;
                        }
                    } else if (arg.getType() == null) {
                        if (isBinary(arg1)) {
                            setDesiredType(arg, DataTypeManager.DefaultDataClasses.BLOB, obj);
                            if (isBinary(arg)) {
                                type = DataTypeManager.DefaultDataClasses.BLOB;
                            }
                        } else if (isCharacter(arg1, false)) {
                            setDesiredType(arg, DataTypeManager.DefaultDataClasses.CLOB, obj);
                            if (isCharacter(arg, false)) {
                                type = DataTypeManager.DefaultDataClasses.CLOB;
                            }
                        }
                    }
                    if (type == null) {
                        throw new QueryResolverException(QueryPlugin.Event.TEIID31141, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31141, obj));
                    }
                    obj.setType(type);
                }
            } catch (QueryResolverException e) {
                handleException(e);
            }
            break;
        default:
            if (obj.isRanking()) {
                obj.setType(metadata.isLongRanks()?DefaultDataClasses.LONG:DefaultDataClasses.INTEGER);
            }
        }
    }

    static boolean isCharacter(Expression arg, boolean includeChar) {
        Class<?> type = arg.getType();
        return isCharacter(type, includeChar);
    }

    static boolean isCharacter(Class<?> type, boolean includeChar) {
        return type == DataTypeManager.DefaultDataClasses.STRING
                || type == DataTypeManager.DefaultDataClasses.CLOB
        || (includeChar && type == DataTypeManager.DefaultDataClasses.CHAR);
    }

    private boolean isBinary(Expression arg) {
        return arg.getType() == DataTypeManager.DefaultDataClasses.VARBINARY
                || arg.getType() == DataTypeManager.DefaultDataClasses.BLOB;
    }

    public TeiidComponentException getComponentException() {
        return this.componentException;
    }

    public QueryResolverException getResolverException() {
        return this.resolverException;
    }

    void handleException(TeiidComponentException e) {
        this.componentException = e;

        // Abort the validation process
        setAbort(true);
    }

    void handleException(QueryResolverException e) {
        this.resolverException = e;

        // Abort the validation process
        setAbort(true);
    }

    public void throwException(boolean includeUnresolvedFunctions)
            throws TeiidComponentException, QueryResolverException {
        if(getComponentException() != null) {
            throw getComponentException();
        }

        if(getResolverException() != null) {
            throw getResolverException();
        }

        if (includeUnresolvedFunctions
                && unresolvedFunctions != null && !unresolvedFunctions.isEmpty()) {
            throw unresolvedFunctions.values().iterator().next();
        }
    }

    /**
     * Resolve function such that all functions are resolved and type-safe.
     */
    void resolveFunction(Function function, FunctionLibrary library)
        throws QueryResolverException, TeiidComponentException {

        // Check whether this function is already resolved
        if(function.getFunctionDescriptor() != null) {
            return;
        }

        // Look up types for all args
        boolean hasArgWithoutType = false;
        Expression[] args = function.getArgs();
        Class<?>[] types = new Class[args.length];
        for(int i=0; i<args.length; i++) {
            types[i] = args[i].getType();
            if(types[i] == null) {
                if(!(args[i] instanceof Reference)){
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30067, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30067, new Object[] {args[i], function}));
                }
                hasArgWithoutType = true;
            }
        }

        //special case handling for convert of an untyped reference
        if (FunctionLibrary.isConvert(function) && hasArgWithoutType) {
            Constant constant = (Constant)function.getArg(1);
            Class<?> type = metadata.getDataTypeClass((String)constant.getValue());

            setDesiredType(function.getArg(0), type, function);
            types[0] = type;
            hasArgWithoutType = false;
        }

        // Attempt to get exact match of function for this signature
        List<FunctionDescriptor> fds;
        try {
            fds = findWithImplicitConversions(library, function, args, types, hasArgWithoutType);
            if(fds.isEmpty()) {
                if(!library.hasFunctionMethod(function.getName(), args.length)) {
                    // Unknown function form
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30068, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30068, function));
                }
                // Known function form - but without type information
                if (hasArgWithoutType) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30069, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30069, function));
                }

                // Known function form - unable to find implicit conversions
                throw new QueryResolverException(QueryPlugin.Event.TEIID30070, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30070, function));
            }
            if (fds.size() > 1) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31150, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31150, function));
            }
        } catch (InvalidFunctionException e) {
            // Known function form - but without type information
            if (hasArgWithoutType) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30069, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30069, function));
            }
            throw new QueryResolverException(QueryPlugin.Event.TEIID31150, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31150, function));
        }

        FunctionDescriptor fd = fds.get(0);
        if (fd.getMethod().isVarArgs()
                && fd.getTypes().length == types.length
                && library.isVarArgArrayParam(fd.getMethod(), types, types.length - 1, fd.getTypes()[types.length - 1])) {
            function.setCalledWithVarArgArrayParam(true);
        }

        if(FunctionLibrary.isConvert(function)) {
            String dataType = (String) ((Constant)args[1]).getValue();
            Class<?> dataTypeClass = metadata.getDataTypeClass(dataType);
            fd = library.findTypedConversionFunction(args[0].getType(), dataTypeClass);

            // Verify that the type conversion from src to type is even valid
            Class<?> srcTypeClass = args[0].getType();
            if(srcTypeClass != null && dataTypeClass != null &&
               !srcTypeClass.equals(dataTypeClass) &&
               !DataTypeManager.isTransformable(srcTypeClass, dataTypeClass)) {

                 throw new QueryResolverException(QueryPlugin.Event.TEIID30071, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30071, new Object[] {DataTypeManager.getDataTypeName(srcTypeClass), dataType}));
            }
        } else if(fd.isSystemFunction(FunctionLibrary.LOOKUP)) {
            ResolverUtil.ResolvedLookup lookup = ResolverUtil.resolveLookup(function, metadata);
            fd = library.copyFunctionChangeReturnType(fd, lookup.getReturnElement().getType());
        } else if (fd.isSystemFunction(FunctionLibrary.ARRAY_GET)) {
            if (args[0].getType() != null && args[0].getType().isArray()) {
                fd = library.copyFunctionChangeReturnType(fd, args[0].getType().getComponentType());
            } else {
                if (function.getType() != null) {
                    setDesiredType(args[0], function.getType(), function);
                }
                if (args[0].getType() != DataTypeManager.DefaultDataClasses.OBJECT) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31145, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31145, DataTypeManager.getDataTypeName(args[0].getType()), function));
                }
            }
        } else if (Boolean.valueOf(fd.getMethod().getProperty(TEIID_PASS_THROUGH_TYPE, false))) {
            //hack largely to support pg
            fd = library.copyFunctionChangeReturnType(fd, args[0].getType());
        }

        function.setFunctionDescriptor(fd);
        function.setType(fd.getReturnType());
        if (CoreConstants.SYSTEM_MODEL.equals(fd.getSchema())) {
            if (StringUtil.startsWithIgnoreCase(function.getName(), SYS_PREFIX)) {
                function.setName(function.getName().substring(SYS_PREFIX.length()));
            }
        } else if (library.getSystemFunctions().hasFunctionWithName(function.getName())
                && !StringUtil.startsWithIgnoreCase(function.getName(), function.getFunctionDescriptor().getSchema() + ElementSymbol.SEPARATOR)) {
            function.setName(function.getFunctionDescriptor().getSchema() + ElementSymbol.SEPARATOR + function.getName());
        }
    }

    /**
     * Find possible matches based on implicit conversions of the arguments.
     * NOTE: This method has the side-effect of explicitly inserting conversions into the function arguments,
     * and thereby changing the structure of the function call.
     * @param library
     * @param function
     * @param types
     * @return
     * @throws TeiidComponentException
     * @throws InvalidFunctionException
     * @since 4.3
     */
    private List<FunctionDescriptor> findWithImplicitConversions(FunctionLibrary library, Function function, Expression[] args, Class<?>[] types, boolean hasArgWithoutType) throws QueryResolverException, TeiidComponentException, InvalidFunctionException {
        // Try to find implicit conversion path to still perform this function
        ConversionResult cr = library.determineNecessaryConversions(function.getName(), function.getType(), args, types, hasArgWithoutType);
        if (cr.method == null) {
            return Collections.emptyList();
        }
        Class<?>[] newSignature = types;
        if (cr.needsConverion) {
            FunctionDescriptor[] conversions = library.getConverts(cr.method, types);
            newSignature = new Class[conversions.length];
            // Insert new conversion functions as necessary, while building new signature
            for(int i=0; i<conversions.length; i++) {

                Class<?> newType = types[i];

                if(conversions[i] != null) {
                    newType = conversions[i].getReturnType();

                    setDesiredType(args[i], newType, function);

                    //only currently typed expressions need conversions
                    if (types[i] != null && newType != DataTypeManager.DefaultDataClasses.OBJECT) {
                        //directly resolve constants
                        if (args[i] instanceof Constant && newType == DataTypeManager.DefaultDataClasses.TIMESTAMP) {
                            args[i] = ResolverUtil.getProperlyTypedConstant(((Constant)args[i]).getValue(), newType);
                        } else {
                            function.insertConversion(i, conversions[i]);
                        }
                    }
                }

                newSignature[i] = newType;
            }
        }
        String name = cr.method.getFullName();

        // Now resolve using the new signature to get the function's descriptor
        return library.findAllFunctions(name, newSignature);
    }

    /**
     * Resolves criteria "a BETWEEN b AND c". If type conversions are necessary,
     * this method attempts the following implicit conversions:
     * <br>
     * <ol type="1" start="1">
     *   <li>convert the lower and upper expressions to the criteria expression's type, or</li>
     *   <li>convert the criteria and upper expressions to the lower expression's type, or</li>
     *   <li>convert the criteria and lower expressions to the upper expression's type, or</li>
     *   <li>convert all expressions to a common type to which all three expressions' types can be implicitly converted.</li>
     * </ol>
     * @param criteria
     * @throws QueryResolverException
     * @throws TeiidComponentException
     * @throws TeiidComponentException
     */
    void resolveBetweenCriteria(BetweenCriteria criteria)
        throws QueryResolverException, TeiidComponentException {

        Expression exp = criteria.getExpression();
        Expression lower = criteria.getLowerExpression();
        Expression upper = criteria.getUpperExpression();

        // invariants: none of the expressions is an aggregate symbol
        setDesiredType(exp,
                                       (lower.getType() == null)
                                            ? upper.getType()
                                            : lower.getType(), criteria);
        // invariants: exp.getType() != null
        setDesiredType(lower, exp.getType(), criteria);
        setDesiredType(upper, exp.getType(), criteria);
        // invariants: none of the types is null

        if (exp.getType() == lower.getType() && exp.getType() == upper.getType()) {
            return;
        }

        String expTypeName = DataTypeManager.getDataTypeName(exp.getType());
        String lowerTypeName = DataTypeManager.getDataTypeName(lower.getType());
        String upperTypeName = DataTypeManager.getDataTypeName(upper.getType());

      //check if all types are the same, or if there is a common type
        String[] types = new String[2];
        types[0] = lowerTypeName;
        types[1] = upperTypeName;
        Class<?> type = null;

        String commonType = ResolverUtil.getCommonRuntimeType(types);
        if (commonType != null) {
            type = DataTypeManager.getDataTypeClass(commonType);
        }

        boolean exprChar = isCharacter(exp, true);

        if (exp.getType() != DataTypeManager.DefaultDataClasses.NULL) {
            boolean success = true;
            // try to apply cast
            // Apply cast and replace current value
            if (!exprChar || metadata.widenComparisonToString() || isCharacter(lower, true)) {
                try {
                    criteria.setLowerExpression(ResolverUtil.convertExpression(lower, lowerTypeName, expTypeName, metadata, true) );
                    lower = criteria.getLowerExpression();
                    lowerTypeName = DataTypeManager.getDataTypeName(lower.getType());
                } catch (QueryResolverException e) {
                    if (lower instanceof Constant && isCharacter(lower, true) && !metadata.widenComparisonToString()) {
                        throw e;
                    }
                    if (type == null) {
                        type = lower.getType();
                    }
                    success = false;
                }
            } else {
                success = false;
            }
            // try to apply cast
            // Apply cast and replace current value
            if (!exprChar || metadata.widenComparisonToString() || isCharacter(upper, true)) {
                try {
                    criteria.setUpperExpression(ResolverUtil.convertExpression(upper, upperTypeName, expTypeName, metadata, true) );
                    upper = criteria.getUpperExpression();
                    upperTypeName = DataTypeManager.getDataTypeName(upper.getType());
                } catch (QueryResolverException e) {
                    if (lower instanceof Constant && isCharacter(lower, true) && !metadata.widenComparisonToString()) {
                        throw e;
                    }
                    if (type == null) {
                        type = upper.getType();
                    }
                    success = false;
                }
            } else {
                success = false;
            }
            if (success) {
                return;
            }
        }

        // If no convert found for first element, check whether everything in the
        // set is the same and the convert can be placed on the left side
        if (type == null) {
            // Couldn't find a common type to implicitly convert to
            throw new QueryResolverException(QueryPlugin.Event.TEIID30072, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30077, criteria));
        }

        // Is there a possible conversion from left to right?
        String typeName = DataTypeManager.getDataTypeName(type);

        if (!isCharacter(type, true) || metadata.widenComparisonToString() || exp.getType() == DataTypeManager.DefaultDataClasses.NULL) {
            criteria.setExpression(ResolverUtil.convertExpression(exp, expTypeName, typeName, metadata, true));
        } else if (type != exp.getType()) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, criteria));
        }

        if(lower.getType() != type) {
            if (!metadata.widenComparisonToString() && exprChar ^ isCharacter(lower, true)) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, criteria));
            }
            criteria.setLowerExpression(ResolverUtil.convertExpression(lower, lowerTypeName, typeName, metadata, true));
        }
        if(upper.getType() != type) {
            if (!metadata.widenComparisonToString() && exprChar ^ isCharacter(lower, true)) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, criteria));
            }
            criteria.setUpperExpression(ResolverUtil.convertExpression(upper, upperTypeName, typeName, metadata, true));
        }
        // invariants: exp.getType() == lower.getType() == upper.getType()
    }

    void resolveCompareCriteria(BinaryComparison ccrit, LanguageObject surrounding)
        throws QueryResolverException {

        Expression leftExpression = ccrit.getLeftExpression();
        Expression rightExpression = ccrit.getRightExpression();

        // Check typing between expressions
        setDesiredType(leftExpression, rightExpression.getType(), surrounding);
        setDesiredType(rightExpression, leftExpression.getType(), surrounding);

        if(leftExpression.getType() == rightExpression.getType()) {
            return;
        }

        // Try to apply an implicit conversion from one side to the other
        String leftTypeName = DataTypeManager.getDataTypeName(leftExpression.getType());
        String rightTypeName = DataTypeManager.getDataTypeName(rightExpression.getType());

        if (leftExpression.getType() == DataTypeManager.DefaultDataClasses.NULL) {
            ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName, metadata, true) );
            return;
        }
        if (rightExpression.getType() == DataTypeManager.DefaultDataClasses.NULL) {
            ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, leftTypeName, metadata, true) );
            return;
        }

        boolean leftChar = isCharacter(leftExpression, true);
        boolean rightChar = isCharacter(rightExpression, true);

        // Special cases when right expression is a constant
        if(rightExpression instanceof Constant && (!leftChar || rightChar)) {
            // Auto-convert constant string on right to expected type on left
            try {
                ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, leftTypeName, metadata, true));
                return;
            } catch (QueryResolverException qre) {
                if (rightChar && !metadata.widenComparisonToString()) {
                    throw qre;
                }
            }
        }

        // Special cases when left expression is a constant
        if(leftExpression instanceof Constant && (!rightChar || leftChar)) {
            // Auto-convert constant string on left to expected type on right
            try {
                ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName, metadata, true));
                return;
            } catch (QueryResolverException qre) {
                if (leftChar && !metadata.widenComparisonToString()) {
                    throw qre;
                }
            }
        }

        // Try to apply a conversion generically

        if ((rightChar ^ leftChar) && !metadata.widenComparisonToString()) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, surrounding));
        }

        if(ResolverUtil.canImplicitlyConvert(leftTypeName, rightTypeName)) {
            ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, rightTypeName, metadata, true) );
            return;
        }

        if(ResolverUtil.canImplicitlyConvert(rightTypeName, leftTypeName)) {
            ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, leftTypeName, metadata, true) );
            return;
        }

        String commonType = ResolverUtil.getCommonRuntimeType(new String[] {leftTypeName, rightTypeName});

        if (commonType == null) {
            // Neither are aggs, but types can't be reconciled
             throw new QueryResolverException(QueryPlugin.Event.TEIID30072, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30072, new Object[] { leftTypeName, rightTypeName, surrounding }));
        }
        ccrit.setLeftExpression(ResolverUtil.convertExpression(leftExpression, leftTypeName, commonType, metadata, true) );
        ccrit.setRightExpression(ResolverUtil.convertExpression(rightExpression, rightTypeName, commonType, metadata, true) );
    }

    void resolveMatchCriteria(MatchCriteria mcrit)
        throws QueryResolverException {

        setDesiredType(mcrit.getLeftExpression(), mcrit.getRightExpression().getType(), mcrit);
        mcrit.setLeftExpression(resolveMatchCriteriaExpression(mcrit, mcrit.getLeftExpression()));

        setDesiredType(mcrit.getRightExpression(), mcrit.getLeftExpression().getType(), mcrit);
        mcrit.setRightExpression(resolveMatchCriteriaExpression(mcrit, mcrit.getRightExpression()));
    }

    /**
     * Checks one side of a LIKE Criteria; implicitly converts to a String or CLOB if necessary.
     * @param mcrit the Match Criteria
     * @param expr either left or right expression
     * @return either 'expr' itself, or a new implicit type conversion wrapping expr
     * @throws QueryResolverException if no implicit type conversion is available
     */
    Expression resolveMatchCriteriaExpression(MatchCriteria mcrit, Expression expr)
    throws QueryResolverException {
        // Check left expression == string or CLOB
        String type = DataTypeManager.getDataTypeName(expr.getType());
        Expression result = expr;
        if(type != null) {
            if (!isCharacter(expr, false)) {

                if(ResolverUtil.canImplicitlyConvert(type, DataTypeManager.DefaultDataTypes.STRING)) {

                    result = ResolverUtil.convertExpression(expr, type, DataTypeManager.DefaultDataTypes.STRING, metadata, false);

                } else if (ResolverUtil.canImplicitlyConvert(type, DataTypeManager.DefaultDataTypes.CLOB)){

                    result = ResolverUtil.convertExpression(expr, type, DataTypeManager.DefaultDataTypes.CLOB, metadata, false);

                } else {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30074, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30074, mcrit));
                }
            }
        }
        return result;
    }

    void resolveSetCriteria(SetCriteria scrit)
        throws QueryResolverException {

        // Check that each of the values are the same type as expression
        Class<?> exprType = scrit.getExpression().getType();
        if(exprType == null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30075, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30075, scrit.getExpression()));
        }

        //check if all types are the same, or if there is a common type
        boolean same = true;
        Iterator valIter = scrit.getValues().iterator();
        String[] types = new String[scrit.getValues().size()];
        int i = 0;
        Class<?> type = null;
        while(valIter.hasNext()) {
            Expression value = (Expression) valIter.next();
            if (value.getType() != exprType) {
                same = false;
            }
            types[i++] = DataTypeManager.getDataTypeName(value.getType());
            type = value.getType();
        }
        if (same && type == exprType) {
            return;
        }

        if (!same) {
            String commonType = ResolverUtil.getCommonRuntimeType(types);
            if (commonType != null) {
                type = DataTypeManager.getDataTypeClass(commonType);
            } else {
                type = null;
            }
        }

        String exprTypeName = DataTypeManager.getDataTypeName(exprType);

        boolean attemptConvert = !isCharacter(exprType, true) || metadata.widenComparisonToString();

        List<Expression> newVals = new ArrayList<Expression>(scrit.getValues().size());
        if (scrit.getExpression().getType() != DataTypeManager.DefaultDataClasses.NULL) {
            valIter = scrit.getValues().iterator();
            while(valIter.hasNext()) {
                Expression value = (Expression) valIter.next();
                setDesiredType(value, exprType, scrit);
                if(value.getType() != exprType) {
                    String valTypeName = DataTypeManager.getDataTypeName(value.getType());
                    // try to apply cast
                    // Apply cast and replace current value
                    if (attemptConvert || isCharacter(value.getType(), true)) {
                        try {
                            newVals.add(ResolverUtil.convertExpression(value, valTypeName, exprTypeName, metadata, true) );
                        } catch (QueryResolverException e) {
                            if (value instanceof Constant && isCharacter(value, true) && !metadata.widenComparisonToString()) {
                                throw e;
                            }
                            if (type == null) {
                                type = value.getType();
                            }
                            break;
                        }
                    }
                } else {
                    newVals.add(value);
                }
            }
            if (newVals.size() == scrit.getValues().size()) {
                scrit.setValues(newVals);
                return;
            }
        }

        // If no convert found for first element, check whether everything in the
        // set is the same and the convert can be placed on the left side
        if (type == null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30077, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30077, scrit));
        }

        // Is there a possible conversion from left to right?
        String setTypeName = DataTypeManager.getDataTypeName(type);

        if (type == DefaultDataClasses.CHAR || !isCharacter(type, true) || metadata.widenComparisonToString() || scrit.getExpression().getType() == DataTypeManager.DefaultDataClasses.NULL) {
            scrit.setExpression(ResolverUtil.convertExpression(scrit.getExpression(), exprTypeName, setTypeName, metadata, true));
        } else if (type != scrit.getExpression().getType()) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, scrit));
        }

        boolean exprChar = isCharacter(scrit.getExpression(), true);

        newVals.clear();
        valIter = scrit.getValues().iterator();
        while(valIter.hasNext()) {
            Expression value = (Expression) valIter.next();
            if(value.getType() == null) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30075, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30075, value));
            } else if(value.getType() != type) {
                if (!metadata.widenComparisonToString() && exprChar ^ isCharacter(value, true)) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, scrit));
                }
                value = ResolverUtil.convertExpression(value, setTypeName, metadata);
            }
            newVals.add(value);
        }
        scrit.setValues(newVals);
    }

    void resolveCaseExpression(CaseExpression obj) throws QueryResolverException {
        // If already resolved, do nothing
        if (obj.getType() != null) {
            return;
        }
        final int whenCount = obj.getWhenCount();
        Expression expr = obj.getExpression();

        Class<?> whenType = null;
        Class<?> thenType = null;
        // Get the WHEN and THEN types, and get a candidate type for each (for the next step)
        for (int i = 0; i < whenCount; i++) {
            if (whenType == null) {
                whenType = obj.getWhenExpression(i).getType();
            }
            if (thenType == null) {
                thenType = obj.getThenExpression(i).getType();
            }
        }

        Expression elseExpr = obj.getElseExpression();
        if (elseExpr != null) {
            if (thenType == null) {
                thenType = elseExpr.getType();
            }
        }
        // Invariant: All the expressions contained in the obj are resolved (except References)

        // 2. Attempt to set the target types of all contained expressions,
        //    and collect their type names for the next step
        ArrayList<String> whenTypeNames = new ArrayList<String>(whenCount + 1);
        ArrayList<String> thenTypeNames = new ArrayList<String>(whenCount + 1);
        setDesiredType(expr, whenType, obj);
        // Add the expression's type to the WHEN types
        whenTypeNames.add(DataTypeManager.getDataTypeName(expr.getType()));
        Expression when = null;
        Expression then = null;
        // Set the types of the WHEN and THEN parts
        boolean whenNotChar = false;
        for (int i = 0; i < whenCount; i++) {
            when = obj.getWhenExpression(i);
            then = obj.getThenExpression(i);

            setDesiredType(when, expr.getType(), obj);
            setDesiredType(then, thenType, obj);

            if (!whenTypeNames.contains(DataTypeManager.getDataTypeName(when.getType()))) {
                whenTypeNames.add(DataTypeManager.getDataTypeName(when.getType()));
            }
            if (!isCharacter(when.getType(), true)) {
                whenNotChar = true;
            }
            if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(then.getType()))) {
                thenTypeNames.add(DataTypeManager.getDataTypeName(then.getType()));
            }
        }
        // Set the type of the else expression
        if (elseExpr != null) {
            setDesiredType(elseExpr, thenType, obj);
            if (!thenTypeNames.contains(DataTypeManager.getDataTypeName(elseExpr.getType()))) {
                thenTypeNames.add(DataTypeManager.getDataTypeName(elseExpr.getType()));
            }
        }

        // Invariants: all the expressions' types are non-null

        // 3. Perform implicit type conversions
        String whenTypeName = ResolverUtil.getCommonRuntimeType(whenTypeNames.toArray(new String[whenTypeNames.size()]));
        if (whenTypeName == null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30079, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30079, "WHEN", obj));//$NON-NLS-1$
        }
        if (!metadata.widenComparisonToString() && whenNotChar && isCharacter(DataTypeManager.getDataTypeClass(whenTypeName), true)) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, obj));
        }
        String thenTypeName = ResolverUtil.getCommonRuntimeType(thenTypeNames.toArray(new String[thenTypeNames.size()]));
        if (thenTypeName == null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30079, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30079, "THEN/ELSE", obj));//$NON-NLS-1$
        }
        obj.setExpression(ResolverUtil.convertExpression(obj.getExpression(), whenTypeName, metadata));
        ArrayList<Expression> whens = new ArrayList<Expression>(whenCount);
        ArrayList<Expression> thens = new ArrayList<Expression>(whenCount);
        for (int i = 0; i < whenCount; i++) {
            whens.add(ResolverUtil.convertExpression(obj.getWhenExpression(i), whenTypeName, metadata));
            thens.add(ResolverUtil.convertExpression(obj.getThenExpression(i), thenTypeName, metadata));
        }
        obj.setWhen(whens, thens);
        if (elseExpr != null) {
            obj.setElseExpression(ResolverUtil.convertExpression(elseExpr, thenTypeName, metadata));
        }
        // Set this CASE expression's type to the common THEN type, and we're done.
        obj.setType(DataTypeManager.getDataTypeClass(thenTypeName));
    }

    private void setDesiredType(Expression obj, Class<?> type, LanguageObject surrounding) throws QueryResolverException {
        ResolverUtil.setDesiredType(obj, type, surrounding);
        //second pass resolving for functions
        if (!(obj instanceof Function)) {
            return;
        }
        if (unresolvedFunctions != null) {
            Function f = (Function)obj;
            if (f.getFunctionDescriptor() != null) {
                return;
            }
            unresolvedFunctions.remove(obj);
            obj.acceptVisitor(this);
            QueryResolverException e = unresolvedFunctions.get(obj);
            if (e != null) {
                throw e;
            }
        }
    }

    void resolveSearchedCaseExpression(SearchedCaseExpression obj) throws QueryResolverException {
        // If already resolved, do nothing
        if (obj.getType() != null) {
            return;
        }
        final int whenCount = obj.getWhenCount();
        // 1. Call recursively to resolve any contained CASE expressions

        Class<?> thenType = null;
        // Get the WHEN and THEN types, and get a candidate type for each (for the next step)
        for (int i = 0; i < whenCount; i++) {
            if (thenType == null) {
                thenType = obj.getThenExpression(i).getType();
            }
        }

        Expression elseExpr = obj.getElseExpression();
        if (elseExpr != null) {
            if (thenType == null) {
                thenType = elseExpr.getType();
            }
        }
        // Invariant: All the expressions contained in the obj are resolved (except References)

        // 2. Attempt to set the target types of all contained expressions,
        //    and collect their type names for the next step
        ArrayList<String> thenTypeNames = new ArrayList<String>(whenCount + 1);
        Expression then = null;
        // Set the types of the WHEN and THEN parts
        for (int i = 0; i < whenCount; i++) {
            then = obj.getThenExpression(i);
            setDesiredType(then, thenType, obj);
            thenTypeNames.add(DataTypeManager.getDataTypeName(then.getType()));
        }
        // Set the type of the else expression
        if (elseExpr != null) {
            setDesiredType(elseExpr, thenType, obj);
            thenTypeNames.add(DataTypeManager.getDataTypeName(elseExpr.getType()));
        }

        // Invariants: all the expressions' types are non-null

        // 3. Perform implicit type conversions
        String thenTypeName = ResolverUtil.getCommonRuntimeType(thenTypeNames.toArray(new String[thenTypeNames.size()]));
        if (thenTypeName == null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30079, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30079, "THEN/ELSE", obj)); //$NON-NLS-1$
        }
        ArrayList<Expression> thens = new ArrayList<Expression>(whenCount);
        for (int i = 0; i < whenCount; i++) {
            thens.add(ResolverUtil.convertExpression(obj.getThenExpression(i), thenTypeName, metadata));
        }
        obj.setWhen(obj.getWhen(), thens);
        if (elseExpr != null) {
            obj.setElseExpression(ResolverUtil.convertExpression(elseExpr, thenTypeName, metadata));
        }
        // Set this CASE expression's type to the common THEN type, and we're done.
        obj.setType(DataTypeManager.getDataTypeClass(thenTypeName));
    }

    public static void resolveLanguageObject(LanguageObject obj, QueryMetadataInterface metadata)
    throws TeiidComponentException, QueryResolverException {
        ResolverVisitor.resolveLanguageObject(obj, null, metadata);
    }

    public static void resolveLanguageObject(LanguageObject obj, Collection<GroupSymbol> groups, QueryMetadataInterface metadata)
        throws TeiidComponentException, QueryResolverException {
        ResolverVisitor.resolveLanguageObject(obj, groups, null, metadata);
    }

    public static void resolveLanguageObject(LanguageObject obj, Collection<GroupSymbol> groups, GroupContext externalContext, QueryMetadataInterface metadata)
        throws TeiidComponentException, QueryResolverException {

        if(obj == null) {
            return;
        }

        // Resolve elements, deal with errors
        final ResolverVisitor elementsVisitor = new ResolverVisitor(metadata, groups, externalContext);
        //special handling for is distinct - we must resolve as both element and group
        //until we generalize the notion of a scalar group reference as an "elementsymbol"
        PostOrderNavigator nav = new PostOrderNavigator(elementsVisitor) {
            @Override
            public void visit(IsDistinctCriteria obj) {
                obj.setLeftRowValue(resolveAsGroup(obj.getLeftRowValue()));
                obj.setRightRowValue(resolveAsGroup(obj.getRightRowValue()));
                super.visit(obj);
            }

            private LanguageObject resolveAsGroup(
                    LanguageObject rowValue) {
                if (rowValue instanceof ElementSymbol) {
                    ElementSymbol es = (ElementSymbol)rowValue;
                    if (es.getMetadataID() == null) {
                        try {
                            elementsVisitor.resolveElementSymbol(es);
                        } catch (QueryResolverException
                                | TeiidComponentException e) {
                            GroupSymbol gs = new GroupSymbol(es.getName());
                            try {
                                ResolverUtil.resolveGroup(gs, metadata);
                                rowValue = gs;
                            } catch (QueryResolverException
                                    | TeiidComponentException e1) {
                            }
                        }
                    }
                }
                return rowValue;
            }
        };
        obj.acceptVisitor(nav);
        elementsVisitor.throwException(true);
    }

    public boolean hasUserDefinedAggregate() {
        return hasUserDefinedAggregate;
    }

}
