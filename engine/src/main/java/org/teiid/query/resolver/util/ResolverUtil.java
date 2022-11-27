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

import java.sql.Timestamp;
import java.util.*;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.UnresolvedSymbolDescription;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.StringUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.metadata.BaseColumn;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.GroupInfo;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.rules.RuleChooseJoinStrategy;
import org.teiid.query.optimizer.relational.rules.RuleRaiseAccess;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * Utilities used during resolution
 */
public class ResolverUtil {

    public static class ResolvedLookup {
        private GroupSymbol group;
        private ElementSymbol keyElement;
        private ElementSymbol returnElement;

        void setGroup(GroupSymbol group) {
            this.group = group;
        }
        public GroupSymbol getGroup() {
            return group;
        }
        void setKeyElement(ElementSymbol keyElement) {
            this.keyElement = keyElement;
        }
        public ElementSymbol getKeyElement() {
            return keyElement;
        }
        void setReturnElement(ElementSymbol returnElement) {
            this.returnElement = returnElement;
        }
        public ElementSymbol getReturnElement() {
            return returnElement;
        }
    }

    // Can't construct
    private ResolverUtil() {}

    /*
     *                        Type Conversion Utilities
     */

    /**
     * Gets the most specific type to which all the given types have an implicit
     * conversion. The method decides a common type as follows:
     * <ul>
     *   <li>If one or more of the given types is a candidate, then this method
     *       will return the candidate that occurs first in the given array.
     *       This is why the order of the names in the array is important. </li>
     *   <li>Otherwise, if none of them is a candidate, this method will attempt
     *       to find a common type to which all of them can be implicitly
     *       converted.</li>
     *   <li>Otherwise this method is unable to find a common type to which all
     *       the given types can be implicitly converted, and therefore returns
     *       a null.</li>
     * </ul>
     * @param typeNames an ordered array of unique type names.
     * @return a type name to which all the given types can be converted
     */
    @SuppressWarnings("null")
    public static String getCommonRuntimeType(String[] typeNames) {
        if (typeNames == null || typeNames.length == 0) {
            return null;
        }
        if (typeNames.length == 1) {
            return typeNames[0];
        }
        LinkedHashSet<String> commonConversions = null;
        Set<String> types = new LinkedHashSet<String>();
        Set<String> conversions = null;
        boolean first = true;
        for (int i = 0; i < typeNames.length && (first || !commonConversions.isEmpty()); i++) {
            String string = typeNames[i];
            if (string == null) {
                return null;
            }
            if (DataTypeManager.DefaultDataTypes.NULL.equals(string) || !types.add(string)) {
                continue;
            }
            if (first) {
                commonConversions = new LinkedHashSet<String>();
                // A type can be implicitly convertd to itself, so we put the implicit
                // conversions as well as the original type in the working list of
                // conversions.
                commonConversions.add(string);
                DataTypeManager.getImplicitConversions(string, commonConversions);
                first = false;
            } else {
                if (conversions == null) {
                    conversions = new HashSet<String>();
                }
                DataTypeManager.getImplicitConversions(string, conversions);
                conversions.add(string);
                // Equivalent to set intersection
                commonConversions.retainAll(conversions);
                conversions.clear();
            }

        }
        // If there is only one type, then simply return it
        if (types.size() == 1) {
            return types.iterator().next();
        }
        if (types.isEmpty()) {
            return DataTypeManager.DefaultDataTypes.NULL;
        }
        for (String string : types) {
            if (commonConversions.contains(string)) {
                return string;
            }
        }
        commonConversions.remove(DefaultDataTypes.STRING);
        commonConversions.remove(DefaultDataTypes.OBJECT);
        if (!commonConversions.isEmpty()) {
            return commonConversions.iterator().next();
        }
        return null;
    }

    /**
     * Gets whether there exists an implicit conversion from the source type to
     * the target type
     * @param fromType
     * @param toType
     * @return true if there exists an implicit conversion from the
     * <code>fromType</code> to the <code>toType</code>.
     */
    public static boolean canImplicitlyConvert(String fromType, String toType) {
        if (fromType.equals(toType)) return true;
        return DataTypeManager.isImplicitConversion(fromType, toType);
    }

    /**
     * Replaces a sourceExpression with a conversion of the source expression
     * to the target type. If the source type and target type are the same,
     * this method does nothing.
     * @param sourceExpression
     * @param targetTypeName
     * @return
     * @throws QueryResolverException
     */
    public static Expression convertExpression(Expression sourceExpression, String targetTypeName, QueryMetadataInterface metadata) throws QueryResolverException {
        return convertExpression(sourceExpression,
                                 DataTypeManager.getDataTypeName(sourceExpression.getType()),
                                 targetTypeName, metadata, false);
    }

    /**
     * Replaces a sourceExpression with a conversion of the source expression
     * to the target type. If the source type and target type are the same,
     * this method does nothing.
     * @param sourceExpression
     * @param sourceTypeName
     * @param targetTypeName
     * @param forComparison if the conversion is for a comparison
     * @return
     * @throws QueryResolverException
     */
    public static Expression convertExpression(Expression sourceExpression, String sourceTypeName, String targetTypeName, QueryMetadataInterface metadata, boolean forComparison) throws QueryResolverException {
        if (sourceTypeName.equals(targetTypeName)) {
            return sourceExpression;
        }

        //can't implicit convert from char in comparisons as it looses the notion of fixed comparison
        if(!(forComparison && !metadata.widenComparisonToString() && sourceTypeName.equals(DataTypeManager.DefaultDataTypes.CHAR)) && canImplicitlyConvert(sourceTypeName, targetTypeName)) {
            return getConversion(sourceExpression, sourceTypeName, targetTypeName, true, metadata.getFunctionLibrary());
        }

        if (sourceExpression instanceof Constant) {
            Expression result = convertConstant(sourceTypeName, targetTypeName, (Constant)sourceExpression);
            if (result != null) {
                if (result.getType() == DataTypeManager.DefaultDataClasses.TIMESTAMP || result.getType() == DataTypeManager.DefaultDataClasses.DATE) {
                    //return the expression as it may be a custom conversion and not something that the implicit convert captures
                    return result;
                }
                return getConversion(sourceExpression, sourceTypeName, targetTypeName, true, metadata.getFunctionLibrary());
            }
        }

        //Expression is wrong type and can't convert
         throw new QueryResolverException(QueryPlugin.Event.TEIID30082, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30082, new Object[] {targetTypeName, sourceExpression, sourceTypeName}));
    }

    public static Constant convertConstant(String sourceTypeName,
                                           String targetTypeName,
                                           Constant constant) {
        if (!DataTypeManager.isTransformable(sourceTypeName, targetTypeName)) {
            return null;
        }

        try {
            //try to get the converted constant, if this fails then it is not in a valid format
            Constant result = getProperlyTypedConstant(constant.getValue(), DataTypeManager.getDataTypeClass(targetTypeName));

            if (DataTypeManager.DefaultDataTypes.STRING.equals(sourceTypeName)) {
                if (DataTypeManager.DefaultDataTypes.CHAR.equals(targetTypeName)) {
                    String value = (String)constant.getValue();
                    if (value != null && value.length() != 1 && FunctionMethods.rightTrim(value, ' ').length() > 1) {
                        return null;
                    }
                }
                return result;
            }

            //for non-strings, ensure that the conversion is consistent
            if (!DataTypeManager.isTransformable(targetTypeName, sourceTypeName)) {
                return null;
            }

            //allow implicit conversion of literal values to floating
            if (Number.class.isAssignableFrom(constant.getType()) &&
                    (result.getType() == DataTypeManager.DefaultDataClasses.DOUBLE || result.getType() == DataTypeManager.DefaultDataClasses.FLOAT)) {
                return result;
            }

            if (!(constant.getValue() instanceof Comparable)) {
                return null; //this is the case for xml constants
            }

            Constant reverse = getProperlyTypedConstant(result.getValue(), constant.getType());

            if (((Comparable)constant.getValue()).compareTo(reverse.getValue()) == 0) {
                return result;
            }
        } catch (QueryResolverException e) {

        }

        return null;
    }

    /**
     * IMPORTANT: source and target must be basic runtime types
     * @param sourceExpression
     * @param sourceTypeName
     * @param targetTypeName
     * @param implicit
     * @param library
     * @return
     */
    public static Function getConversion(Expression sourceExpression,
                                            String sourceTypeName,
                                            String targetTypeName,
                                            boolean implicit, FunctionLibrary library) {
        Class<?> srcType = DataTypeManager.getDataTypeClass(sourceTypeName);

        Class<?> targetType = DataTypeManager.getDataTypeClass(targetTypeName);

        try {
            setDesiredType(sourceExpression, targetType, sourceExpression);
        } catch (QueryResolverException e) {
        }

        FunctionDescriptor fd = library.findTypedConversionFunction(srcType, DataTypeManager.getDataTypeClass(targetTypeName));

        Function conversion = new Function(fd.getName(), new Expression[] { sourceExpression, new Constant(targetTypeName) });
        conversion.setType(DataTypeManager.getDataTypeClass(targetTypeName));
        conversion.setFunctionDescriptor(fd);
        if (implicit) {
            conversion.makeImplicit();
        }

        return conversion;
    }

    public static void setDesiredType(List<DerivedColumn> passing, LanguageObject obj) throws QueryResolverException {
        setDesiredType(passing, obj, DataTypeManager.DefaultDataClasses.XML);
    }

    public static void setDesiredType(List<DerivedColumn> passing, LanguageObject obj, Class<?> type) throws QueryResolverException {
        for (DerivedColumn dc : passing) {
            ResolverUtil.setDesiredType(dc.getExpression(), type, obj);
        }
    }

    /**
     * Utility to set the type of an expression if it is a Reference and has a null type.
     * @param expression the expression to test
     * @param targetType the target type, if the expression's type is null.
     * @throws QueryResolverException if unable to set the reference type to the target type.
     */
    public static void setDesiredType(Expression expression, Class<?> targetType, LanguageObject surroundingExpression) throws QueryResolverException {
        if (expression instanceof Reference) {
            Reference ref = (Reference)expression;
            if (ref.isPositional() && ref.getType() == null) {
                if (targetType == null) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30083, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30083, surroundingExpression));
                }
                ref.setType(targetType);
            }
        } else if (expression instanceof Function) {
            Function f = (Function)expression;
            if (f.getType() == null) {
                f.setType(targetType);
            }
        }
    }

    /**
     * Attempt to resolve the order by throws QueryResolverException if the
     * symbol is not of SingleElementSymbol type
     *
     * @param orderBy
     */
    public static void resolveOrderBy(OrderBy orderBy, QueryCommand command, TempMetadataAdapter metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {

        List<Expression> knownElements = command.getProjectedQuery().getSelect().getProjectedSymbols();

        boolean isSimpleQuery = false;
        List<GroupSymbol> fromClauseGroups = Collections.emptyList();
        GroupBy groupBy = null;
        if (command instanceof Query) {
            Query query = (Query)command;
            isSimpleQuery = !query.getSelect().isDistinct() && !query.hasAggregates();
            if (query.getFrom() != null) {
                fromClauseGroups = query.getFrom().getGroups();
            }
            if (!query.getSelect().isDistinct()) {
                groupBy = query.getGroupBy();
            }
        }

        // Cached state, if needed
        String[] knownShortNames = new String[knownElements.size()];
        List<Expression> expressions = new ArrayList<Expression>(knownElements.size());

        for(int i=0; i<knownElements.size(); i++) {
            Expression knownSymbol = knownElements.get(i);
            expressions.add(SymbolMap.getExpression(knownSymbol));
            if (knownSymbol instanceof ElementSymbol || knownSymbol instanceof AliasSymbol) {
                String name = ((Symbol)knownSymbol).getShortName();

                knownShortNames[i] = name;
            }
        }

        for (int i = 0; i < orderBy.getVariableCount(); i++) {
            Expression sortKey = orderBy.getVariable(i);
            if (sortKey instanceof ElementSymbol) {
                ElementSymbol symbol = (ElementSymbol)sortKey;
                String groupPart = null;
                if (symbol.getGroupSymbol() != null) {
                    groupPart = symbol.getGroupSymbol().getName();
                }
                String symbolName = symbol.getName();
                String shortName = symbol.getShortName();
                if (groupPart == null) {
                    int position = -1;
                    Expression matchedSymbol = null;
                    // walk the SELECT col short names, looking for a match on the current ORDER BY 'short name'
                    for(int j=0; j<knownShortNames.length; j++) {
                        if( !shortName.equalsIgnoreCase( knownShortNames[j] )) {
                            continue;
                        }
                        // if we already have a matched symbol, matching again here means it is duplicate/ambiguous
                        if(matchedSymbol != null) {
                            if (!matchedSymbol.equals(knownElements.get(j))) {
                                 throw new QueryResolverException(QueryPlugin.Event.TEIID30084, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30084, symbolName));
                            }
                            continue;
                        }
                        matchedSymbol = knownElements.get(j);
                        position = j;
                    }
                    if (matchedSymbol != null) {
                        TempMetadataID tempMetadataID = new TempMetadataID(symbol.getName(), matchedSymbol.getType());
                        symbol.setMetadataID(tempMetadataID);
                        symbol.setType(matchedSymbol.getType());
                    }
                    if (position != -1) {
                        orderBy.setExpressionPosition(i, position);
                        continue;
                    }
                }
            } else if (sortKey instanceof Constant) {
                // check for legacy positional
                Constant c = (Constant)sortKey;
                int elementOrder = Integer.valueOf(c.getValue().toString()).intValue();
                // adjust for the 1 based index.
                if (elementOrder > knownElements.size() || elementOrder < 1) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30085, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30085, c));
                }
                orderBy.setExpressionPosition(i, elementOrder - 1);
                continue;
            }
            //handle order by expressions
            if (command instanceof SetQuery) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30086, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30086, sortKey));
            }

            //resolve subqueries
            for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(sortKey)) {
                Command c = container.getCommand();

                QueryResolver.setChildMetadata(c, command);
                c.pushNewResolvingContext(fromClauseGroups);

                QueryResolver.resolveCommand(c, metadata.getMetadata(), false);
            }

            for (ElementSymbol symbol : ElementCollectorVisitor.getElements(sortKey, false)) {
                try {
                    ResolverVisitor.resolveLanguageObject(symbol, fromClauseGroups, command.getExternalGroupContexts(), metadata);
                } catch(QueryResolverException e) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30087, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30087, symbol.getName()) );
                }
            }

            ResolverVisitor.resolveLanguageObject(sortKey, metadata);

            int index = expressions.indexOf(SymbolMap.getExpression(sortKey));
            //if unrelated and not a simple query - that is more than just a grouping, throw an exception
            if (index == -1 && !isSimpleQuery && groupBy == null) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID30088, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30088, sortKey));
            }
            orderBy.setExpressionPosition(i, index);
        }
    }

    /**
     * Get the default value for the parameter, which could be null
     * if the parameter is set to NULLABLE.  If no default is available,
     * a QueryResolverException will be thrown.
     *
     * @param symbol ElementSymbol retrieved from metadata, fully-resolved
     * @param metadata QueryMetadataInterface
     * @return expr param (if it is non-null) or default value (if there is one)
     * or null Constant (if parameter is optional and therefore allows this)
     * @throws QueryResolverException if expr is null, parameter is required and no
     * default value is defined
     * @throws QueryMetadataException for error retrieving metadata
     * @throws TeiidComponentException
     * @since 4.3
     */
    public static Expression getDefault(ElementSymbol symbol, QueryMetadataInterface metadata) throws TeiidComponentException, QueryMetadataException, QueryResolverException {
        //Check if there is a default value, if so use it
        Object mid = symbol.getMetadataID();
        Class<?> type = symbol.getType();

        String defaultValue = metadata.getDefaultValue(mid);

        boolean omit = false;
        String extensionProperty = metadata.getExtensionProperty(mid,  BaseColumn.DEFAULT_HANDLING, false);
        if (BaseColumn.EXPRESSION_DEFAULT.equalsIgnoreCase(extensionProperty)) {
            Expression ex = null;
            try {
                ex = QueryParser.getQueryParser().parseExpression(defaultValue);
            } catch (QueryParserException e) {
                //TODO: also validate this at load time
                throw new QueryMetadataException(QueryPlugin.Event.TEIID31170, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31170, symbol));
            }
            List<SubqueryContainer<?>> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ex);
            ResolverVisitor.resolveLanguageObject(ex, metadata);
            for (SubqueryContainer<?> container : subqueries) {
                QueryResolver.resolveCommand(container.getCommand(), metadata);
            }
            return ResolverUtil.convertExpression(ex, DataTypeManager.getDataTypeName(type), metadata);
        } else if (BaseColumn.OMIT_DEFAULT.equalsIgnoreCase(extensionProperty)) {
            Object id = metadata.getGroupIDForElementID(symbol.getMetadataID());
            if (!metadata.isVirtualGroup(id)) {
                omit = true;
                defaultValue = null; //for physical procedures we just need a dummy value
            }
        }

        if (!omit && defaultValue == null && !metadata.elementSupports(mid, SupportConstants.Element.NULL)) {
            throw new QueryResolverException(QueryPlugin.Event.TEIID30089, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30089, symbol.getOutputName()));
        }

        return getProperlyTypedConstant(defaultValue, type);
    }

    public static boolean hasDefault(Object mid, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        Object defaultValue = metadata.getDefaultValue(mid);

        return defaultValue != null || metadata.elementSupports(mid, SupportConstants.Element.NULL);
    }

    /**
     * Construct a Constant with proper type, given the String default
     * value for the parameter and the parameter type.  Throw a
     * QueryResolverException if the String can't be transformed.
     * @param defaultValue either null or a String
     * @param parameterType modeled type of parameter (MetaMatrix runtime type)
     * @return Constant with proper type and default value Object of proper Class.  Will
     * be null Constant if defaultValue is null.
     * @throws QueryResolverException if TransformationException is encountered
     * @since 4.3
     */
    static Constant getProperlyTypedConstant(Object defaultValue,
                                                Class<?> parameterType)
    throws QueryResolverException{
        try {
            Object newValue = DataTypeManager.transformValue(defaultValue, parameterType);
            return new Constant(newValue, parameterType);
        } catch (TransformationException e) {
            //timestamp literals also allow date format
            if (parameterType == DataTypeManager.DefaultDataClasses.TIMESTAMP) {
                try {
                    Object newValue = DataTypeManager.transformValue(defaultValue, DataTypeManager.DefaultDataClasses.DATE);
                    return new Constant(new Timestamp(((Date)newValue).getTime()), parameterType);
                } catch (TransformationException e1) {
                }
            }
            if (parameterType == DataTypeManager.DefaultDataClasses.TIMESTAMP
                    || (parameterType == DataTypeManager.DefaultDataClasses.DATE && defaultValue.toString().endsWith("00:00"))) { //$NON-NLS-1$
                //see if the seconds were omitted
                String val = defaultValue.toString() + ":00"; //$NON-NLS-1$
                try {
                    Object newValue = DataTypeManager.transformValue(val, DataTypeManager.DefaultDataClasses.TIMESTAMP);
                    if (parameterType == DataTypeManager.DefaultDataClasses.DATE) {
                        return new Constant(TimestampWithTimezone.createDate((Timestamp)newValue), DataTypeManager.DefaultDataClasses.DATE);
                    }
                    return new Constant(new Timestamp(((Date)newValue).getTime()), DataTypeManager.DefaultDataClasses.TIMESTAMP);
                } catch (TransformationException e1) {
                }
            }
             throw new QueryResolverException(QueryPlugin.Event.TEIID30090, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30090, defaultValue, defaultValue.getClass(), parameterType));
        }
    }

    /**
     * Returns the resolved elements in the given group.  This method has the side effect of caching the resolved elements on the group object.
     * The resolved elements may not contain non-selectable columns depending on the metadata first used for resolving.
     *
     */
    public static List<ElementSymbol> resolveElementsInGroup(GroupSymbol group, QueryMetadataInterface metadata)
    throws QueryMetadataException, TeiidComponentException {
        return new ArrayList<ElementSymbol>(getGroupInfo(group, metadata).getSymbolList());
    }

    public static void clearGroupInfo(GroupSymbol group, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        metadata.addToMetadataCache(group.getMetadataID(), GroupInfo.CACHE_PREFIX + group.getName(), null);
    }

    static GroupInfo getGroupInfo(GroupSymbol group,
            QueryMetadataInterface metadata)
            throws TeiidComponentException, QueryMetadataException {
        String key = GroupInfo.CACHE_PREFIX + group.getName();
        GroupInfo groupInfo = (GroupInfo)metadata.getFromMetadataCache(group.getMetadataID(), key);

        if (groupInfo == null) {
            group = group.clone();
            // get all elements from the metadata
            List elementIDs = metadata.getElementIDsInGroupID(group.getMetadataID());

            LinkedHashMap<Object, ElementSymbol> symbols = new LinkedHashMap<Object, ElementSymbol>(elementIDs.size());

            for (Object elementID : elementIDs) {
                String elementName = metadata.getName(elementID);
                // Form an element symbol from the ID
                ElementSymbol element = new ElementSymbol(elementName, group);
                element.setMetadataID(elementID);
                element.setType( DataTypeManager.getDataTypeClass(metadata.getElementRuntimeTypeName(element.getMetadataID())) );

                symbols.put(elementID, element);
            }
            groupInfo = new GroupInfo(symbols);
            metadata.addToMetadataCache(group.getMetadataID(), key, groupInfo);
        }
        return groupInfo;
    }

    /**
     * When access patterns are flattened, they are an approximation the user
     * may need to enter as criteria.
     *
     * @param metadata
     * @param groups
     * @param flatten
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    public static List getAccessPatternElementsInGroups(final QueryMetadataInterface metadata, Collection groups, boolean flatten) throws TeiidComponentException, QueryMetadataException {
        List accessPatterns = null;
        Iterator i = groups.iterator();
        while (i.hasNext()){

            GroupSymbol group = (GroupSymbol)i.next();

            //Check this group for access pattern(s).
            Collection accessPatternIDs = metadata.getAccessPatternsInGroup(group.getMetadataID());
            if (accessPatternIDs != null && accessPatternIDs.size() > 0){
                Iterator j = accessPatternIDs.iterator();
                if (accessPatterns == null){
                    accessPatterns = new ArrayList();
                }
                while (j.hasNext()) {
                    List elements = metadata.getElementIDsInAccessPattern(j.next());
                    GroupInfo groupInfo = getGroupInfo(group, metadata);
                    List result = new ArrayList(elements.size());
                    for (Iterator iterator = elements.iterator(); iterator.hasNext();) {
                        Object id = iterator.next();
                        ElementSymbol symbol = groupInfo.getSymbol(id);
                        assert symbol != null;
                        result.add(symbol);
                    }
                    if (flatten) {
                        accessPatterns.addAll(result);
                    } else {
                        accessPatterns.add(new AccessPattern(result));
                    }
                }
            }
        }

        return accessPatterns;
    }

    public static void resolveLimit(Limit limit) throws QueryResolverException {
        if (limit.getOffset() != null) {
            setDesiredType(limit.getOffset(), DataTypeManager.DefaultDataClasses.INTEGER, limit);
        }
        setDesiredType(limit.getRowLimit(), DataTypeManager.DefaultDataClasses.INTEGER, limit);
    }

    public static void resolveImplicitTempGroup(TempMetadataAdapter metadata, GroupSymbol symbol, List symbols)
        throws TeiidComponentException, QueryResolverException {

        if (symbol.isImplicitTempGroupSymbol()) {
            if (metadata.getMetadataStore().getTempElementElementIDs(symbol.getName())==null) {
                addTempGroup(metadata, symbol, symbols, true);
            }
            ResolverUtil.resolveGroup(symbol, metadata);
        }
    }

    public static TempMetadataID addTempGroup(TempMetadataAdapter metadata,
                                    GroupSymbol symbol,
                                    List<? extends Expression> symbols, boolean tempTable) throws QueryResolverException {
        Set<String> names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        for (Expression ses : symbols) {
            if (!names.add(Symbol.getShortName(ses))) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30091, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30091, symbol, Symbol.getShortName(ses)));
            }
        }

        if (tempTable) {
            resolveNullLiterals(symbols);
        }
        TempMetadataStore store = metadata.getMetadataStore();
        return store.addTempGroup(symbol.getName(), symbols, !tempTable, tempTable);
    }

    public static TempMetadataID addTempTable(TempMetadataAdapter metadata,
                                     GroupSymbol symbol,
                                     List<? extends Expression> symbols) throws QueryResolverException {
        return addTempGroup(metadata, symbol, symbols, true);
    }

    /**
     * Look for a null literal in the SELECT clause and set it's type to STRING.  This ensures that
     * the result set metadata retrieved for this query will be properly set to something other than
     * the internal NullType.  Added for defect 15437.
     *
     * @param symbols The select clause symbols
     * @since 4.2
     */
    public static void resolveNullLiterals(List symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            Expression selectSymbol = (Expression) symbols.get(i);

            setTypeIfNull(selectSymbol, DataTypeManager.DefaultDataClasses.STRING);
        }
    }

    public static void setTypeIfNull(Expression symbol,
            Class<?> replacement) {
        if(!DataTypeManager.DefaultDataClasses.NULL.equals(symbol.getType()) && symbol.getType() != null) {
            return;
        }
        symbol = SymbolMap.getExpression(symbol);
        if(symbol instanceof Constant) {
            ((Constant)symbol).setType(replacement);
        } else if (symbol instanceof AbstractCaseExpression) {
            ((AbstractCaseExpression)symbol).setType(replacement);
        } else if (symbol instanceof ScalarSubquery) {
            ((ScalarSubquery)symbol).setType(replacement);
        } else if(symbol instanceof ElementSymbol) {
            ElementSymbol elementSymbol = (ElementSymbol)symbol;
            elementSymbol.setType(replacement);
        } else {
            try {
                ResolverUtil.setDesiredType(symbol, replacement, symbol);
            } catch (QueryResolverException e) {
                //cannot happen
            }
        }
    }

    /**
     *
     * @param groupContext
     * @param groups
     * @param metadata
     * @return the List of groups that match the given groupContext out of the supplied collection
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    public static List<GroupSymbol> findMatchingGroups(String groupContext,
                            Collection<GroupSymbol> groups,
                            QueryMetadataInterface metadata) throws TeiidComponentException,
                                                            QueryMetadataException {

        if (groups == null) {
            return null;
        }

        LinkedList<GroupSymbol> matchedGroups = new LinkedList<GroupSymbol>();

        if (groupContext == null) {
            matchedGroups.addAll(groups);
        } else {
            for (GroupSymbol group : groups) {
                String fullName = group.getName();
                if (nameMatchesGroup(groupContext, matchedGroups, group, fullName)) {
                    if (groupContext.length() == fullName.length()) {
                        return matchedGroups;
                    }
                    continue;
                }

                // don't try to vdb qualify temp metadata
                if (group.getMetadataID() instanceof TempMetadataID) {
                    continue;
                }

                String actualVdbName = metadata.getVirtualDatabaseName();

                if (actualVdbName != null) {
                    fullName = actualVdbName + Symbol.SEPARATOR + fullName;
                    if (nameMatchesGroup(groupContext, matchedGroups, group, fullName)
                        && groupContext.length() == fullName.length()) {
                        return matchedGroups;
                    }
                }
            }
        }

        return matchedGroups;
    }


    public static boolean nameMatchesGroup(String groupContext,
                                            String fullName) {
        //if there is a name match, make sure that it is the full name or a proper qualifier
        if (StringUtil.endsWithIgnoreCase(fullName, groupContext)) {
            int matchIndex = fullName.length() - groupContext.length();
            if (matchIndex == 0 || fullName.charAt(matchIndex - 1) == '.') {
                return true;
            }
        }
        return false;
    }

    private static boolean nameMatchesGroup(String groupContext,
                                            LinkedList<GroupSymbol> matchedGroups,
                                            GroupSymbol group,
                                            String fullName) {
        if (nameMatchesGroup(groupContext, fullName)) {
            matchedGroups.add(group);
            return true;
        }
        return false;
    }

    /**
     * Check the type of the (left) expression and the type of the single
     * projected symbol of the subquery.  If they are not the same, try to find
     * an implicit conversion from the former type to the latter type, and wrap
     * the left expression in that conversion function; otherwise throw an
     * Exception.
     * @param expression the Expression on one side of the predicate criteria
     * @param crit the SubqueryContainer containing the subquery Command of the other
     * side of the predicate criteria
     * @return implicit conversion Function, or null if none is necessary
     * @throws QueryResolverException if a conversion is necessary but none can
     * be found
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    static Expression resolveSubqueryPredicateCriteria(Expression expression, SubqueryContainer crit, QueryMetadataInterface metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {

        List<Expression> projectedSymbols = crit.getCommand().getProjectedSymbols();
        int size = projectedSymbols.size();
        if (size != 1){
            //special handling for array comparison, we don't require the subquery to be wrapped explicitly
            if (expression instanceof Array) {
                Array array = (Array)expression;
                if (array.getExpressions().size() != size) {
                    throw new QueryResolverException(QueryPlugin.Event.TEIID31210, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31210, size, array.getExpressions().size(), crit));
                }
                for (int i = 0; i < size; i++) {
                    array.getExpressions().set(i, resolveComparision(array.getExpressions().get(i), crit, metadata, projectedSymbols.get(i).getType()));
                }
                return array;
            }
            throw new QueryResolverException(QueryPlugin.Event.TEIID30093, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30093, crit.getCommand()));
        }
        Class<?> subqueryType = projectedSymbols.iterator().next().getType();

        return resolveComparision(expression, crit, metadata,
                subqueryType);
    }

    private static Expression resolveComparision(Expression expression,
            SubqueryContainer crit, QueryMetadataInterface metadata,
            Class<?> subqueryType) throws QueryResolverException {
        // Check that type of the expression is same as the type of the
        // single projected symbol of the subquery
        Class<?> exprType = expression.getType();
        if(exprType == null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30075, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30075, expression));
        }
        String exprTypeName = DataTypeManager.getDataTypeName(exprType);

        String subqueryTypeName = DataTypeManager.getDataTypeName(subqueryType);
        Expression result = null;
        try {
            if (!metadata.widenComparisonToString() && ResolverVisitor.isCharacter(subqueryType, true) && !ResolverVisitor.isCharacter(expression, true)) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID31172, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31172, crit));
            }
            result = convertExpression(expression, exprTypeName, subqueryTypeName, metadata, true);
        } catch (QueryResolverException qre) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30094, qre, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30094, crit));
        }
        return result;
    }

    public static ResolvedLookup resolveLookup(Function lookup, QueryMetadataInterface metadata) throws QueryResolverException, TeiidComponentException {
        Expression[] args = lookup.getArgs();
        ResolvedLookup result = new ResolvedLookup();
        // Special code to handle setting return type of the lookup function to match the type of the return element
        if( !(args[0] instanceof Constant) || !(args[1] instanceof Constant) || !(args[2] instanceof Constant)) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30095, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30095));
        }
        // If code table name in lookup function refers to temp group throw exception
        GroupSymbol groupSym = new GroupSymbol((String) ((Constant)args[0]).getValue());
        try {
            groupSym.setMetadataID(metadata.getGroupID((String) ((Constant)args[0]).getValue()));
            if (groupSym.getMetadataID() instanceof TempMetadataID) {
                 throw new QueryResolverException(QueryPlugin.Event.TEIID30096, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30096, ((Constant)args[0]).getValue()));
            }
        } catch(QueryMetadataException e) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30097, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30097, ((Constant)args[0]).getValue()));
        }
        result.setGroup(groupSym);

        List<GroupSymbol> groups = Arrays.asList(groupSym);

        String returnElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[1]).getValue(); //$NON-NLS-1$
        ElementSymbol returnElement = new ElementSymbol(returnElementName);
        try {
            ResolverVisitor.resolveLanguageObject(returnElement, groups, metadata);
        } catch(QueryMetadataException e) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30098, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30098, returnElementName));
        }
        result.setReturnElement(returnElement);

        String keyElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[2]).getValue(); //$NON-NLS-1$
        ElementSymbol keyElement = new ElementSymbol(keyElementName);
        try {
            ResolverVisitor.resolveLanguageObject(keyElement, groups, metadata);
        } catch(QueryMetadataException e) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30099, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30099, keyElementName));
        }
        result.setKeyElement(keyElement);
        args[3] = convertExpression(args[3], DataTypeManager.getDataTypeName(keyElement.getType()), metadata);
        return result;
    }

    private static QueryResolverException handleUnresolvedGroup(GroupSymbol symbol, String description) {
        UnresolvedSymbolDescription usd = new UnresolvedSymbolDescription(symbol.toString(), description);
        QueryResolverException e = new QueryResolverException(usd.getDescription()+": "+usd.getSymbol()); //$NON-NLS-1$
        e.setUnresolvedSymbols(Arrays.asList(usd));
        return e;
    }

    public static void resolveGroup(GroupSymbol symbol, QueryMetadataInterface metadata)
        throws TeiidComponentException, QueryResolverException {

        if (symbol.getMetadataID() != null){
            return;
        }

        // determine the "metadataID" part of the symbol to look up
        String potentialID = symbol.getNonCorrelationName();

        String name = symbol.getName();
        String definition = symbol.getDefinition();

        Object groupID = null;
        try {
            // get valid GroupID for possibleID - this may throw exceptions if group is invalid
            groupID = metadata.getGroupID(potentialID);
        } catch(QueryMetadataException e) {
            // didn't find this group ID
        }

        // If that didn't work, try to strip a vdb name from potentialID
        if(groupID == null) {
            String[] parts = potentialID.split("\\.", 2); //$NON-NLS-1$
            if (parts.length > 1 && parts[0].equalsIgnoreCase(metadata.getVirtualDatabaseName())) {
                try {
                    groupID = metadata.getGroupID(parts[1]);
                } catch(QueryMetadataException e) {
                    // ignore - just didn't find it
                }
                if(groupID != null) {
                    potentialID = parts[1];
                }
            }
        }

        // the group could be partially qualified,  verify that this group exists
        // and there is only one group that matches the given partial name
        if(groupID == null) {
            Collection groupNames = null;
            try {
                groupNames = metadata.getGroupsForPartialName(potentialID);
            } catch(QueryMetadataException e) {
                // ignore - just didn't find it
            }

            if(groupNames != null) {
                int matches = groupNames.size();
                if(matches == 1) {
                    potentialID = (String) groupNames.iterator().next();
                    try {
                        // get valid GroupID for possibleID - this may throw exceptions if group is invalid
                        groupID = metadata.getGroupID(potentialID);
                    } catch(QueryMetadataException e) {
                        // didn't find this group ID
                    }
                } else if(matches > 1) {
                    throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString("ERR.015.008.0055")); //$NON-NLS-1$
                }
            }
        }

        if (groupID == null || metadata.isProcedure(groupID)) {
            //try procedure relational resolving
            try {
                StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(potentialID);
                symbol.setProcedure(true);
                groupID = storedProcedureInfo.getProcedureID();
            } catch(QueryMetadataException e) {
                // just ignore
            }
        }

        if(groupID == null) {
            throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString("ERR.015.008.0056")); //$NON-NLS-1$
        }
        // set real metadata ID in the symbol
        symbol.setMetadataID(groupID);
        potentialID = metadata.getFullName(groupID);
        if(symbol.getDefinition() == null) {
            symbol.setName(potentialID);
        } else {
            symbol.setDefinition(potentialID);
        }
        try {
            if (!symbol.isProcedure()) {
                symbol.setIsTempTable(metadata.isTemporaryTable(groupID));
            }
        } catch(QueryMetadataException e) {
            // should not come here
        }

        if (metadata.useOutputName()) {
            symbol.setOutputDefinition(definition);
            symbol.setOutputName(name);
        }
    }

    public static void findKeyPreserved(Query query, Set<GroupSymbol> keyPreservingGroups, QueryMetadataInterface metadata)
    throws TeiidComponentException, QueryMetadataException {
        if (query.getFrom() == null) {
            return;
        }
        if (query.getFrom().getClauses().size() == 1) {
            findKeyPreserved(query.getFrom().getClauses().get(0), keyPreservingGroups, metadata);
            return;
        }
        //non-ansi join
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>(query.getFrom().getGroups());
        for (GroupSymbol groupSymbol : groups) {
            if (metadata.getUniqueKeysInGroup(groupSymbol.getMetadataID()).isEmpty()) {
                return;
            }
        }
        LinkedList<Expression> leftExpressions = new LinkedList<Expression>();
        LinkedList<Expression> rightExpressions = new LinkedList<Expression>();
        for (Criteria crit : Criteria.separateCriteriaByAnd(query.getCriteria())) {
            if (!(crit instanceof CompareCriteria)) {
                continue;
            }
            CompareCriteria cc = (CompareCriteria)crit;
            if (cc.getOperator() != CompareCriteria.EQ) {
                continue;
            }
            if (cc.getLeftExpression() instanceof ElementSymbol && cc.getRightExpression() instanceof ElementSymbol) {
                ElementSymbol left = (ElementSymbol)cc.getLeftExpression();
                ElementSymbol right = (ElementSymbol)cc.getRightExpression();
                int compare = left.getGroupSymbol().compareTo(right.getGroupSymbol());
                if (compare > 0) {
                    leftExpressions.add(left);
                    rightExpressions.add(right);
                } else if (compare != 0) {
                    leftExpressions.add(right);
                    rightExpressions.add(left);
                }
            }
        }
        HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits = createGroupMap(leftExpressions, rightExpressions);
        HashSet<GroupSymbol> tempSet = new HashSet<GroupSymbol>();
        HashSet<GroupSymbol> nonKeyPreserved = new HashSet<GroupSymbol>();
        for (GroupSymbol group : groups) {
            LinkedHashSet<GroupSymbol> visited = new LinkedHashSet<GroupSymbol>();
            LinkedList<GroupSymbol> toVisit = new LinkedList<GroupSymbol>();
            toVisit.add(group);
            while (!toVisit.isEmpty()) {
                GroupSymbol visiting = toVisit.removeLast();
                if (!visited.add(visiting) || nonKeyPreserved.contains(visiting)) {
                    continue;
                }
                if (keyPreservingGroups.contains(visiting)) {
                    visited.addAll(groups);
                    break;
                }
                toVisit.addAll(findKeyPreserved(tempSet, Collections.singleton(visiting), crits, true, metadata, groups));
                toVisit.addAll(findKeyPreserved(tempSet, Collections.singleton(visiting), crits, false, metadata, groups));
            }
            if (visited.containsAll(groups)) {
                keyPreservingGroups.add(group);
            } else {
                nonKeyPreserved.add(group);
            }
        }
    }

    public static void findKeyPreserved(FromClause clause, Set<GroupSymbol> keyPreservingGroups, QueryMetadataInterface metadata)
    throws TeiidComponentException, QueryMetadataException {
        if (clause instanceof UnaryFromClause) {
            UnaryFromClause ufc = (UnaryFromClause)clause;

            if (!metadata.getUniqueKeysInGroup(ufc.getGroup().getMetadataID()).isEmpty()) {
                keyPreservingGroups.add(ufc.getGroup());
            }
        }
        if (clause instanceof JoinPredicate) {
            JoinPredicate jp = (JoinPredicate)clause;
            if (jp.getJoinType() == JoinType.JOIN_CROSS || jp.getJoinType() == JoinType.JOIN_FULL_OUTER) {
                return;
            }
            HashSet<GroupSymbol> leftPk = new HashSet<GroupSymbol>();
            findKeyPreserved(jp.getLeftClause(), leftPk, metadata);
            HashSet<GroupSymbol> rightPk = new HashSet<GroupSymbol>();
            findKeyPreserved(jp.getRightClause(), rightPk, metadata);

            if (leftPk.isEmpty() && rightPk.isEmpty()) {
                return;
            }

            HashSet<GroupSymbol> leftGroups = new HashSet<GroupSymbol>();
            HashSet<GroupSymbol> rightGroups = new HashSet<GroupSymbol>();
            jp.getLeftClause().collectGroups(leftGroups);
            jp.getRightClause().collectGroups(rightGroups);

            LinkedList<Expression> leftExpressions = new LinkedList<Expression>();
            LinkedList<Expression> rightExpressions = new LinkedList<Expression>();
            RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, leftExpressions, rightExpressions, jp.getJoinCriteria(), new LinkedList<Criteria>());

            HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits = createGroupMap(leftExpressions, rightExpressions);
            if (!leftPk.isEmpty() && (jp.getJoinType() == JoinType.JOIN_INNER || jp.getJoinType() == JoinType.JOIN_LEFT_OUTER)) {
                findKeyPreserved(keyPreservingGroups, leftPk, crits, true, metadata, rightPk);
            }
            if (!rightPk.isEmpty() && (jp.getJoinType() == JoinType.JOIN_INNER || jp.getJoinType() == JoinType.JOIN_RIGHT_OUTER)) {
                findKeyPreserved(keyPreservingGroups, rightPk, crits, false, metadata, leftPk);
            }
        }
    }

    private static HashMap<List<GroupSymbol>, List<HashSet<Object>>> createGroupMap(
            LinkedList<Expression> leftExpressions,
            LinkedList<Expression> rightExpressions) {
        HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits = new HashMap<List<GroupSymbol>, List<HashSet<Object>>>();

        for (int i = 0; i < leftExpressions.size(); i++) {
            Expression lexpr = leftExpressions.get(i);
            Expression rexpr = rightExpressions.get(i);
            if (!(lexpr instanceof ElementSymbol) || !(rexpr instanceof ElementSymbol)) {
                continue;
            }
            ElementSymbol les = (ElementSymbol)lexpr;
            ElementSymbol res = (ElementSymbol)rexpr;
            List<GroupSymbol> tbls = Arrays.asList(les.getGroupSymbol(), res.getGroupSymbol());
            List<HashSet<Object>> ids = crits.get(tbls);
            if (ids == null) {
                ids = Arrays.asList(new HashSet<Object>(), new HashSet<Object>());
                crits.put(tbls, ids);
            }
            ids.get(0).add(les.getMetadataID());
            ids.get(1).add(res.getMetadataID());
        }
        return crits;
    }

    static private HashSet<GroupSymbol> findKeyPreserved(Set<GroupSymbol> keyPreservingGroups,
        Set<GroupSymbol> pk,
        HashMap<List<GroupSymbol>, List<HashSet<Object>>> crits, boolean left, QueryMetadataInterface metadata, Set<GroupSymbol> otherGroups)
        throws TeiidComponentException, QueryMetadataException {
        HashSet<GroupSymbol> result = new HashSet<GroupSymbol>();
        for (GroupSymbol gs : pk) {
            for (Map.Entry<List<GroupSymbol>, List<HashSet<Object>>> entry : crits.entrySet()) {
                if (!entry.getKey().get(left?0:1).equals(gs) || !otherGroups.contains(entry.getKey().get(left?1:0))) {
                    continue;
                }
                if (RuleRaiseAccess.matchesForeignKey(metadata, entry.getValue().get(left?0:1), entry.getValue().get(left?1:0), gs, false, true)) {
                    keyPreservingGroups.add(gs);
                    result.add(entry.getKey().get(left?1:0));
                }
            }
        }
        return result;
    }

    /**
     * This method will convert all elements in a command to their fully qualified name.
     * @param command Command to convert
     */
    public static void fullyQualifyElements(Command command) {
        Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(command, false, true);
        for (ElementSymbol element : elements) {
            element.setDisplayFullyQualified(true);
        }
    }

}
