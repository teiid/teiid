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

package org.teiid.query.eval;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.SourceWarning;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BaseClobType;
import org.teiid.core.types.BaseLob;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.JsonType;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Sequencable;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.core.types.basic.StringToSQLXMLTransform;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.language.Like.MatchMode;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.JSONFunctionMethods.JSONBuilder;
import org.teiid.query.function.source.XMLHelper;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.CollectionValueIterator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.IsDistinctCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.proc.ExceptionExpression;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.DerivedExpression;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.JSONObject;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLExists;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;
import org.teiid.query.sql.symbol.XMLParse;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.ValueIteratorSource;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.util.WSUtil;

public class Evaluator {

    private final class SequenceReader extends Reader {
        private LinkedList<Reader> readers;
        private Reader current = null;

        public SequenceReader(LinkedList<Reader> readers) {
            this.readers = readers;
        }

        @Override
        public void close() throws IOException {
            for (Reader reader : readers) {
                try {
                    reader.close();
                } catch (IOException e) {

                }
            }
        }

        @Override
        public int read(char[] cbuf, int off, int len)
                throws IOException {
            if (current == null && !readers.isEmpty()) {
                current = readers.removeFirst();
            }
            if (current == null) {
                return -1;
            }
            int read = current.read(cbuf, off, len);
            if (read == -1) {
                current.close();
                current = null;
                read = 0;
            }
            if (read < len) {
                int nextRead = read(cbuf, off + read, len - read);
                if (nextRead > 0) {
                    read += nextRead;
                }
            }
            return read;
        }
    }

    public static class NameValuePair<T> {
        public String name;
        public T value;

        public NameValuePair(String name, T value) {
            this.name = name;
            this.value = value;
        }
    }

    public final static char[] REGEX_RESERVED = new char[] {'$', '(', ')', '*', '+', '.', '?', '[', '\\', ']', '^', '{', '|', '}'}; //in sorted order
    private final static MatchCriteria.PatternTranslator LIKE_TO_REGEX = new MatchCriteria.PatternTranslator(new char[] {'%', '_'}, new String[] {".*", "."},  REGEX_RESERVED, '\\', Pattern.DOTALL);  //$NON-NLS-1$ //$NON-NLS-2$

    private final static char[] SIMILAR_REGEX_RESERVED = new char[] {'$', '.', '\\', '^'}; //in sorted order
    public final static MatchCriteria.PatternTranslator SIMILAR_TO_REGEX = new MatchCriteria.PatternTranslator(
            new char[] {'%', '(', ')', '*', '?', '+', '[', ']', '_', '{', '|', '}'},
            new String[] {"([a]|[^a])*", "(", ")", "*", "?", "+", //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$ //$NON-NLS-4$  //$NON-NLS-5$ //$NON-NLS-6$
                    "[", "]", "([a]|[^a])", "{", "|", "}"},  SIMILAR_REGEX_RESERVED, '\\', 0);  //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$ //$NON-NLS-4$  //$NON-NLS-5$ //$NON-NLS-6$

    protected Map elements;

    protected ProcessorDataManager dataMgr;
    protected CommandContext context;

    public static boolean evaluate(Criteria criteria) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        return new Evaluator(Collections.emptyMap(), null, null).evaluate(criteria, Collections.emptyList());
    }

    public static Object evaluate(Expression expression) throws ExpressionEvaluationException, BlockedException, TeiidComponentException  {
        return new Evaluator(Collections.emptyMap(), null, null).evaluate(expression, Collections.emptyList());
    }

    public Evaluator(Map elements, ProcessorDataManager dataMgr, CommandContext context) {
        this.context = context;
        this.dataMgr = dataMgr;
        this.elements = elements;
    }

    public void initialize(CommandContext context, ProcessorDataManager dataMgr) {
        this.context = context;
        this.dataMgr = dataMgr;
    }

    public boolean evaluate(Criteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        return Boolean.TRUE.equals(evaluateTVL(criteria, tuple));
    }

    public Boolean evaluateTVL(Criteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        return internalEvaluateTVL(criteria, tuple);
    }

    private Boolean internalEvaluateTVL(Criteria criteria, List<?> tuple)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        if(criteria instanceof CompoundCriteria) {
            return evaluate((CompoundCriteria)criteria, tuple);
        } else if(criteria instanceof NotCriteria) {
            return evaluate((NotCriteria)criteria, tuple);
        } else if(criteria instanceof CompareCriteria) {
            return evaluate((CompareCriteria)criteria, tuple);
        } else if(criteria instanceof MatchCriteria) {
            return evaluate((MatchCriteria)criteria, tuple);
        } else if(criteria instanceof AbstractSetCriteria) {
            return evaluate((AbstractSetCriteria)criteria, tuple);
        } else if(criteria instanceof IsNullCriteria) {
            return Boolean.valueOf(evaluate((IsNullCriteria)criteria, tuple));
        } else if(criteria instanceof SubqueryCompareCriteria) {
            return evaluate((SubqueryCompareCriteria)criteria, tuple);
        } else if(criteria instanceof ExistsCriteria) {
            return Boolean.valueOf(evaluate((ExistsCriteria)criteria, tuple));
        } else if (criteria instanceof ExpressionCriteria) {
            return (Boolean)evaluate(((ExpressionCriteria)criteria).getExpression(), tuple);
        } else if (criteria instanceof XMLExists) {
            return (Boolean) evaluateXMLQuery(tuple, ((XMLExists)criteria).getXmlQuery(), true);
        } else if (criteria instanceof IsDistinctCriteria) {
            return evaluateIsDistinct((IsDistinctCriteria)criteria, tuple);
        } else {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30311, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30311, criteria));
        }
    }

    private interface RowValue {
        Object get(int index) throws ExpressionEvaluationException, BlockedException, TeiidComponentException;
        int length();
    }

    private Boolean evaluateIsDistinct(IsDistinctCriteria idc, List<?> tuple)
            throws AssertionError, ExpressionEvaluationException, BlockedException, TeiidComponentException {
        RowValue left = getRowValue(tuple, idc.getLeftRowValue());
        RowValue right = getRowValue(tuple, idc.getRightRowValue());
        if (left.length() != right.length()) {
            return !idc.isNegated();
        }
        boolean result = false;
        for (int i = 0; i < left.length(); i++) {
            Object l = left.get(i);
            Object r = right.get(i);
            if (l == null) {
                if (r != null) {
                    result = true;
                    break;
                }
                continue;
            } else if (r == null) {
                result = true;
                break;
            }
            try {
                Boolean b = compare(CompareCriteria.EQ, l, r);
                if (b == null) {
                    continue; //shouldn't happen
                }
                if (!b) {
                    result = true;
                    break;
                }
            } catch (Exception e) {
                //we'll consider this a difference
                //more than likely they are different types
                result = true;
                break;
            }
        }
        if (idc.isNegated()) {
            return !result;
        }
        return result;
    }

    private RowValue getRowValue(final List<?> tuple, final LanguageObject lo) {
        if (lo instanceof GroupSymbol) {
            GroupSymbol leftRowValue = (GroupSymbol)lo;
            TempMetadataID id = (TempMetadataID)leftRowValue.getMetadataID();
            VariableContext vc = this.context.getVariableContext();
            List<TempMetadataID> cols = id.getElements();

            return new RowValue() {

                @Override
                public int length() {
                    return cols.size();
                }

                @Override
                public Object get(int index) {
                    return vc.getValue(new ElementSymbol(cols.get(index).getName(), leftRowValue));
                }
            };
        }
        return new RowValue() {

            @Override
            public int length() {
                return 1;
            }

            @Override
            public Object get(int index) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
                return internalEvaluate((Expression) lo, tuple);
            }
        };
    }

    private Boolean evaluate(CompoundCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        List<Criteria> subCrits = criteria.getCriteria();
        boolean and = criteria.getOperator() == CompoundCriteria.AND;
        Boolean result = and?Boolean.TRUE:Boolean.FALSE;
        for (int i = 0; i < subCrits.size(); i++) {
            Criteria subCrit = subCrits.get(i);
            Boolean value = internalEvaluateTVL(subCrit, tuple);
            if (value == null) {
                result = null;
            } else if (!value.booleanValue()) {
                if (and) {
                    return Boolean.FALSE;
                }
            } else if (!and) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    private Boolean evaluate(NotCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        Criteria subCrit = criteria.getCriteria();
        Boolean result = internalEvaluateTVL(subCrit, tuple);
        if (result == null) {
            return null;
        }
        if (result.booleanValue()) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean evaluate(CompareCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        // Evaluate left expression
        Object leftValue = null;
        try {
            leftValue = evaluate(criteria.getLeftExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "left", criteria)); //$NON-NLS-1$
        }

        // Shortcut if null
        if(leftValue == null) {
            return null;
        }

        // Evaluate right expression
        Object rightValue = null;
        try {
            rightValue = evaluate(criteria.getRightExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "right", criteria)); //$NON-NLS-1$
        }

        // Shortcut if null
        if(rightValue == null) {
            return null;
        }

        // Compare two non-null values using specified operator
        return compare(criteria.getOperator(), leftValue, rightValue);
    }

    private Boolean evaluate(MatchCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        boolean result = false;
        // Evaluate left expression
        Object value = null;
        try {
            value = evaluate(criteria.getLeftExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "left", criteria)); //$NON-NLS-1$
        }

        // Shortcut if null
        if(value == null) {
            return null;
        }

        CharSequence leftValue = null;

        if (value instanceof CharSequence) {
            leftValue = (CharSequence)value;
        } else {
            try {
                leftValue = ((Sequencable)value).getCharSequence();
            } catch (SQLException err) {
                 throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30316, err, err.getMessage());
            }
        }

        // Evaluate right expression
        String rightValue = null;
        try {
            rightValue = (String) evaluate(criteria.getRightExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "right", criteria)); //$NON-NLS-1$
        }

        // Shortcut if null
        if(rightValue == null) {
            return null;
        }

        result = match(rightValue, criteria.getEscapeChar(), leftValue, criteria.getMode());

        return Boolean.valueOf(result ^ criteria.isNegated());
    }

    private boolean match(String pattern, char escape, CharSequence search, MatchMode mode)
        throws ExpressionEvaluationException {

        Pattern patternRegex = null;
        switch (mode) {
        case LIKE:
            patternRegex = LIKE_TO_REGEX.translate(pattern, escape);
            break;
        case SIMILAR:
            patternRegex = SIMILAR_TO_REGEX.translate(pattern, escape);
            break;
        case REGEX:
            patternRegex = MatchCriteria.getPattern(pattern, pattern, 0);
            break;
        default:
            throw new AssertionError();
        }

        Matcher matcher = patternRegex.matcher(search);
        return matcher.find();
    }

    private Boolean evaluate(AbstractSetCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        // Evaluate expression
        Object leftValue = null;
        try {
            leftValue = evaluate(criteria.getExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, criteria));
        }

        Boolean result = Boolean.FALSE;

        ValueIterator valueIter = null;
        if (criteria instanceof SetCriteria) {
            SetCriteria set = (SetCriteria)criteria;
            // Shortcut if null
            if(leftValue == null) {
                if (!set.getValues().isEmpty()) {
                    return null;
                }
                return criteria.isNegated();
            }
            if (set.isAllConstants()) {
                boolean exists = set.getValues().contains(new Constant(leftValue, criteria.getExpression().getType()));
                if (!exists) {
                    if (set.getValues().contains(Constant.NULL_CONSTANT)) {
                        return null;
                    }
                    return criteria.isNegated();
                }
                return !criteria.isNegated();
            }
            valueIter = new CollectionValueIterator(((SetCriteria)criteria).getValues());
        } else if (criteria instanceof DependentSetCriteria){
            DependentSetCriteria ref = (DependentSetCriteria)criteria;
            VariableContext vc = getContext(criteria).getVariableContext();
            ValueIteratorSource vis = (ValueIteratorSource)vc.getGlobalValue(ref.getContextSymbol());
            if(leftValue == null) {
                return null;
            }
            Set<Object> values;
            try {
                values = vis.getCachedSet(ref.getValueExpression());
            } catch (TeiidProcessingException e) {
                 throw new ExpressionEvaluationException(e);
            }
            if (values != null) {
                return values.contains(leftValue);
            }
            vis.setUnused(true);
            //there are too many values to justify a linear search or holding
            //them in memory
            return true;
        } else if (criteria instanceof SubquerySetCriteria) {
            try {
                valueIter = evaluateSubquery((SubquerySetCriteria)criteria, tuple);
            } catch (TeiidProcessingException e) {
                 throw new ExpressionEvaluationException(e);
            }
        } else {
            throw new AssertionError("unknown set criteria type"); //$NON-NLS-1$
        }
        while(valueIter.hasNext()) {
            if(leftValue == null) {
                return null;
            }
            Object possibleValue = valueIter.next();
            Object value = null;
            if(possibleValue instanceof Expression) {
                try {
                    value = evaluate((Expression) possibleValue, tuple);
                } catch(ExpressionEvaluationException e) {
                     throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, possibleValue));
                }
            } else {
                value = possibleValue;
            }

            if(value != null) {
                if(Constant.COMPARATOR.compare(leftValue, value) == 0) {
                    return Boolean.valueOf(!criteria.isNegated());
                } // else try next value
            } else {
                result = null;
            }
        }

        if (result == null) {
            return null;
        }

        return Boolean.valueOf(criteria.isNegated());
    }

    private boolean evaluate(IsNullCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        // Evaluate expression
        Object value = null;
        try {
            value = evaluate(criteria.getExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, criteria));
        }

        return (value == null ^ criteria.isNegated());
    }

    private Boolean evaluate(SubqueryCompareCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        // Evaluate expression
        Object leftValue = null;
        try {
            leftValue = evaluate(criteria.getLeftExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, criteria));
        }

        // Need to be careful to initialize this variable carefully for the case
        // where valueIterator has no values, and the block below is not entered.
        // If there are no rows, and ALL is the predicate quantifier, the result
        // should be true.  If SOME is the predicate quantifier, or no quantifier
        // is used, the result should be false.
        Boolean result = Boolean.FALSE;
        if (criteria.getPredicateQuantifier() == SubqueryCompareCriteria.ALL){
            result = Boolean.TRUE;
        }

        ValueIterator valueIter;
        if (criteria.getCommand() != null) {
            try {
                valueIter = evaluateSubquery(criteria, tuple);
            } catch (TeiidProcessingException e) {
                 throw new ExpressionEvaluationException(e);
            }
        } else {
            Object array = evaluate(criteria.getArrayExpression(), tuple);
            final Object[] vals;
            if (array instanceof Object[]) {
                vals = (Object[])array;
            } else if (array != null){
                ArrayImpl arrayImpl = (ArrayImpl)array;
                vals = arrayImpl.getValues();
            } else {
                return result;
            }
            valueIter = new ValueIterator() {
                int index = 0;

                @Override
                public void reset() {
                    index = 0;
                }

                @Override
                public boolean hasNext() {
                    return index < vals.length;
                }

                @Override
                public Object next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return vals[index++];
                }
            };
        }
        while(valueIter.hasNext()) {
            Object value = valueIter.next();

            // Shortcut if null
            if(leftValue == null) {
                return null;
            }

            if(value != null) {
                Boolean comp = compare(criteria.getOperator(), leftValue, value);

                switch(criteria.getPredicateQuantifier()) {
                    case SubqueryCompareCriteria.ALL:
                        if (Boolean.FALSE.equals(comp)){
                            return Boolean.FALSE;
                        }
                        break;
                    case SubqueryCompareCriteria.SOME:
                        if (Boolean.TRUE.equals(comp)){
                            return Boolean.TRUE;
                        }
                        break;
                    default:
                         throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30326, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30326, criteria.getPredicateQuantifier()));
                }

            } else { // value is null
                switch(criteria.getPredicateQuantifier()) {
                case SubqueryCompareCriteria.ALL:
                    if (Boolean.TRUE.equals(result)){
                        result = null;
                    }
                    break;
                case SubqueryCompareCriteria.SOME:
                    if (Boolean.FALSE.equals(result)){
                        result = null;
                    }
                    break;
                default:
                     throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30326, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30326, criteria.getPredicateQuantifier()));
                }
            }


        } //end value iteration

        return result;
    }

    public static Boolean compare(int operator, Object leftValue,
            Object value) throws AssertionError {
        int compare = 0;
        //TODO: we follow oracle style array comparison
        //semantics.  each element is treated as an individual comparison,
        //so null implies unknown. h2 (and likely other dbms) allow for null
        //array element equality
        if (leftValue instanceof ArrayImpl) {
            ArrayImpl av = (ArrayImpl)leftValue;
            try {
                compare = av.compareTo((ArrayImpl)value, true, Constant.COMPARATOR);
            } catch (ArrayImpl.NullException e) {
                return null;
            }
        } else {
            compare = Constant.COMPARATOR.compare(leftValue, value);
        }
        // Compare two non-null values using specified operator
        Boolean result = null;
        switch(operator) {
            case CompareCriteria.EQ:
                result = Boolean.valueOf(compare == 0);
                break;
            case CompareCriteria.NE:
                result = Boolean.valueOf(compare != 0);
                break;
            case CompareCriteria.LT:
                result = Boolean.valueOf(compare < 0);
                break;
            case CompareCriteria.LE:
                result = Boolean.valueOf(compare <= 0);
                break;
            case CompareCriteria.GT:
                result = Boolean.valueOf(compare > 0);
                break;
            case CompareCriteria.GE:
                result = Boolean.valueOf(compare >= 0);
                break;
            default:
                throw new AssertionError();
        }
        return result;
    }

    private boolean evaluate(ExistsCriteria criteria, List<?> tuple)
        throws BlockedException, TeiidComponentException, ExpressionEvaluationException {

        ValueIterator valueIter;
        try {
            valueIter = evaluateSubquery(criteria, tuple);
        } catch (TeiidProcessingException e) {
             throw new ExpressionEvaluationException(e);
        }
        if(valueIter.hasNext()) {
            return !criteria.isNegated();
        }
        return criteria.isNegated();
    }

    public Object evaluate(Expression expression, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        try {
            return internalEvaluate(expression, tuple);
        } catch (ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30328, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30328, new Object[] {expression, e.getMessage()}));
        }
    }

    protected Object internalEvaluate(Expression expression, List<?> tuple)
       throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

       if(expression instanceof DerivedExpression) {
           if (elements != null) {
               // Try to evaluate by lookup in the elements map (may work for both ElementSymbol and ExpressionSymbol
               Integer index = (Integer) elements.get(expression);
               if(index != null) {
                   return tuple.get(index.intValue());
               }
           }

           // Otherwise this should be an ExpressionSymbol and we just need to dive in and evaluate the expression itself
           if (expression instanceof ExpressionSymbol) {
               ExpressionSymbol exprSyb = (ExpressionSymbol) expression;
               Expression expr = exprSyb.getExpression();
               return internalEvaluate(expr, tuple);
           }

           return getContext(expression).getFromContext(expression);
       }
       if(expression instanceof Constant) {
           Constant c = (Constant) expression;
           if (c.isMultiValued()) {
               throw new AssertionError("Multi-valued constant not allowed to be directly evaluated"); //$NON-NLS-1$
           }
           return c.getValue();
       } else if(expression instanceof Function) {
           return evaluate((Function) expression, tuple);
       } else if(expression instanceof CaseExpression) {
           return evaluate((CaseExpression) expression, tuple);
       } else if(expression instanceof SearchedCaseExpression) {
           return evaluate((SearchedCaseExpression) expression, tuple);
       } else if(expression instanceof Reference) {
           Reference ref = (Reference)expression;
           if (ref.isPositional() && ref.getExpression() == null) {
               return getContext(ref).getVariableContext().getGlobalValue(ref.getContextSymbol());
           }
           Object result = getContext(ref.getExpression()).getFromContext(ref.getExpression());
           if (ref.getConstraint() != null) {
               try {
                   ref.getConstraint().validate(result);
               } catch (QueryValidatorException e) {
                   throw new ExpressionEvaluationException(e);
               }
           }
           return result;
       } else if(expression instanceof Criteria) {
           return evaluate((Criteria) expression, tuple);
       } else if(expression instanceof ScalarSubquery) {
           return evaluate((ScalarSubquery) expression, tuple);
       } else if (expression instanceof Criteria) {
           return evaluate((Criteria)expression, tuple);
       } else if (expression instanceof TextLine){
           return evaluateTextLine(tuple, (TextLine)expression);
       } else if (expression instanceof XMLElement){
           return evaluateXMLElement(tuple, (XMLElement)expression);
       } else if (expression instanceof XMLForest){
           return evaluateXMLForest(tuple, (XMLForest)expression);
       } else if (expression instanceof JSONObject){
           return evaluateJSONObject(tuple, (JSONObject)expression, null);
       } else if (expression instanceof XMLSerialize){
           return evaluateXMLSerialize(tuple, (XMLSerialize)expression);
       } else if (expression instanceof XMLQuery) {
           return evaluateXMLQuery(tuple, (XMLQuery)expression, false);
       } else if (expression instanceof QueryString) {
           return evaluateQueryString(tuple, (QueryString)expression);
       } else if (expression instanceof XMLParse){
           return evaluateXMLParse(tuple, (XMLParse)expression);
       } else if (expression instanceof Array) {
           Array array = (Array)expression;
           List<Expression> exprs = array.getExpressions();
           Object[] result = (Object[]) java.lang.reflect.Array.newInstance(array.getComponentType(), exprs.size());
           for (int i = 0; i < exprs.size(); i++) {
               Object eval = internalEvaluate(exprs.get(i), tuple);
               if (eval instanceof ArrayImpl) {
                   eval = ((ArrayImpl)eval).getValues();
               }
               result[i] = eval;
           }
           return new ArrayImpl(result);
       } else if (expression instanceof ExceptionExpression) {
           return evaluate(tuple, (ExceptionExpression)expression);
       } else if (expression instanceof XMLCast) {
           return evaluate(tuple, (XMLCast)expression);
       } else {
            throw new TeiidComponentException(QueryPlugin.Event.TEIID30329, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30329, expression.getClass().getName()));
       }
    }

    private Object evaluate(List<?> tuple, XMLCast expression) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Object val = internalEvaluate(expression.getExpression(), tuple);
        if (val == null) {
            return new Constant(null, expression.getType());
        }
        return XMLHelper.getInstance().evaluate((XMLType)val, expression, context);
    }

    private Object evaluate(List<?> tuple, ExceptionExpression ee)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        String msg = (String) internalEvaluate(ee.getMessage(), tuple);
        String sqlState = ee.getDefaultSQLState();
        if (ee.getSqlState() != null) {
            sqlState = (String) internalEvaluate(ee.getSqlState(), tuple);
        }
        Integer errorCode = null;
        if (ee.getErrorCode() != null) {
            errorCode = (Integer) internalEvaluate(ee.getErrorCode(), tuple);
        }
        Exception parent = null;
        if (ee.getParent() != null) {
            parent = (Exception) internalEvaluate(ee.getParent(), tuple);
        }
        Exception result = new TeiidSQLException(parent, msg, sqlState, errorCode!=null?errorCode:0);
        result.setStackTrace(SourceWarning.EMPTY_STACK_TRACE);
        return result;
    }

    private Object evaluateXMLParse(List<?> tuple, final XMLParse xp) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Object value = internalEvaluate(xp.getExpression(), tuple);
        if (value == null) {
            return null;
        }
        XMLType.Type type = Type.DOCUMENT;
        SQLXMLImpl result = null;
        try {
            if (value instanceof String) {
                String string = (String)value;
                result = new SQLXMLImpl(string);
                result.setCharset(Streamable.CHARSET);
                if (!xp.isWellFormed()) {
                    Reader r = new StringReader(string);
                    type = validate(xp, r);
                }
            } else if (value instanceof BinaryType) {
                BinaryType string = (BinaryType)value;
                result = new SQLXMLImpl(string.getBytesDirect());
                result.setCharset(Streamable.CHARSET);
                if (!xp.isWellFormed()) {
                    Reader r = result.getCharacterStream();
                    type = validate(xp, r);
                }
            } else {
                InputStreamFactory isf = null;
                Streamable<?> s = (Streamable<?>)value;
                isf = getInputStreamFactory(s);
                result = new SQLXMLImpl(isf);
                if (!xp.isWellFormed()) {
                    Reader r = result.getCharacterStream();
                    type = validate(xp, r);
                }
            }
        } catch (TransformationException e) {
             throw new ExpressionEvaluationException(e);
        } catch (SQLException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30331, e, e.getMessage());
        }
        if (!xp.isDocument()) {
            type = Type.CONTENT;
        }
        XMLType xml = new XMLType(result);
        xml.setType(type);
        return xml;
    }

    public static InputStreamFactory getInputStreamFactory(Streamable<?> s) {
        if (s.getReference() instanceof Streamable<?>) {
            return getInputStreamFactory((Streamable<?>) s.getReference());
        }
        if (s.getReference() instanceof BaseLob) {
            BaseLob bl = (BaseLob) s.getReference();
            try {
                InputStreamFactory isf = bl.getStreamFactory();
                if (isf != null) {
                    return isf;
                }
            } catch (SQLException e) {
            }
        }
        if (s instanceof BaseClobType) {
            return new InputStreamFactory.ClobInputStreamFactory((Clob)s.getReference());
        } else if (s instanceof BlobType){
            return new InputStreamFactory.BlobInputStreamFactory((Blob)s.getReference());
        }
        return new InputStreamFactory.SQLXMLInputStreamFactory((SQLXML)s.getReference());
    }

    private Type validate(final XMLParse xp, Reader r)
            throws TransformationException {
        if (!xp.isDocument()) {
            LinkedList<Reader> readers = new LinkedList<Reader>();
            readers.add(new StringReader("<r>")); //$NON-NLS-1$
            readers.add(r);
            readers.add(new StringReader("</r>")); //$NON-NLS-1$
            r = new SequenceReader(readers);
        }
        return StringToSQLXMLTransform.isXml(r);
    }

    //TODO: exception if length is too long?
    private Object evaluateQueryString(List<?> tuple, QueryString queryString)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        Evaluator.NameValuePair<Object>[] pairs = getNameValuePairs(tuple, queryString.getArgs(), false, true);
        String path = (String)internalEvaluate(queryString.getPath(), tuple);
        if (path == null) {
            path = ""; //$NON-NLS-1$
        }
        boolean appendedAny = false;
        StringBuilder result = new StringBuilder();
        for (Evaluator.NameValuePair<Object> nameValuePair : pairs) {
            if (nameValuePair.value == null) {
                continue;
            }
            if (appendedAny) {
                result.append('&');
            }
            appendedAny = true;
            result.append(WSUtil.httpURLEncode(nameValuePair.name)).append('=').append(WSUtil.httpURLEncode((String)nameValuePair.value));
        }
        if (!appendedAny) {
            return path;
        }
        result.insert(0, '?');
        result.insert(0, path);
        return result.toString();
    }

    /**
     *
     * @param tuple
     * @param xmlQuery
     * @param exists - check only for the existence of a non-empty result
     * @return Boolean if exists is true, otherwise an XMLType value
     * @throws BlockedException
     * @throws TeiidComponentException
     * @throws FunctionExecutionException
     */
    private Object evaluateXMLQuery(List<?> tuple, XMLQuery xmlQuery, boolean exists)
            throws BlockedException, TeiidComponentException,
            FunctionExecutionException {
        Map<String, Object> parameters = new HashMap<String, Object>();
        try {
            evaluateParameters(xmlQuery.getPassing(), tuple, parameters);
        } catch (ExpressionEvaluationException e) {
            throw new FunctionExecutionException(QueryPlugin.Event.TEIID30333, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30333, e.getMessage()));
        }
        return XMLHelper.getInstance().evaluateXMLQuery(tuple, xmlQuery, exists, parameters, context);
    }

    private Object evaluateXMLSerialize(List<?> tuple, XMLSerialize xs)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException, FunctionExecutionException {
        XMLType value = (XMLType) internalEvaluate(xs.getExpression(), tuple);
        if (value == null) {
            return null;
        }
        try {
            if (!xs.isDocument()) {
                return XMLSystemFunctions.serialize(xs, value);
            }
            if (value.getType() == Type.UNKNOWN) {
                Type type = StringToSQLXMLTransform.isXml(value.getCharacterStream());
                value.setType(type);
            }
            if (value.getType() == Type.DOCUMENT || value.getType() == Type.ELEMENT) {
                return XMLSystemFunctions.serialize(xs, value);
            }
        } catch (SQLException e) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30334, e);
        } catch (TransformationException e) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30335, e);
        }
         throw new FunctionExecutionException(QueryPlugin.Event.TEIID30336, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30336));
    }

    private static TextLine.ValueExtractor<NameValuePair<Object>> defaultExtractor = new TextLine.ValueExtractor<NameValuePair<Object>>() {
        public Object getValue(NameValuePair<Object> t) {
            return t.value;
        }
    };

    private Object evaluateTextLine(List<?> tuple, TextLine function) throws ExpressionEvaluationException, BlockedException, TeiidComponentException, FunctionExecutionException {
        List<DerivedColumn> args = function.getExpressions();
        Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, false, true);

        try {
            return new ArrayImpl(TextLine.evaluate(Arrays.asList(nameValuePairs), defaultExtractor, function));
        } catch (TransformationException e) {
             throw new ExpressionEvaluationException(e);
        } catch (TeiidProcessingException e) {
            throw new ExpressionEvaluationException(e);
        }
    }

    private Object evaluateXMLForest(List<?> tuple, XMLForest function)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException, FunctionExecutionException {
        List<DerivedColumn> args = function.getArgs();
        Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, true, true);

        try {
            return XMLSystemFunctions.xmlForest(context, namespaces(function.getNamespaces()), nameValuePairs);
        } catch (TeiidProcessingException e) {
             throw new FunctionExecutionException(e);
        }
    }

    private JsonType evaluateJSONObject(List<?> tuple, JSONObject function, JSONBuilder builder)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException, FunctionExecutionException {
        List<DerivedColumn> args = function.getArgs();
        Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, false, false);
        boolean returnValue = false;
        try {
            if (builder == null) {
                returnValue = true;
                //preevaluate subqueries to prevent blocked exceptions
                for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(function)) {
                    evaluateSubquery(container, tuple);
                }
                builder = new JSONBuilder(context.getBufferManager());
            }
            builder.start(false);
            for (NameValuePair<Object> nameValuePair : nameValuePairs) {
                addValue(tuple, builder, nameValuePair.name, nameValuePair.value);
            }
            builder.end(false);
            if (returnValue) {
                JsonType result = builder.close(context);
                builder = null;
                return result;
            }
            return null;
        } catch (TeiidProcessingException e) {
            throw new FunctionExecutionException(e);
        } finally {
            if (returnValue && builder != null) {
                builder.remove();
            }
        }
    }

    private void addValue(List<?> tuple, JSONBuilder builder,
            String name, Object value)
            throws TeiidProcessingException, ExpressionEvaluationException,
            BlockedException, TeiidComponentException,
            FunctionExecutionException {
        try {
            if (value instanceof JSONObject) {
                builder.startValue(name);
                evaluateJSONObject(tuple, (JSONObject)value, builder);
                return;
            }
            if (value instanceof Function) {
                Function f = (Function)value;
                if (f.getName().equalsIgnoreCase(FunctionLibrary.JSONARRAY)) {
                    builder.startValue(name);
                    jsonArray(context, f, f.getArgs(), builder, this, tuple);
                    return;
                }
            }
            builder.addValue(name, internalEvaluate((Expression)value, tuple));
        } catch (BlockedException e) {
            throw e;
        }
    }

    public static JsonType jsonArray(CommandContext context, Function f, Object[] vals, JSONBuilder builder, Evaluator eval, List<?> tuple) throws TeiidProcessingException, BlockedException, TeiidComponentException {
        boolean returnValue = false;
        try {
            if (builder == null) {
                returnValue = true;
                if (eval != null) {
                    //preevaluate subqueries to prevent blocked exceptions
                    for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(f)) {
                        eval.evaluateSubquery(container, tuple);
                    }
                }
                builder = new JSONBuilder(context.getBufferManager());
            }
            builder.start(true);
            for (Object object : vals) {
                if (eval != null) {
                    eval.addValue(tuple, builder, null, object);
                } else {
                    builder.addValue(object);
                }
            }
            builder.end(true);
            if (returnValue) {
                JsonType result = builder.close(context);
                builder = null;
                return result;
            }
            return null;
        } finally {
            if (returnValue && builder != null) {
                builder.remove();
            }
        }
    }

    private Object evaluateXMLElement(List<?> tuple, XMLElement function)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException, FunctionExecutionException {
        List<Expression> content = function.getContent();
        List<Object> values = new ArrayList<Object>(content.size());
        for (Expression exp : content) {
            values.add(internalEvaluate(exp, tuple));
        }
        try {
            Evaluator.NameValuePair<Object>[] attributes = null;
            if (function.getAttributes() != null) {
                attributes = getNameValuePairs(tuple, function.getAttributes().getArgs(), true, true);
            }
            return XMLSystemFunctions.xmlElement(context, function.getName(), namespaces(function.getNamespaces()), attributes, values);
        } catch (TeiidProcessingException e) {
            throw new FunctionExecutionException(e);
        }
    }

    /**
     * Evaluate the parameters and return the context item if it exists
     */
    public void evaluateParameters(List<DerivedColumn> cols, List<?> tuple, Map<String, Object> parameters)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        for (DerivedColumn passing : cols) {
            Object value = evaluateParameter(tuple, passing);
            if (passing.getAlias() == null) {
                parameters.put(null, value);
            } else {
                parameters.put(passing.getAlias(), value);
            }
        }
    }

    private Object evaluateParameter(List<?> tuple, DerivedColumn passing)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        if (passing.getExpression() instanceof Function) {
            Function f = (Function)passing.getExpression();
            //narrow optimization of json based documents to allow for lower overhead streaming
            if (f.getName().equalsIgnoreCase(SourceSystemFunctions.JSONTOXML)) {
                String rootName = (String)this.evaluate(f.getArg(0), tuple);
                Object lob = this.evaluate(f.getArg(1), tuple);
                if (rootName == null || lob == null) {
                    return null;
                }
                try {
                    if (lob instanceof Blob) {
                        return XMLSystemFunctions.jsonToXml(context, rootName, (Blob)lob, true);
                    }
                    return XMLSystemFunctions.jsonToXml(context, rootName, (Clob)lob, true);
                } catch (IOException e) {
                    throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, f.getFunctionDescriptor().getName()));
                } catch (SQLException e) {
                    throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, f.getFunctionDescriptor().getName()));
                } catch (TeiidProcessingException e) {
                    throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, f.getFunctionDescriptor().getName()));
                }
            }
        } else if (passing.getExpression() instanceof XMLParse) {
            XMLParse xmlParse = (XMLParse)passing.getExpression();
            xmlParse.setWellFormed(true);
        }
        Object value = this.evaluate(passing.getExpression(), tuple);
        return value;
    }

    private Evaluator.NameValuePair<Object>[] getNameValuePairs(List<?> tuple, List<DerivedColumn> args, boolean xmlNames, boolean eval)
            throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Evaluator.NameValuePair<Object>[] nameValuePairs = new Evaluator.NameValuePair[args.size()];
        for (int i = 0; i < args.size(); i++) {
            DerivedColumn symbol = args.get(i);
            String name = symbol.getAlias();
            Expression ex = symbol.getExpression();
            if (name == null && ex instanceof ElementSymbol) {
                name = ((ElementSymbol)ex).getShortName();
            }
            if (name != null) {
                if (xmlNames) {
                    name = XMLHelper.getInstance().escapeName(name, true);
                }
            } else if (!xmlNames) {
                name = "expr" + (i+1); //$NON-NLS-1$
            }
            nameValuePairs[i] = new Evaluator.NameValuePair<Object>(name, eval?internalEvaluate(ex, tuple):ex);
        }
        return nameValuePairs;
    }

    private Evaluator.NameValuePair<String>[] namespaces(XMLNamespaces namespaces) {
        if (namespaces == null) {
            return null;
        }
        List<NamespaceItem> args = namespaces.getNamespaceItems();
        Evaluator.NameValuePair<String>[] nameValuePairs = new Evaluator.NameValuePair[args.size()];
        for(int i=0; i < args.size(); i++) {
            NamespaceItem item = args.get(i);
            nameValuePairs[i] = new Evaluator.NameValuePair<String>(item.getPrefix(), item.getUri());
        }
        return nameValuePairs;
    }

    private Object evaluate(CaseExpression expr, List<?> tuple)
    throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        Object exprVal = internalEvaluate(expr.getExpression(), tuple);
        for (int i = 0; i < expr.getWhenCount(); i++) {
            if (EquivalenceUtil.areEqual(exprVal, internalEvaluate(expr.getWhenExpression(i), tuple))) {
                return internalEvaluate(expr.getThenExpression(i), tuple);
            }
        }
        if (expr.getElseExpression() != null) {
            return internalEvaluate(expr.getElseExpression(), tuple);
        }
        return null;
    }

    private Object evaluate(SearchedCaseExpression expr, List<?> tuple)
    throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        for (int i = 0; i < expr.getWhenCount(); i++) {
            if (evaluate(expr.getWhenCriteria(i), tuple)) {
                return internalEvaluate(expr.getThenExpression(i), tuple);
            }
        }
        if (expr.getElseExpression() != null) {
            return internalEvaluate(expr.getElseExpression(), tuple);
        }
        return null;
    }

    private Object evaluate(Function function, List<?> tuple)
        throws BlockedException, TeiidComponentException, ExpressionEvaluationException {

        // Get function based on resolved function info
        FunctionDescriptor fd = function.getFunctionDescriptor();

        // Evaluate args
        Expression[] args = function.getArgs();
        Object[] values = null;
        int start = 0;

        if (fd.requiresContext()) {
            values = new Object[args.length+1];
            values[0] = context;
            start = 1;
        }
        else {
            values = new Object[args.length];
        }

        for(int i=0; i < args.length; i++) {
            values[i+start] = internalEvaluate(args[i], tuple);
            if (values[i+start] instanceof Constant) {
                //leaked a multivalued constant
                throw new AssertionError("Multi-valued constant not allowed to be directly evaluated"); //$NON-NLS-1$
            }
        }

        if (fd.getPushdown() == PushDown.MUST_PUSHDOWN) {
            try {
                return evaluatePushdown(function, tuple, values);
            } catch (TeiidProcessingException e) {
                throw new ExpressionEvaluationException(e);
            }
        }

        if (fd.getProcedure() != null) {
            try {
                return evaluateProcedure(function, tuple, values);
            } catch (TeiidProcessingException e) {
                throw new ExpressionEvaluationException(e);
            }
        }

        // Check for special lookup function
        if(function.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)) {
            if(dataMgr == null) {
                 throw new ComponentNotFoundException(QueryPlugin.Event.TEIID30342, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30342));
            }

            String codeTableName = (String) values[0];
            String returnElementName = (String) values[1];
            String keyElementName = (String) values[2];

            try {
                return dataMgr.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, values[3]);
            } catch (TeiidProcessingException e) {
                throw new ExpressionEvaluationException(e);
            }
        }

        // Execute function
        return fd.invokeFunction(values, context, null, function.isCalledWithVarArgArrayParam());
    }

    protected Object evaluatePushdown(Function function, List<?> tuple,
            Object[] values) throws FunctionExecutionException, TeiidComponentException, TeiidProcessingException {
        throw new FunctionExecutionException(QueryPlugin.Event.TEIID30341, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30341, function.getFunctionDescriptor().getFullName()));
    }

    private Object evaluate(ScalarSubquery scalarSubquery, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        Object result = null;
        ValueIterator valueIter;
        try {
            valueIter = evaluateSubquery(scalarSubquery, tuple);
        } catch (TeiidProcessingException e) {
             throw new ExpressionEvaluationException(e);
        }
        if(valueIter.hasNext()) {
            result = valueIter.next();
            if(valueIter.hasNext()) {
                // The subquery should be scalar, but has produced
                // more than one result value - this is an exception case
                 throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30345, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30345, scalarSubquery.getCommand()));
            }
        }
        return result;
    }

    /**
     * @param container
     * @param tuple
     * @return
     * @throws TeiidProcessingException
     * @throws BlockedException
     * @throws TeiidComponentException
     */
    protected ValueIterator evaluateSubquery(SubqueryContainer<?> container, List<?> tuple)
    throws TeiidProcessingException, BlockedException, TeiidComponentException {
        throw new UnsupportedOperationException("Subquery evaluation not possible with a base Evaluator"); //$NON-NLS-1$
    }

    private CommandContext getContext(LanguageObject expression) throws TeiidComponentException {
        if (context == null) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30328, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30328, expression, QueryPlugin.Util.getString("Evaluator.no_value"))); //$NON-NLS-1$
        }
        return context;
    }

    protected Object evaluateProcedure(Function function, List<?> tuple,
            Object[] values) throws TeiidComponentException,
            TeiidProcessingException {
        throw new UnsupportedOperationException("Procedure evaluation not possible with a base Evaluator"); //$NON-NLS-1$
    }

}
