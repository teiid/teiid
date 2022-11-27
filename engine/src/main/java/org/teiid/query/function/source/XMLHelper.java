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

package org.teiid.query.function.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.util.CommandContext;
import org.teiid.query.xquery.XQueryExpression;

public class XMLHelper {

    private static XMLHelper INSTANCE;

    public static XMLHelper getInstance() {
        if (INSTANCE == null) {
            try {
                INSTANCE = (XMLHelper) ReflectionHelper.create("org.teiid.xquery.saxon.XMLHelperImpl", null, XMLHelper.class.getClassLoader());//$NON-NLS-1$
            } catch (TeiidException e) {
                INSTANCE = new XMLHelper();
            }
        }
        return INSTANCE;
    }

    /**
     *
     * @param xpath
     * @throws TeiidProcessingException
     */
    public void validateXpath(String xpath) throws TeiidProcessingException {
        throw new TeiidProcessingException("Cannot validate without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param name
     * @return
     * @throws TeiidProcessingException
     */
    public String[] validateQName(String name) throws TeiidProcessingException {
        throw new TeiidProcessingException("Cannot validate without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param prefix
     * @return
     * @throws TeiidProcessingException
     */
    public boolean isValidNCName(String prefix) throws TeiidProcessingException {
        throw new TeiidProcessingException("Cannot validate without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param value
     * @return
     * @throws TransformerException
     */
    public String convertToAtomicValue(Object value) throws TransformerException {
        throw new TransformerException("Cannot perform without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param name
     * @param fully
     * @return
     */
    public String escapeName(String name, boolean fully) {
        throw new TeiidRuntimeException("Cannot perform without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param xquery
     * @param namespaces
     * @param passing
     * @param columns
     * @return
     * @throws QueryResolverException
     */
    public XQueryExpression compile(String xquery, XMLNamespaces namespaces,
            List<DerivedColumn> passing, List<XMLColumn> columns) throws QueryResolverException {
        throw new QueryResolverException("Cannot compile XQuery without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param tuple
     * @param xmlQuery
     * @param exists
     * @param parameters
     * @param context
     * @return
     * @throws FunctionExecutionException
     * @throws TeiidComponentException
     * @throws BlockedException
     */
    public Object evaluateXMLQuery(List<?> tuple, XMLQuery xmlQuery,
            boolean exists, Map<String, Object> parameters,
            CommandContext context) throws FunctionExecutionException, BlockedException, TeiidComponentException {
        throw new TeiidComponentException("Cannot perform without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param val
     * @param expression
     * @param context
     * @return
     * @throws ExpressionEvaluationException
     */
    public Object evaluate(XMLType val, XMLCast expression,
            CommandContext context) throws ExpressionEvaluationException {
        throw new FunctionExecutionException("Cannot perform without the optional XML depedency"); //$NON-NLS-1$
    }

    /**
     *
     * @param id
     * @param xt
     * @param filteredColumns
     * @return
     * @throws TeiidComponentException
     */
    public RelationalNode newXMLTableNode(int id, XMLTable xt,
            ArrayList<XMLColumn> filteredColumns) throws TeiidComponentException {
        throw new TeiidComponentException("Cannot perform without the optional XML depedency"); //$NON-NLS-1$
    }

    public boolean isRealImplementation() {
        return false;
    }

}
