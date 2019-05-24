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

package org.teiid.xquery.saxon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.XMLType;
import org.teiid.query.function.source.XMLHelper;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.util.CommandContext;
import org.teiid.query.xquery.XQueryExpression;

public class XMLHelperImpl extends XMLHelper {

    @Override
    public XQueryExpression compile(String xquery, XMLNamespaces namespaces,
            List<DerivedColumn> passing, List<XMLColumn> columns)
            throws QueryResolverException {
        return SaxonXQueryExpression.compile(xquery, namespaces, passing, columns);
    }

    @Override
    public String convertToAtomicValue(Object value)
            throws TransformerException {
        return XQueryEvaluator.convertToAtomicValue(value).getStringValue();
    }

    @Override
    public String escapeName(String name, boolean fully) {
        return XMLFunctions.escapeName(name, fully);
    }

    @Override
    public Object evaluate(XMLType val, XMLCast expression,
            CommandContext context) throws ExpressionEvaluationException {
        return XQueryEvaluator.evaluate(val, expression, context);
    }

    @Override
    public Object evaluateXMLQuery(List<?> tuple, XMLQuery xmlQuery,
            boolean exists, Map<String, Object> parameters,
            CommandContext context) throws FunctionExecutionException, BlockedException, TeiidComponentException {
        return XQueryEvaluator.evaluateXMLQuery(tuple, xmlQuery, exists, parameters, context);
    }

    @Override
    public boolean isRealImplementation() {
        return true;
    }

    @Override
    public boolean isValidNCName(String prefix) throws TeiidProcessingException {
        return XMLFunctions.isValidNCName(prefix);
    }

    @Override
    public RelationalNode newXMLTableNode(int id, XMLTable xt,
            ArrayList<XMLColumn> filteredColumns)
            throws TeiidComponentException {
        SaxonXMLTableNode result = new SaxonXMLTableNode(id);
        result.setTable(xt);
        result.setProjectedColumns(filteredColumns);
        return result;
    }

    @Override
    public String[] validateQName(String name) throws TeiidProcessingException {
        return XMLFunctions.validateQName(name);
    }

    @Override
    public void validateXpath(String xpath) throws TeiidProcessingException {
        XMLFunctions.validateXpath(xpath);
    }

}
