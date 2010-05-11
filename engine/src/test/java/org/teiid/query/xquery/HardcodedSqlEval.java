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

package org.teiid.query.xquery;

import java.io.StringReader;
import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.xquery.XQuerySQLEvaluator;



/** 
 */
public class HardcodedSqlEval implements XQuerySQLEvaluator {

    String result;
    Map<String, Object> params;
    public HardcodedSqlEval(String result) {
        this.result = result;
    }

    public Source executeSQL(String sql) 
        throws QueryParserException, TeiidProcessingException, TeiidComponentException {
        if (this.result != null) {
            return new StreamSource(new StringReader(result));
        }
        return null;
    }

    /** 
     * @see org.teiid.query.xquery.XQuerySQLEvaluator#close()
     */
    public void close() throws TeiidComponentException {
    }

	@Override
	public Object getParameterValue(String key)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		return params.get(key);
	}

}
