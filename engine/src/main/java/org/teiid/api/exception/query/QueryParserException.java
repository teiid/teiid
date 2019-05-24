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

package org.teiid.api.exception.query;

import org.teiid.core.BundleUtil;
import org.teiid.query.parser.ParseException;


/**
 * Thrown when a query cannot be parsed.  This is most likely due to not
 * following the Query Parser grammar, which defines how queries are parsed.
 */
public class QueryParserException extends QueryProcessingException {

    private static final long serialVersionUID = 7565287582917117432L;
    private ParseException parseException;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public QueryParserException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public QueryParserException( String message ) {
        super( message );
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public QueryParserException( Throwable e, String message ) {
        super( e, message );
    }


    public ParseException getParseException() {
        return parseException;
    }

    public void setParseException(ParseException parseException) {
        this.parseException = parseException;
    }

    public QueryParserException(BundleUtil.Event event, Throwable e) {
        super( event, e);
    }

    public QueryParserException(BundleUtil.Event event, Throwable e, String msg) {
        super(event, e, msg);
    }

    public QueryParserException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }
}
