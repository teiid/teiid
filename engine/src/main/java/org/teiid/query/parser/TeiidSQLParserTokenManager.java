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

import java.io.IOException;

/**
 * A customized TokenManager to keep a token history and to treat lexical errors as invalid single character tokens
 */
class TeiidSQLParserTokenManager extends SQLParserTokenManager {

    static final int INVALID_TOKEN = -1;
    int tokenCount = 0;
    Token head;

    public TeiidSQLParserTokenManager(JavaCharStream stream) {
        super(stream);
    }

    void reinit() {
        tokenCount = 0;
        head = null;
    }

    @Override
    public Token getNextToken() {
        try {
            Token t = super.getNextToken();
            //if we're in the default lex state, keep track of a history of tokens
            //this logic is not perfect as deep lookaheads can ruin the positioning
            if (tokenCount == 0) {
                head = t;
            }
            tokenCount++;
            if (tokenCount > 10) {
                head = head.next;
            }
            return t;
        } catch (TokenMgrError err) {
            Token t = new Token();
            t.kind = INVALID_TOKEN;
            t.beginColumn = this.input_stream.getBeginColumn();
            t.beginLine = this.input_stream.getBeginLine();
            t.endColumn = t.beginColumn;
            t.endLine = t.beginLine;
            t.image = this.input_stream.GetImage().substring(0, 1);
            try {
                //mark the char a consumed
                this.input_stream.readChar();
            } catch (IOException e) {
            }
            return t;
        }
    }

}
