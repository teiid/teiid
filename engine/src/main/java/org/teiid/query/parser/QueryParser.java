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

import static org.teiid.query.parser.SQLParserConstants.*;
import static org.teiid.query.parser.TeiidSQLParserTokenManager.*;

import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Parser;
import org.teiid.metadata.Server;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.DatabaseStore.Mode;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Expression;

/**
 * <p>Converts a SQL-string to an object version of a query.  This
 * QueryParser can be reused but is NOT thread-safe as the parser uses an
 * input stream.  Putting multiple queries into the same stream will result
 * in unpredictable and most likely incorrect behavior.
 */
public class QueryParser implements Parser {

    private static final class SingleSchemaDatabaseStore extends DatabaseStore {
        private final MetadataFactory factory;
        private TransformationMetadata transformationMetadata;

        private SingleSchemaDatabaseStore(MetadataFactory factory) {
            this.factory = factory;
        }

        @Override
        public Map<String, Datatype> getRuntimeTypes() {
            return factory.getDataTypes();
        }

        @Override
        protected TransformationMetadata getTransformationMetadata() {
            return transformationMetadata;
        }

        public void setTransformationMetadata(
                TransformationMetadata transformationMetadata) {
            this.transformationMetadata = transformationMetadata;
        }
    }

    private static ThreadLocal<QueryParser> QUERY_PARSER = new ThreadLocal<QueryParser>() {
        /**
         * @see java.lang.ThreadLocal#initialValue()
         */
        @Override
        protected QueryParser initialValue() {
            return new QueryParser();
        }
    };

    private static final String XQUERY_DECLARE = "declare"; //$NON-NLS-1$
    private static final String XML_OPEN_BRACKET = "<"; //$NON-NLS-1$
    private static final String NONE = "none"; //$NON-NLS-1$

    private SQLParser parser;
    private TeiidSQLParserTokenManager tm;

    /**
     * Construct a QueryParser - this may be reused.
     */
    public QueryParser() {}

    public static QueryParser getQueryParser() {
        return QUERY_PARSER.get();
    }

    /**
     * Helper method to get a SQLParser instance for given sql string.
     */
    private SQLParser getSqlParser(String sql) {
        return getSqlParser(new StringReader(sql));
    }

    private SQLParser getSqlParser(Reader sql) {
        if(parser == null) {
            JavaCharStream jcs = new JavaCharStream(sql);
            tm = new TeiidSQLParserTokenManager(new JavaCharStream(sql));
            parser = new SQLParser(tm);
            parser.jj_input_stream = jcs;
        } else {
            parser.ReInit(sql);
            tm.reinit();
        }
        return parser;
    }

    /**
     * Takes a SQL string representing a Command and returns the object
     * representation.
     * @param sql SQL string
     * instead of string litral
     * @return SQL object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    public Command parseCommand(String sql) throws QueryParserException {
        return parseCommand(sql, new ParseInfo());
    }

    public Command parseProcedure(String sql, boolean update) throws QueryParserException {
        try{
            if (update) {
                return getSqlParser(sql).forEachRowTriggerAction(new ParseInfo());
            }
            Command result = getSqlParser(sql).procedureBodyCommand(new ParseInfo());
            result.setCacheHint(SQLParserUtil.getQueryCacheOption(sql));
            return result;
        } catch(ParseException pe) {
            throw convertParserException(pe);
        } finally {
            tm.reinit();
        }
    }

    /**
     * Takes a SQL string representing a Command and returns the object
     * representation.
     * @param sql SQL string
     * @param parseInfo - instructions to parse
     * @return SQL object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    public Command parseCommand(String sql, ParseInfo parseInfo) throws QueryParserException {
        if(sql == null || sql.length() == 0) {
             throw new QueryParserException(QueryPlugin.Event.TEIID30377, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30377));
        }

        Command result = null;
        try{
            result = getSqlParser(sql).command(parseInfo);
            result.setCacheHint(SQLParserUtil.getQueryCacheOption(sql));
        } catch(ParseException pe) {
            if(sql.startsWith(XML_OPEN_BRACKET) || sql.startsWith(XQUERY_DECLARE)) {
                 throw new QueryParserException(QueryPlugin.Event.TEIID30378, convertParserException(pe), QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30378, sql));
            }
            throw convertParserException(pe);
        } finally {
            tm.reinit();
        }
        return result;
    }

    public CacheHint parseCacheHint(String sql) {
        if(sql == null || sql.length() == 0) {
             return null;
        }
        return SQLParserUtil.getQueryCacheOption(sql);
    }

    /**
     * Takes a SQL string representing an SQL criteria (i.e. just the WHERE
     * clause) and returns the object representation.
     * @param sql SQL criteria (WHERE clause) string
     * @return Criteria SQL object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    public Criteria parseCriteria(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlCrit")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Criteria result = null;
        try{
            result = getSqlParser(sql).criteria(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);
        } finally {
            tm.reinit();
        }
        return result;
    }

    private QueryParserException convertParserException(ParseException pe) {
        if (pe.currentToken == null) {
            List<Token> preceeding = findPreceeding(parser.token, 1);
            if (!preceeding.isEmpty()) {
                pe.currentToken = preceeding.get(0);
            } else {
                pe.currentToken = parser.token;
            }
        }
        QueryParserException qpe = new QueryParserException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31100, getMessage(pe, 10)));
        qpe.setParseException(pe);
        return qpe;
    }

    /**
     * The default JavaCC message is not very good.  This method produces a much more readable result.
     * @param pe
     * @param maxExpansions
     * @return
     */
    public String getMessage(ParseException pe, int maxExpansions) {
        if (pe.expectedTokenSequences == null) {
            if (pe.currentToken == null) {
                return pe.getMessage();
            }
            StringBuilder sb = encountered(pe, pe.currentToken.next!=null?1:0);
            if (pe.currentToken.kind == INVALID_TOKEN) {
                sb.append(QueryPlugin.Util.getString("QueryParser.lexicalError", pe.currentToken.image)); //$NON-NLS-1$
            } else if (pe.currentToken.next != null && pe.currentToken.next.kind == -1) {
                sb.append(QueryPlugin.Util.getString("QueryParser.lexicalError", pe.currentToken.next.image)); //$NON-NLS-1$
            }
            if (pe.getMessage() != null) {
                sb.append(pe.getMessage());
            }
            return sb.toString();
        }

        Token currentToken = pe.currentToken;

        //if the next token is invalid, we wan to use a lexical message, not the sequences
        if (currentToken.next.kind == INVALID_TOKEN) {
            StringBuilder retval = encountered(pe, 1);
            retval.append(QueryPlugin.Util.getString("QueryParser.lexicalError", currentToken.next.image)); //$NON-NLS-1$
            return retval.toString();
        }

        //find the longest match chain an all possible end tokens
        int[][] expectedTokenSequences = pe.expectedTokenSequences;
        int[] ex = null;
        Set<Integer> last = new TreeSet<Integer>();
        outer : for (int i = 0; i < expectedTokenSequences.length; i++) {
            if (ex == null || expectedTokenSequences[i].length > ex.length) {
                ex = expectedTokenSequences[i];
                last.clear();
            } else if (expectedTokenSequences[i].length < ex.length) {
                continue;
            } else {
                for (int j = 0; j < ex.length -1; j++) {
                    if (ex[j] != expectedTokenSequences[i][j]) {
                        continue outer; //TODO : not sure how to handle this case
                    }
                }
            }
            last.add(expectedTokenSequences[i][expectedTokenSequences[i].length-1]);
        }
        if (ex == null) {
            return pe.getMessage(); //shouldn't happen
        }

        StringBuilder retval = encountered(pe, ex.length);

        //output the expected tokens condensing the id/non-reserved
        retval.append("Was expecting: "); //$NON-NLS-1$
        boolean id = last.contains(SQLParserConstants.ID);
        int count = 0;
        for (Integer t : last) {
            String img = tokenImage[t];
            if (id && img.startsWith("\"") //$NON-NLS-1$
                    && Character.isLetter(img.charAt(1))
                    && (!SQLConstants.isReservedWord(img.substring(1, img.length()-1)) || img.equals("\"default\""))) { //$NON-NLS-1$
                continue;
            }
            if (count > 0) {
                retval.append(" | "); //$NON-NLS-1$
            }
            count++;
            if (t == SQLParserConstants.ID) {
                retval.append("id"); //$NON-NLS-1$
            } else {
                retval.append(img);
            }
            if (count == maxExpansions) {
                retval.append(" ..."); //$NON-NLS-1$
                break;
            }
        }
        return retval.toString();
    }

    private StringBuilder encountered(ParseException pe, int offset) {
        StringBuilder retval = new StringBuilder("Encountered \""); //$NON-NLS-1$
        Token currentToken = pe.currentToken;
        for (int i = 1; i < offset; i++) {
            //TODO: for large offsets we don't have to call findPreceeding
            currentToken = currentToken.next;
        }
        List<Token> preceeding = findPreceeding(currentToken, 2);
        if (offset > 0 && !preceeding.isEmpty()) {
            addTokenSequence(preceeding.size() + 1, retval, null, preceeding.get(0), false);
        } else {
            addTokenSequence(1, retval, null, currentToken, offset==0);
        }
        if (currentToken.next != null && offset>0) {
            addTokenSequence(3, retval, currentToken, currentToken.next, true);
            currentToken = currentToken.next; //move to the error token
        }
        retval.append("\" at line ").append(currentToken.beginLine).append(", column ").append(currentToken.beginColumn); //$NON-NLS-1$ //$NON-NLS-2$
        retval.append(".\n"); //$NON-NLS-1$
        return retval;
    }

    private List<Token> findPreceeding(Token currentToken, int count) {
        LinkedList<Token> preceeding = new LinkedList<Token>();
        Token tok = this.tm.head;
        boolean found = false;
        while (tok != null) {
            if (tok == currentToken) {
                found = true;
                break;
            }
            preceeding.add(tok);
            if (preceeding.size() > count) {
                preceeding.removeFirst();
            }
            tok = tok.next;
        }
        if (!found) {
            preceeding.clear();
        }
        return preceeding;
    }

    private Token addTokenSequence(int maxSize, StringBuilder retval,
            Token last, Token tok, boolean highlight) {
        for (int i = 0; i < maxSize && tok != null; i++) {
            if (last != null && last.endColumn + 1 != tok.beginColumn && tok.kind != SQLParserConstants.EOF) {
                retval.append(" "); //$NON-NLS-1$
            }
            last = tok;
            if (i == 0 && highlight) {
                retval.append("[*]"); //$NON-NLS-1$
            }
            if (tok.image != null && !tok.image.isEmpty()) {
                add_escapes(tok.image, retval);
                if (i == 0 && highlight) {
                    retval.append("[*]"); //$NON-NLS-1$
                }
            }
            while (tok.next == null) {
                if (this.parser.getNextToken() == null) {
                    break;
                }
            }
            tok = tok.next;
        }
        return last;
    }

      /**
       * Used to convert raw characters to their escaped version
       * when these raw version cannot be used as part of an ASCII
       * string literal.  Also escapes double quotes.
       */
      protected void add_escapes(String str, StringBuilder retval) {
          for (int i = 0; i < str.length(); i++) {
              char ch = str.charAt(i);
            switch (ch)
            {
               case 0 :
                  continue;
               case '\b':
                  retval.append("\\b"); //$NON-NLS-1$
                  continue;
               case '\t':
                  retval.append("\\t"); //$NON-NLS-1$
                  continue;
               case '\n':
                  retval.append("\\n"); //$NON-NLS-1$
                  continue;
               case '\f':
                  retval.append("\\f"); //$NON-NLS-1$
                  continue;
               case '\r':
                  retval.append("\\r"); //$NON-NLS-1$
                  continue;
               case '\"':
                  retval.append("\\\""); //$NON-NLS-1$
                  continue;
               case '\\':
                  retval.append("\\\\"); //$NON-NLS-1$
                  continue;
               default:
                  if (ch < 0x20 || ch > 0x7e) {
                     String s = "0000" + Integer.toString(ch, 16); //$NON-NLS-1$
                     retval.append("\\u" + s.substring(s.length() - 4, s.length())); //$NON-NLS-1$
                  } else {
                     retval.append(ch);
                  }
                  continue;
            }
          }
       }

    /**
     * Takes a SQL string representing an SQL expression
     * and returns the object representation.
     * @param sql SQL expression string
     * @return SQL expression object
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    public Expression parseExpression(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlExpr")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Expression result = null;
        try{
            result = getSqlParser(sql).expression(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);
        } finally {
            tm.reinit();
        }
        return result;
    }

    public Expression parseSelectExpression(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlExpr")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Expression result = null;
        try{
            result = getSqlParser(sql).selectExpression(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);
        } finally {
            tm.reinit();
        }
        return result;
    }

    public void parseDDL(MetadataFactory factory, String ddl) {
        parseDDL(factory, new StringReader(ddl));
    }

    public void parseDDL(final MetadataFactory factory, Reader ddl) {
        SingleSchemaDatabaseStore store = new SingleSchemaDatabaseStore(factory);

        store.startEditing(true);
        Database db = new Database(factory.getVdbName(), factory.getVdbVersion());
        store.databaseCreated(db);
        store.databaseSwitched(factory.getVdbName(), factory.getVdbVersion());

        store.dataWrapperCreated(new DataWrapper(NONE));
        Server server = new Server(NONE);
        server.setDataWrapper(NONE);

        store.serverCreated(server);
        if (factory.getSchema().isPhysical()) {
            Server s = new Server(factory.getSchema().getName());
            s.setDataWrapper(NONE);
            store.serverCreated(s);
        }
        List<String> servers = Collections.emptyList();
        store.schemaCreated(factory.getSchema(), servers);

        //with the schema created, create the TransformationMetadata
        CompositeMetadataStore cms = new CompositeMetadataStore(db.getMetadataStore());
        TransformationMetadata qmi = new TransformationMetadata(DatabaseUtil.convert(db), cms, null, null, null);

        store.setTransformationMetadata(qmi.getDesignTimeMetadata());

        store.schemaSwitched(factory.getSchema().getName());
        store.setMode(Mode.SCHEMA);
        store.setStrict(true);
        try {
            parseDDL(store, ddl);
        } finally {
            store.stopEditing();
        }
    }

    public void parseDDL(DatabaseStore repository, Reader ddl)
            throws MetadataException {
        SQLParser sqlParser = getSqlParser(ddl);
        try {
            sqlParser.parseMetadata(repository);
        } catch (org.teiid.metadata.ParseException e) {
            throw e;
        } catch (MetadataException e) {
            Token t = sqlParser.token;
            throw new org.teiid.metadata.ParseException(QueryPlugin.Event.TEIID31259, e,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31259, t.image, t.beginLine, t.beginColumn, e.getMessage()));
        } catch (ParseException e) {
            throw new org.teiid.metadata.ParseException(QueryPlugin.Event.TEIID30386, convertParserException(e));
        } finally {
            tm.reinit();
        }
    }

}
