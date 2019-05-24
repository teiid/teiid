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

package org.teiid.translator.yahoo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.teiid.language.ColumnReference;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


/**
 * Represents the execution of a command.
 */
public class YahooExecution implements ResultSetExecution {

    public static final String JAVA_PROP_HTTP_PROXY_HOST = "http.proxyHost"; //$NON-NLS-1$
    public static final String JAVA_PROP_HTTP_PROXY_PORT = "http.proxyPort"; //$NON-NLS-1$

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy"); //$NON-NLS-1$
    private static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mma"); //$NON-NLS-1$

    private Select command;

    // Execution state
    Iterator<List<?>> results;
    int[] neededColumns;
    private Select query;

    /**
     *
     */
    public YahooExecution(Select query) {
        this.query = query;
    }

    @Override
    public void execute() throws TranslatorException {
        // Log our command
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Yahoo executing command: " + command); //$NON-NLS-1$

        // Build url
        String yahooUrl = translateIntoUrl(query);

        // Execute url to get results
        this.results = executeUrl(yahooUrl).iterator();

        // Determine needed columns in results
        this.neededColumns = getNeededColumns(query.getDerivedColumns());
    }

    static String translateIntoUrl(Select query) throws TranslatorException {
        StringBuffer url = new StringBuffer();
        url.append(YahooPlugin.Util.getString("YahooExecution.URL_BEGIN")); //$NON-NLS-1$

        Set<String> tickers = getTickers(query);
        if(tickers.size() == 0) {
            throw new TranslatorException(YahooPlugin.Util.getString("YahooExecution.No_tickers")); //$NON-NLS-1$
        }
        String urlAppendChar = YahooPlugin.Util.getString("YahooExecution.URL_APPEND_CHAR"); //$NON-NLS-1$
        Iterator<String> tickerIter = tickers.iterator();
        url.append(tickerIter.next());
        while(tickerIter.hasNext()) {
            url.append(urlAppendChar);
            url.append(tickerIter.next());
        }

        url.append(YahooPlugin.Util.getString("YahooExecution.URL_END")); //$NON-NLS-1$
        return url.toString();
    }

    /**
     * @return
     */
    static Set<String> getTickers(Select query) throws TranslatorException {
        Condition crit = query.getWhere();
        if(crit == null) {
            throw new TranslatorException(YahooPlugin.Util.getString("YahooExecution.Must_have_criteria")); //$NON-NLS-1$
        }
        return TickerCollectorVisitor.getTickers(crit);
    }

    /**
     * @param yahooUrl
     * @return
     */
    protected List<List<?>> executeUrl(String yahooUrl) throws TranslatorException {
        List<List<?>> rows = new ArrayList<List<?>>();
        InputStreamReader inSR  = null;
        BufferedReader buffReader = null;

        try {
            // create the URL object
            URL url = new URL(yahooUrl);

            // create the connection to the URL
            URLConnection conn = url.openConnection();

            // establish the connection to the URL
            conn.connect();

            // get the stream from the commection
            inSR = new InputStreamReader(conn.getInputStream());

            // place the stream into a buffered reader
            buffReader = new BufferedReader(inSR);

            // now read each line from the Yahoo! Source and place
            // it into a StringBuffer object
            String line = null;
            while((line = buffReader.readLine()) != null){
                rows.add(parseLine(line));
            }
            // clean up our opened connections
            buffReader.close();
            inSR.close();

        } catch(MalformedURLException mue){
            throw new TranslatorException(mue, mue.getMessage());
        } catch(IOException e) {
            throw new TranslatorException(e, e.getMessage());
        } finally {
            if (buffReader != null) {
                try {
                    buffReader.close();
                } catch (IOException e) {
                }
            }
        }

        return rows;
    }

    /**
     * @param line
     * @return
     */
    static List<Object> parseLine(String line) {
        List<Object> row = new ArrayList<Object>();
        StringTokenizer rowToken = new StringTokenizer(line,","); //$NON-NLS-1$
        for(int i=0; rowToken.hasMoreTokens(); i++){
            String data = rowToken.nextToken();
            if(data.charAt(0) == '"') {
                data = data.substring(1, data.length()-1);
            }

            if(data.equals("N/A")) { //$NON-NLS-1$
                row.add(null);
            } else if(i==1 || i==4 || i== 5 || i==6 || i==7) {
                row.add(Double.valueOf(data));
            } else if(i==8) {
                row.add(new BigInteger(data));
            } else if(i==2) {
                if(!data.equals("0")){ //$NON-NLS-1$
                    try {
                        synchronized (DATE_FORMAT) {
                            Date date = DATE_FORMAT.parse(data);
                            row.add(new java.sql.Date(date.getTime()));
                        }
                    } catch(ParseException e) {
                        Object[] params = new Object[] { data, e.getMessage() };
                        LogManager.logWarning(LogConstants.CTX_CONNECTOR, YahooPlugin.Util.gs(YahooPlugin.Event.TEIID14001, params));
                        row.add(null);
                    }
                } else{
                    row.add(null);
                }
            } else if(i==3) {
                if(!data.equals("0")){ //$NON-NLS-1$
                    try {
                        synchronized (TIME_FORMAT) {
                            Date time = TIME_FORMAT.parse(data);
                            row.add(new java.sql.Time(time.getTime()));
                        }
                    } catch(ParseException e) {
                        Object[] params = new Object[] { data, e.getMessage() };
                        LogManager.logWarning(LogConstants.CTX_CONNECTOR, YahooPlugin.Util.gs(YahooPlugin.Event.TEIID14002, params));
                        row.add(null);
                    }
                } else {
                    row.add(null);
                }

            } else {
                row.add(data);
            }
        }

        return row;
    }

    /**
     * @param select
     * @return
     * @throws TranslatorException
     */
    static int[] getNeededColumns(List<DerivedColumn> select) throws TranslatorException {
        int[] cols = new int[select.size()];
        Iterator<DerivedColumn> iter = select.iterator();
        for(int i=0; iter.hasNext(); i++) {
            DerivedColumn symbol = iter.next();
            Expression expr = symbol.getExpression();
            if(expr instanceof ColumnReference) {
                Column element = ((ColumnReference)expr).getMetadataObject();
                cols[i] = element.getPosition();
            } else {
                throw new TranslatorException(YahooPlugin.Util.getString("YahooExecution.Invalid_select_symbol", expr)); //$NON-NLS-1$
            }
        }

        return cols;
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (results.hasNext()) {
            return projectRow(results.next(), neededColumns);
        }
        return null;
    }

    /**
     * @param row
     * @param neededColumns
     */
    static List<Object> projectRow(List<?> row, int[] neededColumns) {
        List<Object> output = new ArrayList<Object>(neededColumns.length);

        for(int i=0; i<neededColumns.length; i++) {
            output.add(row.get(neededColumns[i]-1));
        }

        return output;
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void cancel() throws TranslatorException {

    }
}
