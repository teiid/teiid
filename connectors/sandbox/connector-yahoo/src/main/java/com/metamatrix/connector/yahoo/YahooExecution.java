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

package com.metamatrix.connector.yahoo;

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
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/**
 * Represents the execution of a command.
 */
public class YahooExecution extends BasicExecution implements ResultSetExecution {
    public static final String HTTP_PROXY_HOST = "HttpProxyHost"; //$NON-NLS-1$
    public static final String HTTP_PROXY_PORT = "HttpProxyPort"; //$NON-NLS-1$

    public static final String JAVA_PROP_HTTP_PROXY_HOST = "http.proxyHost"; //$NON-NLS-1$
    public static final String JAVA_PROP_HTTP_PROXY_PORT = "http.proxyPort"; //$NON-NLS-1$

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy"); //$NON-NLS-1$
    private static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mma"); //$NON-NLS-1$

    // Connector resources
    private ConnectorEnvironment env;
    private RuntimeMetadata metadata;
    private IQuery command;
    
    // Execution state
    List results;
    int[] neededColumns;
    int returnIndex = 0;
    private IQuery query;

    private String previousHttpProxyHost;
    private String previousHttpProxyPort;

    /**
     * 
     */
    public YahooExecution(IQuery query, ConnectorEnvironment env, RuntimeMetadata metadata) {
        this.env = env;
        this.metadata = metadata;
        this.query = query;
    }
    
    /* 
     * @see com.metamatrix.data.SynchQueryExecution#execute(com.metamatrix.data.language.IQuery, int)
     */
    @Override
    public void execute() throws ConnectorException {
        // Log our command
        env.getLogger().logTrace("Yahoo executing command: " + command); //$NON-NLS-1$

        // Build url
        String yahooUrl = translateIntoUrl(query);
        
        // Execute url to get results
        this.results = executeUrl(yahooUrl, this.env.getLogger());
        
        // Determine needed columns in results
        this.neededColumns = getNeededColumns(query.getSelect(), this.metadata);        
    }    

    static String translateIntoUrl(IQuery query) throws ConnectorException {
        StringBuffer url = new StringBuffer();
        url.append(YahooPlugin.Util.getString("YahooExecution.URL_BEGIN")); //$NON-NLS-1$
        
        Set tickers = getTickers(query);
        if(tickers.size() == 0) { 
            throw new ConnectorException(YahooPlugin.Util.getString("YahooExecution.No_tickers")); //$NON-NLS-1$
        }
        String urlAppendChar = YahooPlugin.Util.getString("YahooExecution.URL_APPEND_CHAR"); //$NON-NLS-1$
        Iterator tickerIter = tickers.iterator();
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
    static Set getTickers(IQuery query) throws ConnectorException {
        ICriteria crit = query.getWhere();
        if(crit == null) {
            throw new ConnectorException(YahooPlugin.Util.getString("YahooExecution.Must_have_criteria")); //$NON-NLS-1$
        }
        return TickerCollectorVisitor.getTickers(crit);
    }

    /**
     * @param yahooUrl
     * @return
     */
    protected List executeUrl(String yahooUrl, ConnectorLogger logger) throws ConnectorException {
        List rows = new ArrayList();
        InputStreamReader inSR  = null;
        BufferedReader buffReader = null;
        
        try {
            setProxy();
            
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
                rows.add(parseLine(line, logger));
            }
            // clean up our opened connections
            buffReader.close();
            inSR.close();
                        
        } catch(MalformedURLException mue){
            throw new ConnectorException(mue, mue.getMessage());
        } catch(IOException e) {
            throw new ConnectorException(e, e.getMessage());
        } finally {
            buffReader = null;
            inSR = null; 
            resetProxy();
        }
        
        return rows;
    }

    /**
     * @param line
     * @return
     */
    static List parseLine(String line, ConnectorLogger logger) {
        List row = new ArrayList();
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
                        Date date = DATE_FORMAT.parse(data);
                        row.add(new java.sql.Date(date.getTime()));
                    } catch(ParseException e) {
                        Object[] params = new Object[] { data, e.getMessage() };
                        logger.logWarning(YahooPlugin.Util.getString("YahooExecution.Parse_date_error", params)); //$NON-NLS-1$
                        row.add(null);
                    }
                } else{
                    row.add(null);
                }
            } else if(i==3) {
                if(!data.equals("0")){ //$NON-NLS-1$
                    try {
                        Date time = TIME_FORMAT.parse(data);
                        row.add(new java.sql.Time(time.getTime()));
                    } catch(ParseException e) {
                        Object[] params = new Object[] { data, e.getMessage() };
                        logger.logWarning(YahooPlugin.Util.getString("YahooExecution.Parse_time_value", params)); //$NON-NLS-1$
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
     */
    static int[] getNeededColumns(ISelect select, RuntimeMetadata metadata) throws ConnectorException {
        int[] cols = new int[select.getSelectSymbols().size()];
        Iterator iter = select.getSelectSymbols().iterator();
        for(int i=0; iter.hasNext(); i++) {
            ISelectSymbol symbol = (ISelectSymbol) iter.next();
            IExpression expr = symbol.getExpression();
            if(expr instanceof IElement) {
                Element element = ((IElement)expr).getMetadataObject();
                cols[i] = element.getPosition();
            } else {
                throw new ConnectorException(YahooPlugin.Util.getString("YahooExecution.Invalid_select_symbol", expr)); //$NON-NLS-1$
            }
        }
        
        return cols;
    }

    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
        if (returnIndex < results.size()) {
            List row = (List) results.get(returnIndex++);
            return projectRow(row, neededColumns);
        }
        
        return null;
    }


    /**
     * @param row
     * @param neededColumns
     */
    static List projectRow(List row, int[] neededColumns) {
        List output = new ArrayList(neededColumns.length);
        
        for(int i=0; i<neededColumns.length; i++) {
            output.add(row.get(neededColumns[i]));
        }
        
        return output;    
    }

    /* 
     * @see com.metamatrix.data.Execution#close()
     */
    @Override
    public void close() throws ConnectorException {
        // nothing to do
    }

    /* 
     * @see com.metamatrix.data.Execution#cancel()
     */
    @Override
    public void cancel() throws ConnectorException {

    }

    private void setProxy() {
        Properties props = env.getProperties();
        String proxyHost = props.getProperty(HTTP_PROXY_HOST);
        String proxyPort = props.getProperty(HTTP_PROXY_PORT);
        previousHttpProxyHost = System.getProperty(JAVA_PROP_HTTP_PROXY_HOST);
        previousHttpProxyPort = System.getProperty(JAVA_PROP_HTTP_PROXY_PORT);
                 
        if(proxyHost != null) {
            System.setProperty(JAVA_PROP_HTTP_PROXY_HOST, proxyHost);
            if(proxyPort == null) {
                env.getLogger().logWarning(YahooPlugin.Util.getString("YahooConnector.proxyPortNotSet"));//$NON-NLS-1$
            }else {
                System.setProperty(JAVA_PROP_HTTP_PROXY_PORT, proxyPort);
            }
        }
    }
    
    private void resetProxy() {
        if(previousHttpProxyHost != null) {
            System.setProperty(JAVA_PROP_HTTP_PROXY_HOST, previousHttpProxyHost);
        }
        if(previousHttpProxyPort != null) {
            System.setProperty(JAVA_PROP_HTTP_PROXY_PORT, previousHttpProxyPort);
        }
    }
}
