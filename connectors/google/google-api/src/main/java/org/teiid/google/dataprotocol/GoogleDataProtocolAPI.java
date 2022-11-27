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

package org.teiid.google.dataprotocol;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.teiid.google.auth.AuthHeaderFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.google.api.SpreadsheetAuthException;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetColumnType;
import org.teiid.translator.google.api.result.PartialResultExecutor;
import org.teiid.translator.google.api.result.RowsResult;
import org.teiid.translator.google.api.result.SheetRow;

/**
 *
 * This class is used to make requests to Google Visualization Data Protocol. The most important
 * method is executeQuery.
 *
 * @author fnguyen
 *
 */
public class GoogleDataProtocolAPI {
    private AuthHeaderFactory headerFactory = null;
    public static String ENCODING = "UTF-8"; //$NON-NLS-1$
    private static GoogleJSONParser PARSER = new GoogleJSONParser();

    public AuthHeaderFactory getHeaderFactory() {
        return headerFactory;
    }

    public void setHeaderFactory(AuthHeaderFactory headerFactory) {
        this.headerFactory = headerFactory;
    }

    /**
     * Most important method that will issue query [1] to specific worksheet. The columns in the query
     * should be identified by their real alphabetic name (A, B, C...).
     *
     * There is one important restriction to query. It should not contain offset and limit clauses.
     * To achieve functionality of offset and limit please use corresponding parameters in this method.
     *
     *
     * [1] https://developers.google.com/chart/interactive/docs/querylanguage
     *
     * @param query The query defined in [1]
     * @param batchSize How big portions of data should be returned by one roundtrip to Google.
     * @return Iterable RowsResult that will actually perform the roundtrips to Google for data
     */
    public RowsResult executeQuery(String spreadsheetKey, String worksheetTitle,
            String query, int batchSize, Integer offset, Integer limit) {

        RowsResult result = new RowsResult(new DataProtocolQueryStrategy(spreadsheetKey,worksheetTitle,query), batchSize);
        if (offset!= null)
            result.setOffset(offset);
        if (limit != null)
            result.setLimit(limit);

        return result;
    }

    public List<Column> getMetadata(String key, String worksheetTitle) {
        DataProtocolQueryStrategy dpqs = new DataProtocolQueryStrategy(key,worksheetTitle,"SELECT *"); //$NON-NLS-1$
        dpqs.getResultsBatch(0, 1);
        return dpqs.getMetadata();
    }

    /**
     * Logic to query portion of data from Google Visualization Data Protocol. We do not use any special library just simple
     * Http request. Google sends response back in CSV that we parse afterwards.
     *
     * @author fnguyen
     *
     */
    public class DataProtocolQueryStrategy implements PartialResultExecutor {
        private String spreadsheetKey;
        private String worksheetTitle;
        private String urlEncodedQuery;
        private List<Column> metadata;

        public DataProtocolQueryStrategy(String key, String worksheetTitle,
                String query) {
            super();
            this.spreadsheetKey = key;
            this.worksheetTitle = worksheetTitle;
            try {
                this.urlEncodedQuery = URLEncoder.encode(query, ENCODING);
            } catch (UnsupportedEncodingException e) {
                throw new SpreadsheetOperationException(e);
            }
        }

        public List<Column> getMetadata() {
            return metadata;
        }

        public List<SheetRow> getResultsBatch(int startIndex, int amount) {
            String boundariedQuery =null;
            String worksheet = null;
            try {
                boundariedQuery = getQueryWithBoundaries(amount, Math.max(0,(startIndex)));
                worksheet = URLEncoder.encode(worksheetTitle, ENCODING);
            } catch (UnsupportedEncodingException e) {
                throw new SpreadsheetOperationException(e);
            }
            HttpGet get = new HttpGet("https://spreadsheets.google.com/tq?key="+spreadsheetKey+"&sheet="+worksheet+"&tqx=responseHandler:x;out:json&tq="+boundariedQuery);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            get.setHeader("Authorization", headerFactory.getAuthHeader()); //$NON-NLS-1$

            try {
                DefaultHttpClient client = new DefaultHttpClient();
                try {
                    return executeAndParse(client, get);
                } catch (SpreadsheetAuthException e) {
                    // relogin
                    headerFactory.refreshToken();
                    get.setHeader("Authorization", headerFactory.getAuthHeader()); //$NON-NLS-1$
                    return executeAndParse(client, get);
                }
            } catch (IOException e) {
                throw new SpreadsheetOperationException("Error retrieving batch from Gogole Visualization Data protocol", e);
            }
        }

        private List<SheetRow> executeAndParse(HttpClient client, HttpGet get) throws IOException {
            HttpResponse response = client.execute(get);

            if (response.getStatusLine().getStatusCode() == 200)
            {
                Calendar cal = null;
                Reader reader = null;
                try {
                    reader = new InputStreamReader(response.getEntity().getContent(), Charset.forName(ENCODING));
                    Map<?, ?> jsonResponse = (Map<?, ?>)PARSER.parseObject(reader, true);
                    String status = (String)jsonResponse.get("status"); //$NON-NLS-1$
                    if ("error".equals(status)) { //$NON-NLS-1$
                        //TODO: better formatting
                        List<Map<?, ?>> errors = (List<Map<?, ?>>) jsonResponse.get("errors"); //$NON-NLS-1$
                        List<String> reasons = new ArrayList<String>();
                        for (Map<?, ?> map : errors) {
                            String reason = (String)map.get("reason"); //$NON-NLS-1$
                            if ("user_not_authenticated".equals(reason)) { //$NON-NLS-1$
                                throw new SpreadsheetAuthException("User not authenticated");
                            }
                            reasons.add(reason);
                        }
                        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Google request failed", errors); //$NON-NLS-1$
                        throw new SpreadsheetOperationException(reasons.toString());
                    }
                    //TODO: the warning could be sent to the client via the ExecutionContext

                    Map<?,?> table = (Map<?,?>)jsonResponse.get("table"); //$NON-NLS-1$
                    List<Map<?, ?>> cols = (List<Map<?, ?>>) table.get("cols"); //$NON-NLS-1$
                    this.metadata = new ArrayList<Column>(cols.size());
                    for (Map<?, ?> col : cols) {
                        Column c = new Column();
                        c.setAlphaName((String) col.get("id")); //$NON-NLS-1$
                        String label = (String)col.get("label"); //$NON-NLS-1$
                        if (label != null && !label.isEmpty()) {
                            c.setLabel(label);
                        }
                        String type = (String)col.get("type"); //$NON-NLS-1$
                        if (type != null) {
                            c.setDataType(SpreadsheetColumnType.valueOf(type.toUpperCase()));
                        }
                        this.metadata.add(c);
                    }

                    List<SheetRow> result = new ArrayList<SheetRow>();

                    List<Map<?,?>> rows = (List<Map<?,?>>) table.get("rows");  //$NON-NLS-1$
                    for (Map<?,?> row : rows) {
                        SheetRow returnRow = new SheetRow();
                        List<Map<?,?>> vals = (List<Map<?,?>>)row.get("c"); //$NON-NLS-1$
                        int i = -1;
                        for (Map<?,?> val : vals) {
                            i++;
                            if (val == null) {
                                returnRow.addColumn(null);
                                continue;
                            }
                            Object object = val.get("v"); //$NON-NLS-1$

                            if (object != null) {
                                //special handling for time types
                                Column c = this.metadata.get(i);
                                object = convertValue(cal, object, c.getDataType());
                            }

                            //TODO: empty string values could be interpreted as null
                            returnRow.addColumn(object);
                        }
                        result.add(returnRow);
                    }

                    return result;
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            } else if (response.getStatusLine().getStatusCode() == 500){
                //500 from server may not be a actual error. It can mean that offset is higher then actual result size. Can be solved by calling "count" first but performance penalty
                return new ArrayList<SheetRow>();
            }
            else {
                throw new SpreadsheetOperationException("Error when getting batch "+response.getStatusLine().getStatusCode()+":" +response.getStatusLine().getReasonPhrase());
            }
        }


        /**
         * Adds limit, offset to the query. This is slightly more complicated becuase limit/offset must appear
         * before certain clauses (label, format, options)
         * @param amount
         * @param offset
         * @return
         * @throws UnsupportedEncodingException
         */
        private String getQueryWithBoundaries(int amount, int offset) throws UnsupportedEncodingException {
            String[] keywordsToJump = new String[] {"label","format","options"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            int indexToPut = urlEncodedQuery.length();

            for (String jumpIt : keywordsToJump){
                int index = urlEncodedQuery.indexOf(jumpIt);

                if (index != -1) {
                    indexToPut = index;
                    break;
                }
            }

            return urlEncodedQuery.substring(0, indexToPut).toString() +URLEncoder.encode(" limit "+amount+" offset "+offset+" ",ENCODING).toString() //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    + urlEncodedQuery.substring(indexToPut).toString();
        }
    }

    static Object convertValue(Calendar cal, Object object, SpreadsheetColumnType type) {
        switch (type) {
        case DATE:
        case DATETIME:
            if (object instanceof String) {
                String stringVal = (String)object;
                if (stringVal.startsWith("Date(") && stringVal.endsWith(")")) { //$NON-NLS-1$ //$NON-NLS-2$
                    String[] parts = stringVal.substring(5, stringVal.length() - 1).split(","); //$NON-NLS-1$
                    if (cal == null) {
                        cal = Calendar.getInstance();
                    }
                    cal.clear();
                    if (type == SpreadsheetColumnType.DATETIME) {
                        cal.set(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]), Integer.valueOf(parts[2]), Integer.valueOf(parts[3]), Integer.valueOf(parts[4]), Integer.valueOf(parts[5]));
                        object = new Timestamp(cal.getTimeInMillis());
                    } else {
                        cal.set(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]), Integer.valueOf(parts[2]));
                        object = new Date(cal.getTimeInMillis());
                    }
                }
            }
            break;
        case TIMEOFDAY:
            if (object instanceof List<?>) {
                List<Double> doubleVals = (List<Double>)object;
                if (cal == null) {
                    cal = Calendar.getInstance();
                }
                cal.clear();
                cal.set(Calendar.YEAR, 1970);
                cal.set(Calendar.MONTH, Calendar.JANUARY);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.HOUR, doubleVals.get(0).intValue());
                cal.set(Calendar.MINUTE, doubleVals.get(1).intValue());
                cal.set(Calendar.SECOND, doubleVals.get(2).intValue());
                //TODO: it's not proper to convey the millis on a time value
                cal.set(Calendar.MILLISECOND, doubleVals.get(3).intValue());
                object = new Time(cal.getTimeInMillis());
            }
            break;
        }
        return object;
    }

}





