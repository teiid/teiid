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
package org.teiid.resource.adapter.google;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.google.SpreadsheetConfiguration;
import org.teiid.google.SpreadsheetConnectionImpl;
import org.teiid.google.SpreadsheetConnectionImpl4;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;

public class SpreadsheetManagedConnectionFactory extends BasicManagedConnectionFactory implements SpreadsheetConfiguration {

    public static final String V_3 = "v3"; //$NON-NLS-1$
    public static final String V_4 = "v4"; //$NON-NLS-1$

    private static final long serialVersionUID = -1832915223199053471L;
    private Integer batchSize = 4096;
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(SpreadsheetManagedConnectionFactory.class);
    public static final String SPREADSHEET_NAME = "SpreadsheetName"; //$NON-NLS-1$

    private String spreadsheetName;

    private String spreadsheetId;

    private String spreadsheetMap;

    private Map<String, String> spreadsheets;

    private String apiVersion = V_3;

    private String refreshToken;

    private String clientId;
    private String clientSecret;

    private static class SpreadsheetConnection extends SpreadsheetConnectionImpl implements ResourceConnection {

        public SpreadsheetConnection(SpreadsheetConfiguration config,
                AtomicReference<SpreadsheetInfo> spreadsheetInfo) {
            super(config, spreadsheetInfo);
        }

    }

    private static class SpreadsheetConnection4 extends SpreadsheetConnectionImpl4 implements ResourceConnection {

        public SpreadsheetConnection4(SpreadsheetConfiguration config,
                AtomicReference<SpreadsheetInfo> spreadsheetInfo) {
            super(config, spreadsheetInfo);
        }

    }

    @Override
    @SuppressWarnings("serial")
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory() throws ResourceException {
        checkConfig();

        return new BasicConnectionFactory<ResourceConnection>() {
            //share the spreadsheet info among all connections
            private AtomicReference<SpreadsheetInfo> spreadsheetInfo = new AtomicReference<SpreadsheetInfo>();

            @Override
            public ResourceConnection getConnection() throws ResourceException {
                if (apiVersion.equals(V_3)) {
                    return new SpreadsheetConnection(SpreadsheetManagedConnectionFactory.this, spreadsheetInfo);
                }
                return new SpreadsheetConnection4(SpreadsheetManagedConnectionFactory.this, spreadsheetInfo);
            }
        };
    }

   private void checkConfig() throws ResourceException {

        //SpreadsheetName should be set
        if ((getSpreadsheetName()==null ||  getSpreadsheetName().trim().equals("")) && getSpreadsheetId() == null && spreadsheetMap == null){ //$NON-NLS-1$
            throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.
                    getString("provide_spreadsheetname",SpreadsheetManagedConnectionFactory.SPREADSHEET_NAME));      //$NON-NLS-1$
        }

        if (apiVersion == null || !(apiVersion.equals(V_3) || apiVersion.equals(V_4))) {
            throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.getString("invalid_protocol"));   //$NON-NLS-1$
        }

        if (getRefreshToken() == null || getRefreshToken().trim().equals("")){ //$NON-NLS-1$
            throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.getString("oauth_requires"));   //$NON-NLS-1$
        }

        if (apiVersion.equals(V_4)) {
            if (getClientId() == null || getClientSecret() == null) {
                throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.getString("oauth_requires"));   //$NON-NLS-1$
            }
            if (getSpreadsheetId() == null && spreadsheetMap == null) {
                throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.getString("v4_sheet_id"));   //$NON-NLS-1$
            }
            if (spreadsheetMap != null) {
                spreadsheets = new LinkedHashMap<>();
                String[] pairs = spreadsheetMap.split(";"); //$NON-NLS-1$
                for (String pair : pairs) {
                    String[] prefixId = pair.split("=", 2); //$NON-NLS-1$
                    if (prefixId.length != 2) {
                        throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.getString("invalid_mapping"));   //$NON-NLS-1$
                    }
                    spreadsheets.put(prefixId[0], prefixId[1]);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((batchSize == null) ? 0 : batchSize.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        result = prime * result
                + ((clientSecret == null) ? 0 : clientSecret.hashCode());
        result = prime * result
                + ((refreshToken == null) ? 0 : refreshToken.hashCode());
        result = prime * result
                + ((spreadsheetName == null) ? 0 : spreadsheetName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SpreadsheetManagedConnectionFactory other = (SpreadsheetManagedConnectionFactory) obj;
        if (batchSize == null) {
            if (other.batchSize != null)
                return false;
        } else if (!batchSize.equals(other.batchSize))
            return false;
        if (clientId == null) {
            if (other.clientId != null)
                return false;
        } else if (!clientId.equals(other.clientId))
            return false;
        if (clientSecret == null) {
            if (other.clientSecret != null)
                return false;
        } else if (!clientSecret.equals(other.clientSecret))
            return false;
        if (refreshToken == null) {
            if (other.refreshToken != null)
                return false;
        } else if (!refreshToken.equals(other.refreshToken))
            return false;
        if (spreadsheetName == null) {
            if (other.spreadsheetName != null)
                return false;
        } else if (!spreadsheetName.equals(other.spreadsheetName))
            return false;
        if (spreadsheetId == null) {
            if (other.spreadsheetId != null)
                return false;
        } else if (!spreadsheetId.equals(other.spreadsheetId))
            return false;
        if (apiVersion == null) {
            if (other.apiVersion != null)
                return false;
        } else if (!apiVersion.equals(other.apiVersion))
            return false;

        if (!Objects.equals(this.spreadsheetMap, other.spreadsheetMap)) {
            return false;
        }
        return true;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getSpreadsheetName() {
        return spreadsheetName;
    }

    public void setSpreadsheetName(String spreadsheetName) {
        this.spreadsheetName = spreadsheetName;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getSpreadsheetId() {
        return spreadsheetId;
    }

    public void setSpreadsheetId(String spreadsheetId) {
        this.spreadsheetId = spreadsheetId;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public Map<String, String> getSpreadsheets() {
        return spreadsheets;
    }

    public String getSpreadsheetMap() {
        return spreadsheetMap;
    }

    public void setSpreadsheetMap(String spreadsheetMap) {
        this.spreadsheetMap = spreadsheetMap;
    }

}
