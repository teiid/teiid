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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@SuppressWarnings("nls")
public class TestPrePlanningLeftOuterJoinOptimization {

    public static final String sql = "SELECT \"PRODUIT\".\"_ISIN\" AS \"PRODUIT__ISIN\",\n" +
            "       \"PRODUIT\".\"_CODEDUTYPE\" AS \"PRODUIT__CODEDUTYPE\",\n" +
            "       \"PRODUIT\".\"_DEVISEACTIF\" AS \"PRODUIT__DEVISEACTIF\",\n" +
            "       \"PRODUIT\".\"_CODEEXTERNE10\" AS \"PRODUIT__CODEEXTERNE10\",\n" +
            "       \"PRDTCLASSPYS\".\"_CODEPAYS\" AS \"PRDTCLASSPYS__CODEPAYS\",\n" +
            "       \"PRDTCLASSPYS\".\"_CODESOUSSECTEUR\" AS \"PRDTCLASSPYS__CODESOUSSECTEU\",\n" +
            "       \"PRDTCLASSPYS\".\"_CLASSIFICATION\" AS \"PRDTCLASSPYS__CLASSIFICATION\",\n" +
            "       \"BAAKREP\".\"AKBDTX\" AS \"BAAKREP_AKBDTX\",\n" +
            "       \"BAAKREP\".\"AKBBTX\" AS \"BAAKREP_AKBBTX\"\n" +
            "  FROM \"OmegaModel_PlZX\".\"Omega\".\"Com\".\"PRODUIT\" \"PRODUIT\"\n" +
            "  LEFT JOIN (SELECT \"__LATEST_sub_t\".\"BaseCode\" AS \"__LATEST_RATE\",\n" +
            "                    \"__LATEST_sub_t\".\"CloseRate\"        AS \"__LATEST_RATE1\"\n" +
            "               FROM \"BInvModel_fFHk\".\"Omega\".\"dbo\".\"__LATEST_RATE\" \"__LATEST_sub_t\"\n" +
            "              WHERE ((\"__LATEST_sub_t\".\"QuoteCode\" =\n" +
            "                    'USD'))) \"__LATEST_R_Sub\"\n" +
            "    ON (\"PRODUIT\".\"_DEVISEACTIF\" =\n" +
            "       \"__LATEST_R_Sub\".\"__LATEST_RATE\")\n" +
            "  LEFT JOIN (SELECT \"_SECU_HISTORY_sub_t\".\"CloseRate\" AS \"_SECU_HISTORY_CloseRa\",\n" +
            "                    \"_SECU_HISTORY_sub_t\".\"VolatilityBefCutoff\" AS \"_SECU_HISTORY_Volatil\",\n" +
            "                    \"_SECU_HISTORY_sub_t\".\"VolatilityAfterCutoff\" AS \"_SECU_HISTORY_Volatil1\",\n" +
            "                    \"_SECU_HISTORY_sub_t\".\"Volatility200Days\" AS \"_SECU_HISTORY_Volatil2\",\n" +
            "                    \"_SECU_HISTORY1_SUB\".\"ProductCode\" AS \"_SECU_HISTORY_Product\",\n" +
            "                    MAX(\"_SECU_HISTORY1_SUB\".\"Date\") AS \"_SECU_HISTORY_Date\"\n" +
            "               FROM \"BInvModel_fFHk\".\"Omega\".\"dbo\".\"_SECU_HISTORY\" \"_SECU_HISTORY_sub_t\"\n" +
            "               LEFT JOIN \"BInvModel_fFHk\".\"Omega\".\"dbo\".\"_SECU_HISTORY\" \"_SECU_HISTORY1_SUB\"\n" +
            "                 ON (\"_SECU_HISTORY_sub_t\".\"ProductCode\" =\n" +
            "                    \"_SECU_HISTORY1_SUB\".\"ProductCode\")\n" +
            "              GROUP BY \"_SECU_HISTORY_sub_t\".\"Date\",\n" +
            "                       \"_SECU_HISTORY_sub_t\".\"CloseRate\",\n" +
            "                       \"_SECU_HISTORY_sub_t\".\"VolatilityBefCutoff\",\n" +
            "                       \"_SECU_HISTORY_sub_t\".\"VolatilityAfterCutoff\",\n" +
            "                       \"_SECU_HISTORY_sub_t\".\"Volatility200Days\",\n" +
            "                       \"_SECU_HISTORY1_SUB\".\"ProductCode\"\n" +
            "             HAVING(\"_SECU_HISTORY_sub_t\".\"Date\" = MAX(\"_SECU_HISTORY1_SUB\".\"Date\"))) \"_SECU_HISTORYEkqxr_Sub\"\n" +
            "    ON (\"PRODUIT\".\"_CODEPRODUI\" =\n" +
            "       \"_SECU_HISTORYEkqxr_Sub\".\"_SECU_HISTORY_Product\")\n" +
            "  LEFT JOIN \"OmegaModel_PlZX\".\"Omega\".\"Com\".\"PRDTCLASSPYS\" \"PRDTCLASSPYS\"\n" +
            "    ON (\"PRODUIT\".\"_CODEPRODUI\" = \"PRDTCLASSPYS\".\"_CODEPRODUI\")\n" +
            "  LEFT JOIN \"SAMPLEModel_B8z7\".\"DB2ADMIN\".\"BAAKREP\" \"BAAKREP\"\n" +
            "    ON ((TRIM(\"PRDTCLASSPYS\".\"_CODEPAYS\") = TRIM(\"BAAKREP\".\"AKAICD\")))\n" +
            "  LEFT JOIN \"OmegaModel_PlZX\".\"Omega\".\"Com\".\"PERIODI\" \"PERIODIRateType\"\n" +
            "    ON (\"PRODUIT\".\"_PERIODICITE\" = \"PERIODIRateType\".\"_CODE\") LIMIT 0, 10";


    @Test public void test() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        es.start(new EmbeddedConfiguration());

        LoopbackExecutionFactory ef = new LoopbackExecutionFactory() {
            @Override
            public Object getConnection(Object factory,
                    ExecutionContext executionContext)
                    throws TranslatorException {
                return null;
            }

            @Override
            public boolean supportsOuterJoins() {
                return true;
            }

            @Override
            public boolean supportsHaving() {
                return true;
            }

            @Override
            public boolean supportsAggregatesMax() {
                return true;
            }

            @Override
            public boolean supportsRowLimit() {
                return true;
            }

            @Override
            public boolean supportsInnerJoins() {
                return true;
            }

            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }

            @Override
            public boolean supportsAliasedTable() {
                return true;
            }
        };

        es.addTranslator("x", ef);
        es.addTranslator("y", ef);
        es.addTranslator("z", ef);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("BInvModel_fFHk");
        mmd.addSourceMapping("x", "x", null);
        mmd.addSourceMetadata("ddl", "create foreign table \"Omega.dbo._SECU_HISTORY\" (\"Date\" date, CloseRate double, VolatilityBefCutoff double, VolatilityAfterCutoff double, Volatility200Days double, ProductCode string);"
                + " create foreign table \"Omega.dbo.__LATEST_RATE\" (BaseCode string, QuoteCode string, CloseRate double);");

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("OmegaModel_PlZX");
        mmd1.addSourceMapping("y", "y", null);
        mmd1.addSourceMetadata("ddl", "create foreign table \"OmegaModel_PlZX.Omega.Com.PRODUIT\" (_"
                + "DEVISEACTIF string, _CODEPRODUI string, _ISIN string, _CODEDUTYPE string, _CODEEXTERNE10 string"
                + ", _PERIODICITE string) options (cardinality 0);"
                + " create foreign table \"Omega.Com.PRDTCLASSPYS\" (_CODEPRODUI string,"
                /*missing in old plan*/        + "_CODEPAYS string, _CODESOUSSECTEUR string, _CLASSIFICATION integer ); "
                + " create foreign table \"Omega.Com.PERIODI\" (_CODE string); ");

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("SAMPLEModel_B8z7");
        mmd2.addSourceMapping("z", "z", null);
        mmd2.addSourceMetadata("ddl", "create foreign table \"DB2ADMIN.BAAKREP\" (AKAICD string, AKBDTX string, AKBBTX string);");

        es.deployVDB("vdb", mmd, mmd1, mmd2);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql);
        assertTrue(rs.next());
    }

}
