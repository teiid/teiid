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

package org.teiid.query.optimizer;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestNestedUnionPlanning {

    static String DDL = "CREATE FOREIGN TABLE visits\n"
            + "(\n"
            + "  channel_id varchar(1048000),\n"
            + "  channel_id_short varchar(1048000),\n"
            + "  campaign varchar(1048000),\n"
            + "  channel varchar(1048000),\n"
            + "  medium varchar(1048000),\n"
            + "  source varchar(1048000),\n"
            + "  keyword varchar(1048000),\n"
            + "  adgroup varchar(1048000),\n"
            + "  channel_category varchar(1048000)\n"
            + ");"
            + "CREATE FOREIGN TABLE adcosts\n"
            + "(\n"
            + "  channel_id varchar(1048000),\n"
            + "  channel_id_short varchar(1048000),\n"
            + "  campaign varchar(1048000),\n"
            + "  channel varchar(1048000),\n"
            + "  medium varchar(1048000),\n"
            + "  source varchar(1048000),\n"
            + "  keyword varchar(1048000),\n"
            + "  adgroup varchar(1048000),\n"
            + "  channel_category varchar(1048000)\n"
            + ");\n"
            + "\n"
            + "CREATE FOREIGN TABLE email_campaigns\n"
            + "(\n"
            + "  channel_id varchar(1048000),\n"
            + "  channel_id_short varchar(1048000),\n"
            + "  campaign varchar(1048000),\n"
            + "  channel varchar(1048000),\n"
            + "  medium varchar(1048000),\n"
            + "  source varchar(1048000),\n"
            + "  keyword varchar(1048000),\n"
            + "  adgroup varchar(1048000),\n"
            + "  channel_category varchar(1048000)\n"
            + ");\n"
            + "\n"
            + "CREATE FOREIGN TABLE for_view\n"
            + "(\n"
            + "  channel_id varchar(1048000),\n"
            + "  channel_id_short varchar(1048000),\n"
            + "  campaign varchar(1048000),\n"
            + "  channel varchar(1048000),\n"
            + "  medium varchar(1048000),\n"
            + "  source varchar(1048000),\n"
            + "  keyword varchar(1048000),\n"
            + "  adgroup varchar(1048000),\n"
            + "  channel_category varchar(1048000)\n"
            + ");\n"
            + "\n"
            + "CREATE FOREIGN TABLE transaction_channel\n"
            + "(\n"
            + "  channel_id varchar(1048000),\n"
            + "  channel_id_short varchar(1048000),\n"
            + "  campaign varchar(1048000),\n"
            + "  channel varchar(1048000),\n"
            + "  medium varchar(1048000),\n"
            + "  source varchar(1048000),\n"
            + "  keyword varchar(1048000),\n"
            + "  adgroup varchar(1048000),\n"
            + "  channel_category varchar(1048000)\n"
            + ");\n"
            + "CREATE VIRTUAL PROCEDURE campaign_to_subchannel1() RETURNS\n"
            + "          (\n"
            + "         subchannel string\n"
            + "          )\n"
            + "          AS BEGIN\n"
            + "          END      ";

    static String query = " SELECT\n"
            + "        *\n"
            + "    FROM\n"
            + "        (\n"
            + "            SELECT\n"
            + "                    email.campaign AS channel_id\n"
            + "                FROM\n"
            + "                    email_campaigns AS email\n"
            + "                    ,TABLE (\n"
            + "                        call campaign_to_subchannel1 ()\n"
            + "                    ) email_channels\n"
            + "            UNION\n"
            + "            ALL SELECT\n"
            + "                    shop.channel_id AS channel_hash\n"
            + "                FROM\n"
            + "                    for_view AS shop\n"
            + "            UNION\n"
            + "            ALL SELECT\n"
            + "                    medium as channel_id\n"
            + "                FROM\n"
            + "                    \"transaction_channel\"\n"
            + "            UNION\n"
            + "            ALL SELECT\n"
            + "                    md5_hash as channel_id\n"
            + "                FROM\n"
            + "                    (\n"
            + "                        SELECT\n"
            + "                                        medium AS md5_hash\n"
            + "                            FROM\n"
            + "                                \"visits\"\n"
            + "                        UNION\n"
            + "                        SELECT\n"
            + "                                medium md5_hash\n"
            + "                            FROM\n"
            + "                                \"adcosts\"\n"
            + "                    ) tracked_channels\n"
            + "        ) AS channel_definition";

    @Test public void testTeiid6080() throws Exception {
        List[] expected = new List[] {
            Arrays.asList("xxx"),
        };

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_3.channel_id AS c_0 FROM y.for_view AS g_3 UNION ALL SELECT g_2.medium AS c_0 FROM y.transaction_channel AS g_2 UNION ALL (SELECT g_1.medium AS c_0 FROM y.visits AS g_1 UNION SELECT g_0.medium AS c_0 FROM y.adcosts AS g_0)", Arrays.asList("xxx"));
        hdm.addData("SELECT g_0.campaign FROM y.email_campaigns AS g_0");

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(DDL, "x", "y");

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(query, metadata, new String[] {
                "SELECT g_3.channel_id AS c_0 FROM y.for_view AS g_3 UNION ALL SELECT g_2.medium AS c_0 FROM y.transaction_channel AS g_2 UNION ALL (SELECT g_1.medium AS c_0 FROM y.visits AS g_1 UNION SELECT g_0.medium AS c_0 FROM y.adcosts AS g_0)",
                "SELECT g_0.campaign FROM y.email_campaigns AS g_0"
        }, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);

        CommandContext cc = TestProcessor.createCommandContext();
        TestProcessor.helpProcess(plan, cc, hdm, expected);
    }


}
