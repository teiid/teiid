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

package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.Command;
import org.teiid.query.unittest.RealMetadataFactory;

public class TestLanguageBridgeFactory {

    @Before public void setUp() {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
    }
    
    @After public void tearDown() {
        TimestampWithTimezone.resetCalendar(null);
    }
    
    @Test public void testFromUnixtimeRewrite() throws Exception {        
        String input = "select from_unixtime(intnum) from bqt1.Smalla"; //$NON-NLS-1$
        
        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        commandBuilder.getLanguageBridgeFactory().setSupportFromUnixtime(false);
        Command obj = commandBuilder.getCommand(input, true, true);
        assertEquals("SELECT timestampadd(SQL_TSI_SECOND, g_0.IntNum, {ts '1969-12-31 18:00:00.0'}) FROM SmallA AS g_0", obj.toString()); //$NON-NLS-1$
    }
    
}
