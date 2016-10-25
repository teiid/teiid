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
