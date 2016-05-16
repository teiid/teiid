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

package org.teiid.translator.jdbc.pi;

import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestPIExecutionFactory {
	
	private static PIExecutionFactory TRANSLATOR;
	
	@BeforeClass
	public static void setup() throws TranslatorException {
		TRANSLATOR = new PIExecutionFactory();
		TRANSLATOR.start();
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT")); 
	}
	
	@AfterClass
	public static void tearDown() {
		TimestampWithTimezone.resetCalendar(null);
	}
	
	@Test public void testDateFormats() throws TranslatorException {
		String input = "SELECT stringkey FROM BQT1.MediumA where datevalue < '2001-01-01' and timevalue < '12:11:01' and timestampvalue < '2012-02-03 11:12:13'"; //$NON-NLS-1$
		String output = "SELECT MediumA.StringKey FROM MediumA WHERE MediumA.DateValue < '2001-01-01' AND MediumA.TimeValue < '12:11:01' AND MediumA.TimestampValue < '2012-02-03 11:12:13.0'"; //$NON-NLS-1$
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
	}

}
