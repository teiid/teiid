/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import junit.framework.TestCase;

import com.metamatrix.cdk.unittest.FakeTranslationFactory;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IQuery;

public class TestYahooTranslation extends TestCase {
	
	@Override
	protected void setUp() throws Exception {
		System.setProperty(CoreConstants.NO_CONFIGURATION, "");//$NON-NLS-1$
	}
	
    public void helpTestTranslation(String sql, String expectedUrl) throws Exception {
        ICommand command = FakeTranslationFactory.getInstance().getYahooTranslationUtility().parseCommand(sql);
        
        String url = YahooExecution.translateIntoUrl((IQuery) command);
        assertEquals("Did not get expected url", expectedUrl, url); //$NON-NLS-1$
    }
    
    public void testURLTranslation1() throws Exception {
        helpTestTranslation(
            "SELECT LastTrade FROM Yahoo.QuoteServer WHERE TickerSymbol = 'BA'",  //$NON-NLS-1$
            "http://finance.yahoo.com/d/quotes.csv?s=BA&f=sl1d1t1c1ohgv&e=.csv"); //$NON-NLS-1$
    }

    public void testURLTranslation2() throws Exception {
        helpTestTranslation(
            "SELECT LastTrade FROM Yahoo.QuoteServer WHERE TickerSymbol IN ('BA', 'MON')",  //$NON-NLS-1$
            "http://finance.yahoo.com/d/quotes.csv?s=MON+BA&f=sl1d1t1c1ohgv&e=.csv"); //$NON-NLS-1$
    }
    
    public void testURLTranslation3() throws Exception {
        helpTestTranslation(
            "SELECT LastTrade FROM Yahoo.QuoteServer WHERE TickerSymbol = 'BA' OR TickerSymbol = 'MON'",  //$NON-NLS-1$
            "http://finance.yahoo.com/d/quotes.csv?s=MON+BA&f=sl1d1t1c1ohgv&e=.csv"); //$NON-NLS-1$
    }


}
