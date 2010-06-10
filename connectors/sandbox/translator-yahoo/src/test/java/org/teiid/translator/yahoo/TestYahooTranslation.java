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

package org.teiid.translator.yahoo;

import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.translator.yahoo.YahooExecution;

import junit.framework.TestCase;


public class TestYahooTranslation extends TestCase {
	
    public void helpTestTranslation(String sql, String expectedUrl) throws Exception {
        Command command = FakeTranslationFactory.getInstance().getYahooTranslationUtility().parseCommand(sql);
        
        String url = YahooExecution.translateIntoUrl((Select) command);
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
