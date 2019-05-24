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
