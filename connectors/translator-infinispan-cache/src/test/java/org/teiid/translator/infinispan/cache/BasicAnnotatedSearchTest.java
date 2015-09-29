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
package org.teiid.translator.infinispan.cache;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.trades.TradesCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;

/**
 * BasicSearchTest represent a common core set of test that are run for all the
 * configuration/search combinations.
 * 
 * @author vhalbert
 *
 */
public abstract class BasicAnnotatedSearchTest {
	private static int SELECT_STAR_COL_COUNT = TradesCacheSource.NUM_OF_ALL_COLUMNS;

	@Test
	public void testQueryGetAllTrades() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade as T"); //$NON-NLS-1$

		performTest(command, 200, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryGetEQ1Trade() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade as T where TradeID = 1"); //$NON-NLS-1$

		performTest(command, 1, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryGetIn1Trade() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade as T where TradeID in (2, 3)"); //$NON-NLS-1$

		performTest(command, 2, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryCompareEQBoolean() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  Settled = 'false'"); //$NON-NLS-1$

		performTest(command, 100, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryCompareNEBoolean() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  Settled <> 'false'"); //$NON-NLS-1$

		performTest(command, 100, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryIn() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  TradeId in ('1', '2')"); //$NON-NLS-1$

		performTest(command, 2, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryNotIn() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  TradeId not in ('2')"); //$NON-NLS-1$

		performTest(command, 199, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryRangeBetween() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select tradeName, tradeId From Trade_Object.Trade  where  TradeId > 1 and TradeId < 3"); //$NON-NLS-1$

		performTest(command, 1, 2);
	}

	@Test
	public void testQueryRangeAbove() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  TradeId > 1"); //$NON-NLS-1$

		performTest(command, 199, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryRangeBelow() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  TradeId < 99"); //$NON-NLS-1$

		performTest(command, 98, SELECT_STAR_COL_COUNT);
	}

	@Test
	public void testQueryAnd() throws Exception {
		Select command = (Select) VDBUtility.TRANSLATION_UTILITY
				.parseCommand("select * From Trade_Object.Trade  where  TradeId > '1' and Settled = 'false'"); //$NON-NLS-1$

		performTest(command, 99, SELECT_STAR_COL_COUNT);
	}

	protected List<Object> performTest(Select command, int rowcnt, int colCount)
			throws Exception {

		ObjectExecution exec = createExecution(command);

		return performTest(rowcnt, colCount, exec);
	}

	static List<Object> performTest(int rowcnt, int colCount,
			ObjectExecution exec) throws TranslatorException {
		exec.execute();

		List<Object> rows = new ArrayList<Object>();

		int cnt = 0;
		List<Object> row = exec.next();

		while (row != null) {
			rows.add(row);
			assertEquals("column count doesnt match", colCount, row.size());
			++cnt;
			row = exec.next();
		}

		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$

		exec.close();
		return rows;
	}

	protected abstract ObjectExecution createExecution(Select command)
			throws Exception;

}