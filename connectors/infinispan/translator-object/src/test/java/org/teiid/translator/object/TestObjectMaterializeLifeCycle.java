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

package org.teiid.translator.object;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Literal;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.simpleMap.SimpleMapCacheExecutionFactory;
import org.teiid.translator.object.testdata.person.PersonCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;

/*
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class TestObjectMaterializeLifeCycle {
	
	private static String STAGE_CACHE_NAME = PersonCacheSource.PERSON_CACHE_NAME + "Stage";
	
	private static ObjectConnection CONNECTION;
	
	private static CacheNameProxy PROXY;
	
	static Map<?, ?> DATA = PersonCacheSource.loadCache();
	
	
    @BeforeClass
    public static void setUp()  {
    	PROXY = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME, STAGE_CACHE_NAME, "aliasCache");
        
		CONNECTION = PersonCacheSource.createConnectionForMaterialization(PROXY);

    }	


	@Test public void testOrder() throws Exception {
		testBefore();
		testAfter();
	}		
	
	
	public void testBefore() throws Exception {

		String nativeQuery = "truncate cache ";
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}
	
	public void testAfter() throws Exception {
		
		String nativeQuery = "swap cache names ";
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);
		
		assertEquals(PROXY.getCacheName(PersonCacheSource.PERSON_CACHE_NAME, CONNECTION),STAGE_CACHE_NAME);
		assertEquals(PROXY.getCacheName(STAGE_CACHE_NAME, CONNECTION), PersonCacheSource.PERSON_CACHE_NAME);

	}	
	

	@Test( expected = TranslatorException.class )
	public void testSwapInvalidx() throws Exception {
		
		String nativeQuery = "swap cache namesx ";
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}

	@Test( expected = TranslatorException.class )
	public void testBeforeInvalidStageCacheName() throws Exception {
		
		String nativeQuery = "truncate cache " + "x";
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	
	
	@Test( expected = TranslatorException.class )
	public void testInvalidBeforeNativeQuery() throws Exception {
		
		String nativeQuery = "truncate caches " + " aliasCacheX";
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	

	@Test( expected = TranslatorException.class )
	public void testInvalidBeforeInvalidNumber3() throws Exception {
		
		String nativeQuery = "truncate cache " + STAGE_CACHE_NAME;
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	


	@Test( expected = TranslatorException.class )
	public void testInvalidBeforeInvalidNumber5() throws Exception {
		
		String nativeQuery = "truncate caches " + PersonCacheSource.PERSON_CACHE_NAME + " " +  STAGE_CACHE_NAME;
		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	
	
	@Test( expected = TranslatorException.class )
	public void testInvalidAfterInvalidNativeQuery1() throws Exception {
		
		String nativeQuery = "swap cachex names ";

		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	

	@Test( expected = TranslatorException.class )
	public void testInvalidAfterInvalidNativeQuery2() throws Exception {
		
		String nativeQuery = "swap cache namesx ";

		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	

	@Test( expected = TranslatorException.class )
	public void testInvalidAfterInvalidNumber5() throws Exception {
		
		String nativeQuery = "swap cache names " + PersonCacheSource.PERSON_CACHE_NAME;

		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	

	@Test( expected = TranslatorException.class )
	public void testInvalidAfterInvalidNumber7() throws Exception {
		
		String nativeQuery = "swap cache names " + PersonCacheSource.PERSON_CACHE_NAME + " " +  STAGE_CACHE_NAME + " " + PersonCacheSource.PERSON_CACHE_NAME;

		List<Argument> args = new ArrayList<Argument>(1);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal(nativeQuery, String.class));

		args.add(arg);
		
		performTest(1, args);

	}	

	protected List<Object> performTest(int rowcnt, List<Argument> args)
			throws TranslatorException {
		
		ObjectDirectExecution exec = createExecution(args);

		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<?> row = exec.next();
	
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
		}
		
		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$
		
		exec.close();
		return rows;
	}
	
	protected List<Object> performTest(int rowcnt, List<Argument> args, List<Object> expectedResults)
			throws TranslatorException {
		
		ObjectDirectExecution exec = createExecution(args);

		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<?> row = exec.next();
	
		while (row != null) {
			rows.add(row);
			
			for (int i=0; i<expectedResults.size(); i++) {
				
				assertEquals("values don't match for row " + cnt + " column " + i, row.get(i), expectedResults.get(i));
				
			}
			
			++cnt;
			row = exec.next();
		}
		
		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$
		
		exec.close();
		return rows;
	}

	protected ObjectDirectExecution createExecution( List<Argument> args) throws TranslatorException {
		ObjectExecutionFactory translator = new SimpleMapCacheExecutionFactory();
		//		ObjectExecutionFactory translator = new ObjectExecutionFactory() {
//			@Override
//			public List<Object> search(Select command, String cacheName,
//					ObjectConnection connection, ExecutionContext executionContext) {
//					List<Object> rows = new ArrayList<Object>(DATA.values());
//        			return rows;
//         	}
//
//        };
        translator.start();

		return (ObjectDirectExecution) translator.createDirectExecution(args, null, Mockito.mock(ExecutionContext.class), VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
