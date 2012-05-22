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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.util.ResultSetUtil;


public abstract class BaseObjectTest  {
	
    protected static boolean REPLACE_EXPECTED = false;
    protected static boolean WRITE_ACTUAL_RESULTS_TO_FILE = false;
    protected static boolean PRINT_RESULTSETS_TO_CONSOLE = false;
    
    private static final int MAX_COL_WIDTH = 65;
    
	protected static boolean print = false;    
    
    public static void writeActualResultsToFile(boolean write) {
    	WRITE_ACTUAL_RESULTS_TO_FILE = write;
    }

	
    public static void compareResultSet(ResultSet... rs) throws IOException, SQLException {
    	StackTraceElement ste = new Exception().getStackTrace()[1];
    	String testName = ste.getMethodName();
    	String className = ste.getClassName();
    	className = className.substring(className.lastIndexOf('.') + 1);
    	testName = className + "/" + testName; //$NON-NLS-1$
        compareResultSet(testName, rs);
    }

	public static void compareResultSet(String testName, ResultSet... rs)
			throws FileNotFoundException, SQLException, IOException {
		FileOutputStream actualOut = null;
        BufferedReader expectedIn = null;
        PrintStream stream = null;
        try {
	        if (REPLACE_EXPECTED) {
	            File actual = new File(UnitTestUtil.getTestDataPath() + "/" +testName+".expected"); //$NON-NLS-1$ //$NON-NLS-2$
	            actualOut = new FileOutputStream(actual);
	        } else {
	            if (WRITE_ACTUAL_RESULTS_TO_FILE) {
	                File actual = new File(UnitTestUtil.getTestDataPath() + "/" +testName+".actual"); //$NON-NLS-1$ //$NON-NLS-2$
	                if (!actual.getParentFile().exists()) {
	                	actual.getParentFile().mkdir();
	                }
	                actualOut = new FileOutputStream(actual);


	            } else {
	            	File expected = new File(UnitTestUtil.getTestDataPath() + "/"+testName+".expected"); //$NON-NLS-1$ //$NON-NLS-2$
	            	expectedIn = new BufferedReader(new FileReader(expected));
	            }

	            
	        }
	        PrintStream defaultStream = null;
	        if (PRINT_RESULTSETS_TO_CONSOLE) {
	            defaultStream = new PrintStream(System.out) {
	                // SYS.out should be protected from being closed.
	                public void close() {}
	            };
	        }
	        stream = ResultSetUtil.getPrintStream(actualOut, expectedIn, defaultStream);
	        for (int i = 0; i < rs.length; i++) {
	        	ResultSetUtil.printResultSet(rs[i], MAX_COL_WIDTH, true, stream);
	        }
	        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                    Collections.EMPTY_LIST,
                    ResultSetUtil.getUnequalLines(stream));
        } finally {
	        if (stream != null) {
	        	stream.close();
	        }
	        if (actualOut != null) {
	            actualOut.close();
	        }
	        if (expectedIn != null) {
	            expectedIn.close();
	        }
        }
	}
	
    public static void compareResultSet(List<Object> rs) throws IOException, SQLException {
    	StackTraceElement ste = new Exception().getStackTrace()[1];
    	String testName = ste.getMethodName();
    	String className = ste.getClassName();
    	className = className.substring(className.lastIndexOf('.') + 1);
    	testName = className + "/" + testName; //$NON-NLS-1$
        compareResultSet(testName, rs);
    }

	
	public static void compareResultSet(String testName, List<Object> rs)
			throws FileNotFoundException, SQLException, IOException {
		FileOutputStream actualOut = null;
		BufferedReader expectedIn = null;
		PrintStream stream = null;
		try {
			if (REPLACE_EXPECTED) {
				File actual = new File(UnitTestUtil.getTestDataPath()
						+ "/" + testName + ".expected"); //$NON-NLS-1$ //$NON-NLS-2$
				actualOut = new FileOutputStream(actual);
			} else {
				if (WRITE_ACTUAL_RESULTS_TO_FILE) {
					File actual = new File(UnitTestUtil.getTestDataPath()
							+ "/" + testName + ".actual"); //$NON-NLS-1$ //$NON-NLS-2$
					if (!actual.getParentFile().exists()) {
						actual.getParentFile().mkdir();
					}
					actualOut = new FileOutputStream(actual);
					
//					ObjectOutput oo = null;
//					oo = new ObjectOutputStream(actualOut);
					
						for (int i = 0; i < rs.size(); i++) {
							List<Object> r = (List) rs.get(i);
							actualOut.write( new String("ROW_" + i).getBytes());
							for (Iterator it=r.iterator(); it.hasNext();) {
								Object o = it.next();
								actualOut.write( o.toString().getBytes());
								actualOut.write( new String("\t").getBytes());
							}
								//ExternalizeUtil.writeList(oo, rs[i]);
						}
										
					return;
					

				} else {
					File expected = new File(UnitTestUtil.getTestDataPath()
							+ "/" + testName + ".expected"); //$NON-NLS-1$ //$NON-NLS-2$
					expectedIn = new BufferedReader(new FileReader(expected));
				}

			}
			PrintStream defaultStream = null;
			if (PRINT_RESULTSETS_TO_CONSOLE) {
				defaultStream = new PrintStream(System.out) {
					// SYS.out should be protected from being closed.
					public void close() {
					}
				};
			}
			stream = ResultSetUtil.getPrintStream(actualOut, expectedIn,
					defaultStream);
//	        for (int i = 0; i < rs.length; i++) {
//	        	ResultSetUtil.printResultSet(rs[i], MAX_COL_WIDTH, true, stream);
//	        }
			assertEquals(
					"Actual data did not match expected", //$NON-NLS-1$
					Collections.EMPTY_LIST,
					ResultSetUtil.getUnequalLines(stream));
		} finally {
			if (stream != null) {
				stream.close();
			}
			if (actualOut != null) {
				actualOut.close();
			}
			if (expectedIn != null) {
				expectedIn.close();
			}
		}
	}
	
	protected void printRow(int rownum, List<?> row) {
		if (!print) return;
		if (row == null) {
			System.out.println("Row " + rownum + " is null");
			return;
		}
		int i = 0;
		for(Object o:row) {
			System.out.println("Row " + rownum + " Col " + i + " - " + o.toString());
			++i;
		}
		
	}
	
}
