/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import junit.framework.TestCase;

public class TestRequestFormation extends TestCase {
	
	
	public void testHandlingMultiValues() {
		java.util.ArrayList list = new java.util.ArrayList();
		String[] baseArr = new String[5];
		String[] first = {"first"};
		String[] second = {"second"};
		String[] newArr = {"third1","third2","third3"};
		String[] arrFour = {"fourth"};
		String[] arrFifth = {"fifth1","fifth2"};
		java.util.ArrayList newParams = new java.util.ArrayList();
		newParams.add(first);
		newParams.add(second);
		newParams.add(newArr);
		newParams.add(arrFour);
		newParams.add(arrFifth);
		list.add(baseArr);

		for(int i = 0; i < newParams.size(); i++) {
			String[] values = (String[]) newParams.get(i);
			if(values.length == 1) {
				java.util.Iterator valIter = list.iterator();
				while(valIter.hasNext()) {
					String[] valArr = (String[]) valIter.next();
					valArr[i] = values[0];
				}
			} else {
				//cannot use iterator - causes concurrent mod exception
				int numQueries = list.size();
				for(int j = 0; j < numQueries; j++) {
					String[] currentQuery = (String[]) list.get(j);
					for(int k = 1; k < values.length; k++) {
						String[] newArray = new String[5];
						System.arraycopy(currentQuery, 0, newArray, 0, currentQuery.length);
						newArray[i] = values[k];
						list.add(newArray);
					}
					currentQuery[i] = values[0];
				}			
			}
		}

		java.util.Iterator iter = list.iterator();
		while(iter.hasNext()) {
			String[] outArray = (String[]) iter.next();
			for(int i = 0; i < outArray.length; i++) {
				System.out.print(outArray[i] + ";");
			}
			System.out.print("\n");
		}
	}

}
