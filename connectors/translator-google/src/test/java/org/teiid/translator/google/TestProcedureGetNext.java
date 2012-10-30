package org.teiid.translator.google;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.execution.SpreadsheetProcedureExecution;

public class TestProcedureGetNext {
	Iterator<SheetRow> rowsIterator;
	List<SheetRow> rows = new ArrayList<SheetRow>();
	int counter = 0;

	public TestProcedureGetNext() {
		rowsIterator = new Iterator<SheetRow>() {

			@Override
			public void remove() {
			}

			@Override
			public SheetRow next() {
				if (counter <= rows.size() - 1) {
					return rows.get(counter++);
				} else {
					return null;
				}
			}

			@Override
			public boolean hasNext() {
				if (counter <= rows.size()-1)
					return true;
				else
					return false;
			}
		};
	}

	private void addSheetRow(SheetRow row) {
		rows.add(row);
	}
	@Test
	public void testGetNext() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, DataNotAvailableException, TranslatorException, NoSuchMethodException, InvocationTargetException{

		addSheetRow(new SheetRow(new String[]{"Filip","Elias","123456"}));
		addSheetRow(new SheetRow(new String[]{"Filip","Nguyen",""}));
		addSheetRow(new SheetRow(new String[]{"Peppe","Vagus(\"Hero\")","1"}));
		addSheetRow(new SheetRow(new String[]{"Charlie,Martin\"???\"","","5555"}));
		
		SpreadsheetProcedureExecution exec=new SpreadsheetProcedureExecution(null, null);
		Field privateRowIterator = SpreadsheetProcedureExecution.class.getDeclaredField("rowIterator");
		privateRowIterator.setAccessible(true);
		privateRowIterator.set(exec, rowsIterator);
		Method method = exec.getClass().getDeclaredMethod("initCsv");
        method.setAccessible(true);
        method.invoke(exec,(Object[])null);

		
		Assert.assertEquals("\"Filip\",\"Elias\",\"123456\"\n",exec.next().get(0));
		Assert.assertEquals("\"Filip\",\"Nguyen\",\"\"\n",exec.next().get(0));
		Assert.assertEquals("\"Peppe\",\"Vagus(\"\"Hero\"\")\",\"1\"\n",exec.next().get(0));
		Assert.assertEquals("\"Charlie,Martin\"\"???\"\"\",\"\",\"5555\"\n",exec.next().get(0));
		Assert.assertNull(exec.next());
	}
}
