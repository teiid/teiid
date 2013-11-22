package org.teiid.translator.simpledb.executors;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.visitors.SimpleDBDeleteVisitor;

public class SimpleDBDeleteExecute implements UpdateExecution {
	
	private Command command;
	private SimpleDBConnection connection;
	private int updatedCount=0;
	
	public SimpleDBDeleteExecute(Command command, SimpleDBConnection connection) {
		this.command = command;
		this.connection = connection;
	}

	@Override
	public void close() {

	}

	@Override
	public void cancel() throws TranslatorException {

	}

	
	@Override
	public void execute() throws TranslatorException {
		Delete delete = (Delete) command;
		SimpleDBDeleteVisitor visitor = new SimpleDBDeleteVisitor(delete, connection.getAPIClass());
		if (visitor.hasWhere()){
			if (visitor.isSimpleDelete()){
				connection.getAPIClass().performDelete(visitor.getTableName(), visitor.getItemName());
				updatedCount = 1;
			}else{
				for (String itemName : visitor.getItemNames()) {
					connection.getAPIClass().performDelete(visitor.getTableName(), itemName);
				}
				updatedCount = visitor.getItemNames().size();
			}
		}else{
			List<List<String>> result = connection.getAPIClass().performSelect("SELECT itemName() FROM "+visitor.getTableName(), Arrays.asList("itemName()"));
			updatedCount = result.size();
			for (List<String> list : result) {
				String itemName = list.get(0);
				connection.getAPIClass().performDelete(visitor.getTableName(), itemName);
			}
		}

	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return new int[] { updatedCount };
	}

}
