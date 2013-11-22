package org.teiid.translator.simpledb.executors;

import java.util.HashMap;
import java.util.Map;

import org.teiid.language.Command;
import org.teiid.language.Update;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.visitors.SimpleDBUpdateVisitor;

public class SimpleDBUpdateExecute implements UpdateExecution {
	
	private Command command;
	private SimpleDBConnection connection;
	private int updatedCount=0;
	
	public SimpleDBUpdateExecute(Command command, SimpleDBConnection connection) {
		this.connection = connection;
		this.command = command;
	}
	
	@Override
	public void close() {

	}

	@Override
	public void cancel() throws TranslatorException {

	}

	@Override
	public void execute() throws TranslatorException {
		Update update = (Update) command;
		SimpleDBUpdateVisitor updateVisitor = new SimpleDBUpdateVisitor(update, connection.getAPIClass());
		Map<String, Map<String,String>> items = new HashMap<String, Map<String,String>>();
		for(String itemName : updateVisitor.getItemNames()){
			updatedCount++;
			items.put(itemName, updateVisitor.getAttributes());
		}
		connection.getAPIClass().performUpdate(updateVisitor.getTableName(), items);

	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return new int[] { updatedCount };
	}

}
