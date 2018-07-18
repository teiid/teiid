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

/*
 */

package org.teiid.translator.infinispan.hotrod;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commons.api.BasicCache;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;


public class InfinispanDirectQueryExecution implements ProcedureExecution {

	private static Pattern truncatePattern = Pattern.compile("truncate (\\S+)");
	private static Pattern renamePattern = Pattern.compile("rename (\\S+)\\s+(\\S+)");
	
    protected int columnCount;
    private List<Argument> arguments;
    protected int updateCount = -1;
    private InfinispanConnection connection;
    private ExecutionContext context;
    private RuntimeMetadata metadata;

	public InfinispanDirectQueryExecution(List<Argument> arguments, Command command, ExecutionContext context,
			RuntimeMetadata metadata, InfinispanConnection connection) {
        this.arguments = arguments;
        this.connection = connection;
        this.context = context;
        this.metadata = metadata;
    }
    
    @Override
    public void execute() throws TranslatorException {
    	String command = (String) this.arguments.get(0).getArgumentValue().getValue();
    	BasicCache<String, String> aliasCache = getAliasCache(this.connection);
    	
    	Matcher m = truncatePattern.matcher(command);
    	if (m.matches()) {
    		String tableName =  m.group(1);
    		clearContents(aliasCache, tableName);
    		return;
    	} 
        
    	m = renamePattern.matcher(command);
    	if (m.matches()) {
    		String tableOne = m.group(1);
    		String tableTwo = m.group(2);
    		
    		String aliasName = getAliasName(context, aliasCache, tableOne);
    		if (aliasName.equals(tableOne)) {
    			aliasCache.put(fqn(context, tableTwo), tableOne);
    			aliasCache.put(fqn(context, tableOne), tableTwo);
    		} else if (aliasName.equals(tableTwo)) {
    			aliasCache.put(fqn(context, tableOne), tableOne);
    			aliasCache.put(fqn(context, tableTwo), tableTwo);
    		} else {
    			throw new TranslatorException(InfinispanPlugin.Event.TEIID25015,
    					InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25015, tableOne, aliasName));
    		}
    		return;
    	}
    	
		throw new TranslatorException(InfinispanPlugin.Event.TEIID25016,
				InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25016, command));
    }

	private void clearContents(BasicCache<String, String> aliasCache, String tableName) throws TranslatorException {
		tableName = getAliasName(context, aliasCache, tableName);
		Table table = metadata.getTable(tableName);
		String cacheName = ProtobufMetadataProcessor.getCacheName(table);
		BasicCache<Object, Object> cache = connection.getCache(cacheName, false);
		if (cache == null) {
			throw new TranslatorException(InfinispanPlugin.Event.TEIID25014,
					InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25014, tableName));
		}
		cache.clear();
	}

	static String getAliasName(ExecutionContext context, BasicCache<String, String> aliasCache, String alias)
			throws TranslatorException {
		String key = fqn(context, alias);
		String value = aliasCache.get(key);
		if (value != null) {
			return value;
		}
		return alias;
	}

	static Table getAliasTable(ExecutionContext context, RuntimeMetadata metadata,
			BasicCache<String, String> aliasCache, Table table) throws TranslatorException {
		String alias = table.getFullName();
		String key = fqn(context, alias);
		String value = aliasCache.get(key);
		if (value != null) {
			alias = value.substring(value.lastIndexOf('.')+1);
		} else {
			alias = alias.substring(alias.lastIndexOf('.')+1);
		}
		return metadata.getTable(table.getParent().getName(), alias);
	}
	
	static String fqn(ExecutionContext context, String key) {
		return context.getVdbName()+"."+context.getVdbVersion()+"."+key;
	}

	static BasicCache<String, String> getAliasCache(InfinispanConnection connection) throws TranslatorException {
		BasicCache<String, String> cache = connection.getCache(InfinispanExecutionFactory.TEIID_ALIAS_NAMING_CACHE,
				true);
		if (cache == null) {
			throw new TranslatorException(InfinispanPlugin.Event.TEIID25014, InfinispanPlugin.Util
					.gs(InfinispanPlugin.Event.TEIID25014, InfinispanExecutionFactory.TEIID_ALIAS_NAMING_CACHE));
		}
		return cache;
	}

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return null;
    }
    
	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;  //could support as an array of output values via given that the native procedure returns an array value
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
}
