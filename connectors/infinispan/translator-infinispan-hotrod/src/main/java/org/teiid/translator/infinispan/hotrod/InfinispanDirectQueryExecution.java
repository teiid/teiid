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
import org.teiid.infinispan.api.InfinispanPlugin;
import org.teiid.infinispan.api.ProtobufMetadataProcessor;
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

        Matcher m = truncatePattern.matcher(command);
        if (m.matches()) {
            String tableName =  m.group(1);
            clearContents(tableName);
            return;
        }

        throw new TranslatorException(InfinispanPlugin.Event.TEIID25016,
                InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25016, command));
    }

    private void clearContents(String tableName) throws TranslatorException {
        Table table = metadata.getTable(tableName);
        String cacheName = ProtobufMetadataProcessor.getCacheName(table);
        BasicCache<Object, Object> cache = connection.getCache(cacheName);
        if (cache == null) {
            throw new TranslatorException(InfinispanPlugin.Event.TEIID25014,
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25014, tableName));
        }
        cache.clear();
    }

    static String fqn(ExecutionContext context, String key) {
        return context.getVdbName()+"."+context.getVdbVersion()+"."+key;
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
