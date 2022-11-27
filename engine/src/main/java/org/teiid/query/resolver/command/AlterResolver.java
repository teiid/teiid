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

package org.teiid.query.resolver.command;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Alter;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.Command;

public class AlterResolver implements CommandResolver {

    @Override
    public void resolveCommand(Command command, TempMetadataAdapter metadata,
            boolean resolveNullLiterals) throws QueryMetadataException,
            QueryResolverException, TeiidComponentException {
        Alter<? extends Command> alter = (Alter<? extends Command>)command;
        ResolverUtil.resolveGroup(alter.getTarget(), metadata);
        int type = Command.TYPE_QUERY;
        boolean viewTarget = true;
        if (alter instanceof AlterTrigger) {
            TriggerEvent event = ((AlterTrigger)alter).getEvent();
            switch (event) {
            case DELETE:
                type = Command.TYPE_DELETE;
                break;
            case INSERT:
                type = Command.TYPE_INSERT;
                break;
            case UPDATE:
                type = Command.TYPE_UPDATE;
                break;
            }
            if (((AlterTrigger)alter).isAfter()) {
                viewTarget = false;
            }
        } else if (alter instanceof AlterProcedure) {
            type = Command.TYPE_STORED_PROCEDURE;
            viewTarget = false;
        }
        if (viewTarget && !QueryResolver.isView(alter.getTarget(), metadata)) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30116, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30116, alter.getTarget()));
        }
        if (alter.getDefinition() != null) {
            QueryResolver.resolveCommand(alter.getDefinition(), alter.getTarget(), type, metadata.getDesignTimeMetadata(), false);
        }
    }

}
