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

package org.teiid.query.validator;

import java.util.Iterator;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;


public class Validator {

    public static final ValidatorReport validate(LanguageObject object, QueryMetadataInterface metadata) throws TeiidComponentException {
        ValidatorReport report1 = Validator.validate(object, metadata, new ValidationVisitor());
        return report1;
    }

    public static final ValidatorReport validate(LanguageObject object, QueryMetadataInterface metadata, AbstractValidationVisitor visitor)
        throws TeiidComponentException {

        // Execute on this command
        executeValidation(object, metadata, visitor);

        // Construct combined runtime / query metadata if necessary
        if(object instanceof Command) {
            // Recursively validate subcommands
            Iterator<Command> iter = CommandCollectorVisitor.getCommands((Command)object).iterator();
            while(iter.hasNext()) {
                Command subCommand = iter.next();
                validate(subCommand, metadata, visitor);
            }
        }

        // Otherwise, return a report
        return visitor.getReport();
    }

    private static final void executeValidation(LanguageObject object, final QueryMetadataInterface metadata, final AbstractValidationVisitor visitor)
        throws TeiidComponentException {

        // Reset visitor
        visitor.reset();

        visitor.setMetadata(metadata);
        setTempMetadata(metadata, visitor, object);

        PreOrderNavigator nav = new PreOrderNavigator(visitor) {

            protected void visitNode(LanguageObject obj) {
                QueryMetadataInterface previous = visitor.getMetadata();
                setTempMetadata(metadata, visitor, obj);
                super.visitNode(obj);
                visitor.setMetadata(previous);
            }

            @Override
            protected void preVisitVisitor(LanguageObject obj) {
                super.preVisitVisitor(obj);
                visitor.stack.add(obj);
            }

            @Override
            protected void postVisitVisitor(LanguageObject obj) {
                visitor.stack.pop();
            }

        };
        object.acceptVisitor(nav);

        // If an error occurred, throw an exception
        TeiidComponentException e = visitor.getException();
        if(e != null) {
            throw e;
        }
    }

    private static void setTempMetadata(final QueryMetadataInterface metadata,
            final AbstractValidationVisitor visitor,
            LanguageObject obj) {
        if (obj instanceof Command) {
            Command command = (Command)obj;
            visitor.currentCommand = command;
            TempMetadataStore tempMetadata = command.getTemporaryMetadata();
            if(tempMetadata != null && !tempMetadata.getData().isEmpty()) {
                visitor.setMetadata(new TempMetadataAdapter(metadata, tempMetadata));
            }
        }
    }

}
