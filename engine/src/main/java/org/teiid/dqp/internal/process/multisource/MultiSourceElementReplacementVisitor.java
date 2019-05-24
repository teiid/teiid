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

package org.teiid.dqp.internal.process.multisource;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;


public class MultiSourceElementReplacementVisitor extends ExpressionMappingVisitor {

    private String bindingName;
    private QueryMetadataInterface metadata;

    public MultiSourceElementReplacementVisitor(String bindingName, QueryMetadataInterface metadata) {
        super(null);
        this.bindingName = bindingName;
        this.metadata = metadata;
    }

    public Expression replaceExpression(Expression expr) {
        if(expr instanceof ElementSymbol) {
            ElementSymbol elem = (ElementSymbol) expr;
            Object metadataID = elem.getMetadataID();
            try {
                if(metadata.isMultiSourceElement(metadataID)) {
                    Constant bindingConst = new Constant(this.bindingName, DataTypeManager.DefaultDataClasses.STRING);
                    return bindingConst;
                }
            } catch (QueryMetadataException e) {
            } catch (TeiidComponentException e) {
            }
        }

        return expr;
    }

    public static void visit(String bindingName, QueryMetadataInterface metadata, Command processingCommand) {
        MultiSourceElementReplacementVisitor visitor = new MultiSourceElementReplacementVisitor(bindingName, metadata);
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(visitor, PreOrPostOrderNavigator.PRE_ORDER, true);
        nav.setSkipEvaluatable(true);
        processingCommand.acceptVisitor(nav);
    }

}
