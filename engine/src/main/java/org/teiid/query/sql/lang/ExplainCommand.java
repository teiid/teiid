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

package org.teiid.query.sql.lang;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;

public class ExplainCommand extends Command {

    public enum Format {
        TEXT,
        XML,
        YAML
    }

    private Format format;
    private Boolean analyze;
    private Command command;

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int getType() {
        return Command.TYPE_EXPLAIN;
    }

    @Override
    public ExplainCommand clone() {
        ExplainCommand clone = new ExplainCommand();
        clone.setFormat(format);
        clone.setAnalyze(analyze);
        if (command != null) {
            clone.setCommand((Command) command.clone());
        }
        return clone;
    }

    @Override
    public List<Expression> getProjectedSymbols() {
        Class<?> type = DefaultDataClasses.CLOB;
        if (format == Format.XML) {
            type = DefaultDataClasses.XML;
        }
        ElementSymbol symbol = new ElementSymbol("QUERY PLAN"); //$NON-NLS-1$
        symbol.setMetadataID(new TempMetadataID("QUERY PLAN", type)); //$NON-NLS-1$
        symbol.setType(type);
        return Arrays.asList(symbol);
    }

    @Override
    public boolean areResultsCachable() {
        return false;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public boolean isNoExec() {
        return analyze == null || !analyze;
    }

    public Boolean getAnalyze() {
        return analyze;
    }

    public void setAnalyze(Boolean analyze) {
        this.analyze = analyze;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyze, command, format);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ExplainCommand other = (ExplainCommand) obj;
        return Objects.equals(analyze, other.analyze)
        && Objects.equals(command, other.command)
        && Objects.equals(format, other.format);
    }

    @Override
    public Command getActualCommand() {
        return command;
    }

    @Override
    public boolean returnsResultSet() {
        return true;
    }

}
