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

import java.util.ArrayList;
import java.util.List;

import javax.script.CompiledScript;
import javax.script.ScriptEngine;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.Expression;

public class ObjectTable extends TableFunctionReference {

    public static final String DEFAULT_LANGUAGE = "teiid_script"; //$NON-NLS-1$

    public static class ObjectColumn extends ProjectedColumn {
        private String path;
        private Expression defaultExpression;
        private CompiledScript compiledScript;

        public ObjectColumn(String name, String type, String path, Expression defaultExpression) {
            super(name, type);
            this.path = path;
            this.defaultExpression = defaultExpression;
        }

        protected ObjectColumn() {

        }

        public Expression getDefaultExpression() {
            return defaultExpression;
        }

        public void setDefaultExpression(Expression defaultExpression) {
            this.defaultExpression = defaultExpression;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public CompiledScript getCompiledScript() {
            return compiledScript;
        }

        public void setCompiledScript(CompiledScript compiledScript) {
            this.compiledScript = compiledScript;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof ObjectColumn)) {
                return false;
            }
            ObjectColumn other = (ObjectColumn)obj;
            return EquivalenceUtil.areEqual(this.path, other.path)
                && EquivalenceUtil.areEqual(this.defaultExpression, other.defaultExpression);
        }

        @Override
        public ObjectColumn clone() {
            ObjectColumn clone = new ObjectColumn();
            super.copyTo(clone);
            clone.path = this.path;
            if (this.defaultExpression != null) {
                clone.defaultExpression = (Expression)this.defaultExpression.clone();
            }
            clone.compiledScript = this.compiledScript;
            return clone;
        }
    }

    private List<ObjectColumn> columns = new ArrayList<ObjectColumn>();
    private String rowScript;
    private List<DerivedColumn> passing = new ArrayList<DerivedColumn>();
    private String scriptingLanguage;

    private CompiledScript compiledScript;
    private ScriptEngine scriptEngine;

    public CompiledScript getCompiledScript() {
        return compiledScript;
    }

    public void setCompiledScript(CompiledScript compiledScript) {
        this.compiledScript = compiledScript;
    }

    public String getScriptingLanguage() {
        return scriptingLanguage;
    }

    public void setScriptingLanguage(String scriptingLanguage) {
        this.scriptingLanguage = scriptingLanguage;
    }

    public List<DerivedColumn> getPassing() {
        return passing;
    }

    public void setPassing(List<DerivedColumn> passing) {
        this.passing = passing;
    }

    public String getRowScript() {
        return rowScript;
    }

    public void setRowScript(String query) {
        this.rowScript = query;
    }

    public List<ObjectColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<ObjectColumn> columns) {
        this.columns = columns;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected ObjectTable cloneDirect() {
        ObjectTable clone = new ObjectTable();
        this.copy(clone);
        for (ObjectColumn column : columns) {
            clone.getColumns().add(column.clone());
        }
        if (this.passing != null) {
            for (DerivedColumn col : this.passing) {
                clone.passing.add(col.clone());
            }
        }
        clone.rowScript = this.rowScript;
        clone.compiledScript = this.compiledScript;
        clone.scriptingLanguage = this.scriptingLanguage;
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ObjectTable)) {
            return false;
        }
        ObjectTable other = (ObjectTable)obj;
        return this.columns.equals(other.columns)
            && this.rowScript.equals(other.rowScript)
            && this.passing.equals(other.passing)
            && EquivalenceUtil.areEqual(scriptingLanguage, other.scriptingLanguage);
    }

    public ScriptEngine getScriptEngine() {
        return scriptEngine;
    }

    public void setScriptEngine(ScriptEngine scriptEngine) {
        this.scriptEngine = scriptEngine;
    }

}
