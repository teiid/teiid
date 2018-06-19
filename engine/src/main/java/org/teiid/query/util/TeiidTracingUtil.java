/*
 * Copyright 2017-2018 The OpenTracing Authors
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.teiid.query.util;

import java.util.Map;

import org.teiid.jdbc.tracing.GlobalTracerInjector;
import org.teiid.json.simple.JSONParser;
import org.teiid.json.simple.ParseException;
import org.teiid.json.simple.SimpleContentHandler;
import org.teiid.logging.CommandLogMessage;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.tag.Tags;

public class TeiidTracingUtil {

    private boolean withActiveSpanOnly;
    private Tracer tracer = GlobalTracerInjector.getTracer();
    
    private static TeiidTracingUtil INSTANCE = new TeiidTracingUtil();

    public static TeiidTracingUtil getInstance() {
        INSTANCE.tracer = GlobalTracerInjector.getTracer();
        return INSTANCE;
    }
    
    /**
     * For use by tests - GlobalTracer is not directly test friendly as the registration can only happen once.
     */
    void setTracer(Tracer tracer) {
        this.tracer = tracer;
    }
    
    public void setWithActiveSpanOnly(boolean withActiveSpanOnly) {
        this.withActiveSpanOnly = withActiveSpanOnly;
    }
    
    /**
     * Build a {@link Span} from the {@link CommandLogMessage} and incoming span context
     * @param message
     * @param spanContextJson
     * @return
     */
    public Span buildSpan(CommandLogMessage msg, String spanContextJson) {
        if (withActiveSpanOnly && tracer.activeSpan() == null && spanContextJson == null) {
            return null;
        } 

        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan("USER COMMAND") //$NON-NLS-1$
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER);

        if (spanContextJson != null) {
            SpanContext parent = extractSpanContext(spanContextJson);
            if (parent != null) {
                spanBuilder.asChildOf(parent);
            } else if (withActiveSpanOnly) {
                return null;
            }
        }
        
        Span span = spanBuilder.start();
        
        Tags.COMPONENT.set(span, "java-teiid"); //$NON-NLS-1$
        
        Tags.DB_STATEMENT.set(span, msg.getSql());
        Tags.DB_TYPE.set(span, "teiid"); //$NON-NLS-1$
        Tags.DB_INSTANCE.set(span, msg.getVdbName());
        Tags.DB_USER.set(span, msg.getPrincipal());
        
        span.setTag("teiid-session", msg.getSessionID()); //$NON-NLS-1$
        span.setTag("teiid-request", msg.getRequestID()); //$NON-NLS-1$
        
        return span;
    }
    
    /**
     * Build a {@link Span} from the {@link CommandLogMessage} and translator type
     * @param msg
     * @param translatorType
     * @return
     */
    public Span buildSourceSpan(CommandLogMessage msg, String translatorType) {
        if (tracer.activeSpan() == null) {
            return null;
        } 

        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan("SRC COMMAND") //$NON-NLS-1$
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

        Span span = spanBuilder.start();
        
        Tags.COMPONENT.set(span, "java-teiid-connector"); //$NON-NLS-1$
        
        Tags.DB_STATEMENT.set(span, msg.getSql());
        Tags.DB_TYPE.set(span, translatorType); 
        Tags.DB_USER.set(span, msg.getPrincipal());
        
        span.setTag("teiid-source-request", msg.getSourceCommandID()); //$NON-NLS-1$
        
        return span;
    }
    
    public Scope activateSpan(Span span) {
        if (tracer.activeSpan() == span) {
            //when a workitem adds itself to a queue the span will already be active
            return null;
        }
        return tracer.scopeManager().activate(span, false);
    }
    
    protected SpanContext extractSpanContext(String spanContextJson) {
        try {
            JSONParser parser = new JSONParser();
            SimpleContentHandler sch = new SimpleContentHandler();
            parser.parse(spanContextJson, sch);
            Map<String, String> result = (Map<String, String>) sch.getResult();
            return tracer.extract(Builtin.TEXT_MAP, new TextMapExtractAdapter(result));
        } catch (IllegalArgumentException | ClassCastException | ParseException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, e, "Could not extract the span context"); //$NON-NLS-1$
            return null;
        }
    }

}