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

    private Tracer tracer;

    private static TeiidTracingUtil INSTANCE = new TeiidTracingUtil();

    public static TeiidTracingUtil getInstance() {
        return INSTANCE;
    }

    /**
     * For use by tests - GlobalTracer is not directly test friendly as the registration can only happen once.
     */
    void setTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    /**
     * Build a {@link Span} from the {@link CommandLogMessage} and incoming span context
     * @param msg
     * @param spanContextJson
     * @return
     */
    public Span buildSpan(Options options, CommandLogMessage msg, String spanContextJson) {
        if (!isTracingEnabled(options, spanContextJson)) {
            return null;
        }

        Tracer.SpanBuilder spanBuilder = getTracer()
                .buildSpan("USER COMMAND") //$NON-NLS-1$
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER);

        if (spanContextJson != null) {
            SpanContext parent = extractSpanContext(spanContextJson);
            if (parent != null) {
                spanBuilder.asChildOf(parent);
            } else if (options.isTracingWithActiveSpanOnly()) {
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
     * Return true if tracing is enabled.
     *
     * Both arguments may be null, in which case true will be returned only if there is an active span
     * @param options
     * @param spanContextJson
     * @return
     */
    public boolean isTracingEnabled(Options options, String spanContextJson) {
        boolean withActiveSpanOnly = options == null?true:options.isTracingWithActiveSpanOnly();
        return !withActiveSpanOnly || getTracer().activeSpan() != null || spanContextJson != null;
    }

    /**
     * Build a {@link Span} from the {@link CommandLogMessage} and translator type
     * @param msg
     * @param translatorType
     * @return
     */
    public Span buildSourceSpan(CommandLogMessage msg, String translatorType) {
        Tracer tr = getTracer();
        if (tr.activeSpan() == null) {
            return null;
        }

        Tracer.SpanBuilder spanBuilder = tr
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
        Tracer tr = getTracer();
        if (tr.activeSpan() == span) {
            //when a workitem adds itself to a queue the span will already be active
            return null;
        }
        return tr.scopeManager().activate(span, false);
    }

    private Tracer getTracer() {
        if (tracer != null) {
            return tracer;
        }
        return GlobalTracerInjector.getTracer();
    }

    protected SpanContext extractSpanContext(String spanContextJson) {
        try {
            JSONParser parser = new JSONParser();
            SimpleContentHandler sch = new SimpleContentHandler();
            parser.parse(spanContextJson, sch);
            Map<String, String> result = (Map<String, String>) sch.getResult();
            return getTracer().extract(Builtin.TEXT_MAP, new TextMapExtractAdapter(result));
        } catch (IllegalArgumentException | ClassCastException | ParseException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, e, "Could not extract the span context"); //$NON-NLS-1$
            return null;
        }
    }

}