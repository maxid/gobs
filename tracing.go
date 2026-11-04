package gobs

import (
	"context"
	"net/http"

	"github.com/maxid/gnety/codec/coap"
)

// Attribute is one span attribute. Values are strings to keep the neutral port
// simple; an OTel adapter maps them to its own attribute type.
type Attribute struct {
	Key   string
	Value string
}

// Span is one tracing unit. The zero TraceContext is allowed for no-op spans.
type Span interface {
	End(err error)
	SetAttributes(attrs ...Attribute)
	TraceContext() TraceContext
}

// Tracer starts spans. Implementations must attach the returned span to the
// returned context with WithSpan so downstream logs can read its ids.
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span)
}

// NoopTracer returns a no-op span and leaves the context untouched.
type NoopTracer struct{}

// Start implements Tracer.
func (NoopTracer) Start(ctx context.Context, _ string, _ ...Attribute) (context.Context, Span) {
	return ctx, NoopSpan{}
}

// NoopSpan drops every operation.
type NoopSpan struct{}

// End implements Span.
func (NoopSpan) End(error) {}

// SetAttributes implements Span.
func (NoopSpan) SetAttributes(...Attribute) {}

// TraceContext implements Span.
func (NoopSpan) TraceContext() TraceContext { return TraceContext{} }

type spanContextKey struct{}

// WithSpan attaches span to ctx. A nil span is a no-op.
func WithSpan(ctx context.Context, span Span) context.Context {
	if ctx == nil || span == nil {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, span)
}

// SpanFromContext returns the span attached by a Tracer.
func SpanFromContext(ctx context.Context) (Span, bool) {
	if ctx == nil {
		return nil, false
	}
	span, ok := ctx.Value(spanContextKey{}).(Span)
	return span, ok
}

// Transport injects the W3C traceparent from the request context into
// outbound HTTP requests. It is opt-in: applications wrap their own
// http.Client.Transport, so no library decides an HTTP client for them.
type Transport struct {
	Base http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	trace, ok := TraceFromContext(req.Context())
	if !ok || !trace.Valid() {
		return base.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("traceparent", trace.Traceparent())
	return base.RoundTrip(clone)
}

// TraceparentOption is an elective CoAP option used for W3C trace propagation.
// It is not a standard option: both peers must agree to it, so callers must
// treat its absence as "internal span only", never as an error.
const TraceparentOption uint16 = 65000

// WithCoAPTrace injects traceparent into a copy of options. It replaces any
// existing traceparent option and preserves every other option. Without a
// valid context trace the original slice is returned unchanged.
func WithCoAPTrace(ctx context.Context, options []coap.Option) []coap.Option {
	trace, ok := TraceFromContext(ctx)
	if !ok || !trace.Valid() {
		return options
	}
	out := make([]coap.Option, 0, len(options)+1)
	for _, option := range options {
		if option.Number != TraceparentOption {
			out = append(out, option)
		}
	}
	out = append(out, coap.Option{Number: TraceparentOption, Value: []byte(trace.Traceparent())})
	return out
}

// TraceFromCoAPOptions extracts a W3C traceparent from CoAP options.
func TraceFromCoAPOptions(options []coap.Option) (TraceContext, bool) {
	for _, option := range options {
		if option.Number == TraceparentOption {
			return ParseTraceparent(string(option.Value))
		}
	}
	return TraceContext{}, false
}
