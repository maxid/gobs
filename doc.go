// Package gobs adapts the g* libraries' neutral observability hooks to the
// gable metrics and health registries.
//
// It is an integration library, not a framework: it depends on gable's
// registry types and on the hook contracts of the protocol libraries, but
// neither gable nor a protocol library depends on gobs. The composition root
// decides which adapters to register and where to mount /metrics.
package gobs
