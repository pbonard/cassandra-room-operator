// Package v1alpha1 is the API of the cassandra-room operator.
//
// +kubebuilder:object:generate=true
// +groupName=cassandraroom.pbonard.github.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API's group and version.
	GroupVersion = schema.GroupVersion{Group: "cassandraroom.pbonard.github.io", Version: "v1alpha1"}

	// SchemeBuilder registers the API's types.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the API's types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
