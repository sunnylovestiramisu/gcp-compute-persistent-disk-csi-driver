/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package v1beta1

import (
	context "context"

	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	applyconfigurationsschedulingv1beta1 "k8s.io/client-go/applyconfigurations/scheduling/v1beta1"
	gentype "k8s.io/client-go/gentype"
	scheme "k8s.io/client-go/kubernetes/scheme"
)

// PriorityClassesGetter has a method to return a PriorityClassInterface.
// A group's client should implement this interface.
type PriorityClassesGetter interface {
	PriorityClasses() PriorityClassInterface
}

// PriorityClassInterface has methods to work with PriorityClass resources.
type PriorityClassInterface interface {
	Create(ctx context.Context, priorityClass *schedulingv1beta1.PriorityClass, opts v1.CreateOptions) (*schedulingv1beta1.PriorityClass, error)
	Update(ctx context.Context, priorityClass *schedulingv1beta1.PriorityClass, opts v1.UpdateOptions) (*schedulingv1beta1.PriorityClass, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*schedulingv1beta1.PriorityClass, error)
	List(ctx context.Context, opts v1.ListOptions) (*schedulingv1beta1.PriorityClassList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *schedulingv1beta1.PriorityClass, err error)
	Apply(ctx context.Context, priorityClass *applyconfigurationsschedulingv1beta1.PriorityClassApplyConfiguration, opts v1.ApplyOptions) (result *schedulingv1beta1.PriorityClass, err error)
	PriorityClassExpansion
}

// priorityClasses implements PriorityClassInterface
type priorityClasses struct {
	*gentype.ClientWithListAndApply[*schedulingv1beta1.PriorityClass, *schedulingv1beta1.PriorityClassList, *applyconfigurationsschedulingv1beta1.PriorityClassApplyConfiguration]
}

// newPriorityClasses returns a PriorityClasses
func newPriorityClasses(c *SchedulingV1beta1Client) *priorityClasses {
	return &priorityClasses{
		gentype.NewClientWithListAndApply[*schedulingv1beta1.PriorityClass, *schedulingv1beta1.PriorityClassList, *applyconfigurationsschedulingv1beta1.PriorityClassApplyConfiguration](
			"priorityclasses",
			c.RESTClient(),
			scheme.ParameterCodec,
			"",
			func() *schedulingv1beta1.PriorityClass { return &schedulingv1beta1.PriorityClass{} },
			func() *schedulingv1beta1.PriorityClassList { return &schedulingv1beta1.PriorityClassList{} },
			gentype.PrefersProtobuf[*schedulingv1beta1.PriorityClass](),
		),
	}
}
