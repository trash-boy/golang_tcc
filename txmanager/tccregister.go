package txmanager

import (
	"errors"
	"fmt"
	"golang_tcc/component"
	"sync"
)

type registerCenter struct {
	//可以使用并发读写的sync.MAP
	mux sync.Mutex
	components map[string]component.TCCComponent
}

func newRegisterCenter()*registerCenter{
	return &registerCenter{
		components: make(map[string]component.TCCComponent),
	}
}

func (r *registerCenter)register(component component.TCCComponent)error{
	r.mux.Lock()
	defer r.mux.Unlock()

	if _,ok := r.components[component.ID()];ok{
		return errors.New("repeat register component id")
	}

	r.components[component.ID()] = component
	return nil
}


func (r *registerCenter)getComponents(componentIDs ...string)([]component.TCCComponent, error){
	components := make([]component.TCCComponent, 0, len(componentIDs))

	r.mux.Lock()
	defer r.mux.Unlock()
	for _,componentID := range componentIDs{
		component,ok := r.components[componentID]
		if !ok{
			return nil, fmt.Errorf("component id: %s not existed", componentID)
		}
		components = append(components, component)
	}
	return components,nil
}
