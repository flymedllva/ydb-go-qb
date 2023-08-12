package yscan

import "reflect"

// isDoublePointer checks if the given interface is a double pointer
func isDoublePointer(i interface{}) bool {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return false
	}
	if v.Elem().Kind() != reflect.Ptr {
		return false
	}

	return true
}
