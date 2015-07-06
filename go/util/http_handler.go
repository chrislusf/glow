package util

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/golang/glog"
	"github.com/labstack/echo"
)

func doWriteJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

// wrapper for doWriteJson - just logs errors
func WriteJson(c *echo.Context, httpStatus int, obj interface{}) {
	if err := doWriteJson(c.Response(), c.Request(), httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON %s: %v", obj, err)
	}
}
func WriteJsonError(c *echo.Context, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	WriteJson(c, httpStatus, m)
}
