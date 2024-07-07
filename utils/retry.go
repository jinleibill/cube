package utils

import (
	"fmt"
	"net/http"
	"time"
)

func HttpWithRetry(f func(string) (*http.Response, error), url string) (*http.Response, error) {
	count := 10
	var resp *http.Response
	var err error
	for i := 0; i < count; i++ {
		resp, err = f(url)
		if err != nil {
			fmt.Printf("调用 url 错误: %v\n", url)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}
	return resp, err
}
