package httphelpers

import "net/http"

func AddQueryParams(r *http.Request, params map[string]string) {
	url := r.URL

	query := url.Query()
	for key, value := range params {
		query.Add(key, value)
	}
	url.RawQuery = query.Encode()
}
