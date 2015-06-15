package keen

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"text/template"
	"time"
)

const (
	baseUrl       = "https://api.keen.io/3.0/projects/"
	queryTemplate = `/queries/{{.Type}}?event_collection={{.EventCollection}}&target_property={{.TargetProperty}}&group_by={{.GroupBy}}`
)

type KeenProperties struct {
	Timestamp string `json:"timestamp"`
}

// Timestamp formats a time.Time object in the ISO-8601 format keen expects
func Timestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

type Client struct {
	ApiKey     string
	WriteKey   string
	ProjectId  string
	httpClient *http.Client
}

type Command struct {
	TargetProperty  string
	GroupBy         string
	Type            string
	EventCollection string
	Filter          Filter
}

type Filter struct {
	Percentile string
	Timeframe  string
}

type Result struct {
	Result float64 `json:"result"`
	UserID string  `json:"userId"`
}
type KeenResult struct {
	Result []Result `json:"result"`
}

type BySentiment []Result

func (a BySentiment) Len() int           { return len(a) }
func (a BySentiment) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySentiment) Less(i, j int) bool { return a[i].Result < a[j].Result }

func NewClient(apiKey, writeKey, projectId string) *Client {
	c := &Client{
		ApiKey:     apiKey,
		WriteKey:   writeKey,
		ProjectId:  projectId,
		httpClient: http.DefaultClient,
	}
	return c
}

func (c *Client) AddEvent(collection string, event interface{}) error {
	//todo add in response as return.
	_, err := c.doRequest("POST", fmt.Sprintf("/events/%s", collection), event)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) AddEvents(events map[string][]interface{}) error {
	_, err := c.doRequest("POST", "/events", events)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetAnalysis(cmd *Command) (*KeenResult, error) {
	var keenResult KeenResult
	t, err := template.New("Get Template").Parse(queryTemplate)
	var path bytes.Buffer
	t.Execute(&path, *cmd)
	s := path.String()
	log.Printf("query string %v", s)
	res, err := c.doRequest("GET", s, nil)

	log.Printf("response: %v error: %v", string(res), err)
	err = json.Unmarshal(res, &keenResult)
	if err != nil {
		log.Printf("Error :96: %v", err.Error())
		return nil, err
	}
	// in theory this doesn't work because it could be a diff result struct each time
	return &keenResult, nil
}

func (c *Client) doRequest(method, path string, body interface{}) ([]byte, error) {
	req, err := c.makeRequest(method, path, body)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/200 != 1 {
		return nil, errors.New(string(respBody))
	}
	return respBody, nil
}

func (c *Client) makeRequest(method, path string, payload interface{}) (*http.Request, error) {

	// construct url
	url := baseUrl + c.ProjectId + path
	log.Printf("Calling URL: %v", url)

	if payload != nil {
		// serialize payload
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		log.Print("making post request")
		log.Printf("ApiKey: %v", c.WriteKey)
		// new request
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		// set length/content-type
		if body != nil {
			req.Header.Add("Content-Type", "application/json")
			req.ContentLength = int64(len(body))
		}
		// add auth
		req.Header.Add("Authorization", c.WriteKey)

		return req, nil
	} else {
		// new request

		if method == "GET" {

			url = url + "&api_key=" + c.ApiKey
			log.Printf("Into get method %v", url)
			req, err := http.NewRequest(method, url, nil)
			if err != nil {
				return nil, err
			}
			return req, nil
		} else {
			req, err := http.NewRequest(method, url, nil)
			if err != nil {
				return nil, err
			}
			// add auth
			req.Header.Add("Authorization", c.ApiKey)
			return http.NewRequest(method, path, nil)
		}

	}

}
