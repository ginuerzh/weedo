// weedo.go
package weedo

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
)

const (
	UploadUri = "/submit"
)

type Client struct {
	Url string
}

type AssignResp struct {
	Count     int
	Fid       string
	Url       string
	PublicUrl string
}

type LookupResp struct {
	Locations map[string]interface{}
}

type UploadResp struct {
	Fid      string
	FileName string
	FileUrl  string
	Size     int
}

func NewClient(masterUrl string) *Client {
	return &Client{masterUrl}
}

func (c *Client) Upload(filename, contentType string, content io.Reader) (url string, e error) {
	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)

	fmt.Println(filename, contentType)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(part, content)
	writer.Close()

	fmt.Println(body)
	resp, err := http.Post(c.Url+UploadUri, "multipart/form-data", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return "", err
	}
	fmt.Println(string(data))

	return
}
