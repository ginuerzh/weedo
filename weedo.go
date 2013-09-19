// weedo.go
package weedo

import (
	"bytes"
	"encoding/json"
	"errors"
	//"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
)

const (
	DefaultMasterUrl = "http://localhost:9333"
	UploadUri        = "/submit"
	LookupUri        = "/dir/lookup?volumeId="
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
	Locations []Location
	Error     string
}

type Location struct {
	PublicUrl string
	Url       string
}

type UploadResp struct {
	Fid      string
	FileName string
	FileUrl  string
	Size     int
	Error    string
}

func NewClient(masterUrl string) *Client {
	if !strings.HasPrefix(masterUrl, "http://") {
		masterUrl = "http://" + masterUrl
	}
	return &Client{masterUrl}
}

func (c *Client) Upload(filename string, content io.Reader) (fid string, err error) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return
	}
	_, err = io.Copy(part, content)
	if err != nil {
		return
	}

	contentType := writer.FormDataContentType()
	writer.Close()
	//fmt.Println(contentType)

	resp, err := http.Post(c.Url+UploadUri, contentType, buf)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	uploadResp := new(UploadResp)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(uploadResp); err != nil {
		return
	}

	if uploadResp.Error != "" {
		err = errors.New(uploadResp.Error)
		return
	}

	return uploadResp.Fid, nil
}

func (c *Client) lookup(volumeId uint64) (url string, err error) {
	resp, err := http.Get(c.Url + LookupUri + strconv.FormatUint(volumeId, 10))
	log.Println(c.Url + LookupUri + strconv.FormatUint(volumeId, 10))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	lookupResp := new(LookupResp)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(lookupResp); err != nil {
		return
	}

	if lookupResp.Error != "" {
		err = errors.New(lookupResp.Error)
		return
	}

	return lookupResp.Locations[0].PublicUrl, nil
}

func ParseFid(fid string) (id, key, cookie uint64, err error) {
	s := strings.Split(fid, ",")
	if len(s) != 2 {
		return
	}
	if id, err = strconv.ParseUint(s[0], 10, 32); err != nil {
		return
	}

	return
}

func (c *Client) GetUrl(fid string) (url string, err error) {
	id, _, _, err := ParseFid(fid)
	if err != nil {
		return
	}

	if url, err = c.lookup(id); err != nil {
		return
	}

	url = "http://" + url + "/" + fid

	return
}
