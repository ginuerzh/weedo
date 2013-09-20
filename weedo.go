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
	AssignUri        = "/dir/assign"
	UploadUri        = "/submit"
	LookupUri        = "/dir/lookup?volumeId="
)

type Client struct {
	Url     string
	volumes map[uint64]string
}

type assignResp struct {
	Count     int
	Fid       string
	Url       string
	PublicUrl string
	Size      int
	Error     string
}

type lookupResp struct {
	Locations []location
	Error     string
}

type location struct {
	Url       string
	PublicUrl string
}

type uploadResp struct {
	Fid      string
	FileName string
	FileUrl  string
	Size     int
	Error    string
}

var defaultClient Client

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defaultClient = Client{DefaultMasterUrl, make(map[uint64]string)}
}

func ParseFid(fid string) (id, key, cookie uint64, err error) {
	s := strings.Split(fid, ",")
	if len(s) != 2 {
		err = errors.New("Fid format invalid")
		log.Println(err)
		return
	}
	if id, err = strconv.ParseUint(s[0], 10, 32); err != nil {
		log.Println(err)
		return
	}

	return
}

func getUrl(c *Client, fid string) (url string, err error) {
	id, _, _, err := ParseFid(fid)
	if err != nil {
		log.Println(err)
		return
	}

	if url, err = c.lookup(id); err != nil {
		log.Println(err)
		return
	}

	url = "http://" + url + "/" + fid

	return
}

func GetUrl(fid string) (url string, err error) {
	return getUrl(&defaultClient, fid)
}

func (c *Client) assign() (r *assignResp, err error) {
	resp, err := http.Get(c.Url + AssignUri)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	assign := new(assignResp)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(assign); err != nil {
		log.Println(err)
		return
	}
	if assign.Error != "" {
		err = errors.New(assign.Error)
		log.Println(err)
		return
	}

	r = assign
	log.Printf("assign fid: %s at %s", r.Fid, r.Url)

	return
}

func AssignUpload(filename string, file io.Reader) (fid string, size int, err error) {
	r, err := defaultClient.assign()
	if err != nil {
		return
	}

	data, contentType, err := makeUploadContent(filename, file)
	if err != nil {
		return
	}

	url := "http://" + r.Url + "/" + r.Fid
	log.Println(url)

	resp, err := upload(&defaultClient, url, contentType, data)
	if err == nil {
		fid = r.Fid
		size = resp.Size
	}

	return
}

func makeUploadContent(filename string, content io.Reader) (data io.Reader, contentType string, err error) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		log.Println(err)
		return
	}
	_, err = io.Copy(part, content)
	if err != nil {
		log.Println(err)
		return
	}

	data = buf
	contentType = writer.FormDataContentType()
	writer.Close()

	return
}

func upload(c *Client, url string, contentType string, data io.Reader) (r *uploadResp, err error) {
	resp, err := http.Post(url, contentType, data)
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	upload := new(uploadResp)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(upload); err != nil {
		log.Println(err)
		return
	}

	if upload.Error != "" {
		err = errors.New(upload.Error)
		log.Println(err)
		return
	}

	r = upload

	return
}

func Upload(filename string, file io.Reader) (fid string, size int, err error) {
	data, contentType, err := makeUploadContent(filename, file)
	if err != nil {
		return
	}
	resp, err := upload(&defaultClient, defaultClient.Url+UploadUri, contentType, data)
	if err == nil {
		fid = resp.Fid
		size = resp.Size
	}
	return
}

func download(c *Client, fid string) (file io.ReadCloser, err error) {
	url, err := c.GetUrl(fid)
	if err != nil {
		return
	}

	resp, err := http.Get(url)
	if err != nil {
		return
	}

	//log.Println(resp.Header.Get("Content-Type"))
	if resp.ContentLength == 0 {
		return nil, errors.New("File Not Found")
	}

	return resp.Body, nil
}

func Download(fid string) (file io.ReadCloser, err error) {
	return download(&defaultClient, fid)
}

func delete(c *Client, fid string) (err error) {
	url, err := c.GetUrl(fid)
	if err != nil {
		return
	}

	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return
	}
	client := http.Client{}

	_, err = client.Do(request)
	if err != nil {
		return
	}
	return
}

func Delete(fid string) (err error) {
	return delete(&defaultClient, fid)
}

func NewClient(ip string, port int) *Client {
	masterUrl := "http://" + ip + ":" + strconv.Itoa(port)
	return &Client{masterUrl, make(map[uint64]string)}
}

func (c *Client) Upload(filename string, file io.Reader) (fid string, size int, err error) {
	data, contentType, err := makeUploadContent(filename, file)
	if err != nil {
		return
	}
	resp, err := upload(c, c.Url+UploadUri, contentType, data)
	if err == nil {
		fid = resp.Fid
		size = resp.Size
	}
	return
}

func (c *Client) lookup(volumeId uint64) (url string, err error) {
	if v, ok := c.volumes[volumeId]; ok {
		return v, nil
	}

	resp, err := http.Get(c.Url + LookupUri + strconv.FormatUint(volumeId, 10))
	//log.Println(c.Url + LookupUri + strconv.FormatUint(volumeId, 10))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	lookup := new(lookupResp)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(lookup); err != nil {
		log.Println(err)
		return
	}

	if lookup.Error != "" {
		err = errors.New(lookup.Error)
		log.Println(err)
		return
	}

	c.volumes[volumeId] = lookup.Locations[0].Url

	return lookup.Locations[0].Url, nil
}

func (c *Client) GetUrl(fid string) (url string, err error) {
	return getUrl(c, fid)
}

func (c *Client) Download(fid string) (file io.ReadCloser, err error) {
	return download(c, fid)
}

func (c *Client) Delete(fid string) (err error) {
	return delete(c, fid)
}
