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
	AssignUri = "/dir/assign"
	UploadUri = "/submit"
	LookupUri = "/dir/lookup?volumeId="
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

var defaultClient *Client

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defaultClient = NewClient("localhost", 9333)
}

func ParseFid(fid string) (id, key, cookie uint64, err error) {
	s := strings.Split(fid, ",")
	if len(s) != 2 || len(s[1]) <= 8 {
		err = errors.New("Fid format invalid")
		log.Println(err)
		return
	}
	if id, err = strconv.ParseUint(s[0], 10, 32); err != nil {
		log.Println(err)
		return
	}
	//log.Println(s, len(s[1]))
	index := len(s[1]) - 8
	if key, err = strconv.ParseUint(s[1][:index], 16, 64); err != nil {
		log.Println(err)
		return
	}
	if cookie, err = strconv.ParseUint(s[1][index:], 16, 32); err != nil {
		log.Println(err)
		return
	}
	return
}

func GetUrl(fid string) (url string, err error) {
	return defaultClient.GetUrl(fid)
}

func VolumeUpload(fid string, version int, filename string, file io.Reader) (size int, err error) {
	return defaultClient.VolumeUpload(fid, version, filename, file)
}

func MasterUpload(filename string, file io.Reader) (fid string, size int, err error) {
	return defaultClient.MasterUpload(filename, file)
}

func AssignUpload(filename string, file io.Reader) (fid string, size int, err error) {
	return defaultClient.AssignUpload(filename, file)
}

func Download(fid string) (file io.ReadCloser, err error) {
	return defaultClient.Download(fid)
}

func Delete(fid string) (err error) {
	return defaultClient.Delete(fid)
}

func NewClient(ip string, port int) *Client {
	masterUrl := "http://" + ip + ":" + strconv.Itoa(port)
	return &Client{masterUrl, make(map[uint64]string)}
}

func (c *Client) Assign() (fid string, err error) {
	return c.AssignN(1)
}

func (c *Client) AssignN(count int) (fid string, err error) {
	if count <= 0 {
		count = 1
	}
	url := c.Url + AssignUri
	if count > 1 {
		url = url + "?count=" + strconv.Itoa(count)
	}
	resp, err := http.Get(url)
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

	fid = assign.Fid
	log.Printf("assign fid:%s at %s, count:%d", fid, assign.Url, assign.Count)

	return
}

func (c *Client) AssignUpload(filename string, file io.Reader) (fid string, size int, err error) {
	fid, err = c.Assign()
	if err != nil {
		return
	}

	size, err = c.VolumeUpload(fid, 0, filename, file)

	return
}

func (c *Client) VolumeUpload(fid string, version int, filename string, file io.Reader) (size int, err error) {
	url, err := c.GetUrl(fid)
	if err != nil {
		return
	}
	if version > 0 {
		url = url + "_" + strconv.Itoa(version)
	}

	formData, contentType, err := c.makeFormData(filename, file)
	if err != nil {
		return
	}

	resp, err := c.upload(url, contentType, formData)
	if err == nil {
		size = resp.Size
	}
	return
}

func (c *Client) MasterUpload(filename string, file io.Reader) (fid string, size int, err error) {
	data, contentType, err := c.makeFormData(filename, file)
	if err != nil {
		return
	}
	resp, err := c.upload(c.Url+UploadUri, contentType, data)
	if err == nil {
		fid = resp.Fid
		size = resp.Size
	}
	return
}

func (_ *Client) upload(url string, contentType string, formData io.Reader) (r *uploadResp, err error) {
	resp, err := http.Post(url, contentType, formData)
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
	id, key, cookie, err := ParseFid(fid)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("id:%d, key:%x, cookie:%x\n", id, key, cookie)

	if url, err = c.lookup(id); err != nil {
		log.Println(err)
		return
	}

	url = "http://" + url + "/" + fid

	return
}

func (_ *Client) makeFormData(filename string, content io.Reader) (formData io.Reader, contentType string, err error) {
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

	formData = buf
	contentType = writer.FormDataContentType()
	writer.Close()

	return
}

func (c *Client) Download(fid string) (file io.ReadCloser, err error) {
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

func (c *Client) Delete(fid string) (err error) {
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
