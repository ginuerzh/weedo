// weedo.go
package weedo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
)

var defaultClient *Client

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defaultClient = NewClient("localhost:9333")
}

type Fid struct {
	Id, Key, Cookie uint64
}

type Client struct {
	master  *Master
	volumes map[uint64]*Volume
	filers  map[string]*Filer
}

func NewClient(masterUrl string, filerUrls ...string) *Client {
	filers := make(map[string]*Filer)
	for _, url := range filerUrls {
		filer := NewFiler(url)
		filers[filer.Url] = filer
	}
	return &Client{
		master:  NewMaster(masterUrl),
		volumes: make(map[uint64]*Volume),
		filers:  filers,
	}
}

func (c *Client) Master() *Master {
	return c.master
}

func (c *Client) Volume(volumeId, collection string) (*Volume, error) {
	vid, _ := strconv.ParseUint(volumeId, 10, 32)
	if vid == 0 {
		fid, _ := ParseFid(volumeId)
		vid = fid.Id
	}

	if vid == 0 {
		return nil, errors.New("id malformed")
	}

	if v, ok := c.volumes[vid]; ok {
		return v, nil
	}
	vol, err := c.Master().lookup(volumeId, collection)
	if err != nil {
		return nil, err
	}

	c.volumes[vid] = vol

	return vol, nil
}

func (c *Client) Filer(url string) *Filer {
	filer := NewFiler(url)
	if v, ok := c.filers[filer.Url]; ok {
		return v
	}

	c.filers[filer.Url] = filer
	return filer
}

func ParseFid(s string) (fid Fid, err error) {
	a := strings.Split(s, ",")
	if len(a) != 2 || len(a[1]) <= 8 {
		return fid, errors.New("Fid format invalid")
	}
	if fid.Id, err = strconv.ParseUint(a[0], 10, 32); err != nil {
		return
	}
	index := len(a[1]) - 8
	if fid.Key, err = strconv.ParseUint(a[1][:index], 16, 64); err != nil {
		return
	}
	if fid.Cookie, err = strconv.ParseUint(a[1][index:], 16, 32); err != nil {
		return
	}

	return
}

// First, contact with master server and assign a fid, then upload to volume server
// It is same as the follow steps
// curl http://localhost:9333/dir/assign
// curl -F file=@example.jpg http://127.0.0.1:8080/3,01637037d6
func AssignUpload(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	return AssignUploadArgs(filename, mimeType, file, url.Values{})
}

func AssignUploadArgs(filename, mimeType string, file io.Reader, args url.Values) (fid string, size int64, err error) {
	return defaultClient.AssignUploadArgs(filename, mimeType, file, args)
}

func Delete(fid string, count int) (err error) {
	return defaultClient.Delete(fid, count)
}

func (c *Client) GetUrl(fid string, collection ...string) (publicUrl, url string, err error) {
	col := ""
	if len(collection) > 0 {
		col = collection[0]
	}
	vol, err := c.Volume(fid, col)
	if err != nil {
		return
	}

	publicUrl = fmt.Sprintf("%s/%s", vol.PublicUrl(), fid)
	url = fmt.Sprintf("%s/%s", vol.Url(), fid)
	return
}

func (c *Client) GetUrls(fid string, collection ...string) (locations []Location, err error) {
	col := ""
	if len(collection) > 0 {
		col = collection[0]
	}
	vol, err := c.Volume(fid, col)
	if err != nil {
		return
	}
	for _, loc := range vol.Locations {
		loc.PublicUrl = fmt.Sprintf("%s/%s", loc.PublicUrl, fid)
		loc.Url = fmt.Sprintf("%s/%s", loc.Url, fid)
		locations = append(locations, loc)
	}
	return
}

func (c *Client) AssignUpload(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	return c.AssignUploadArgs(filename, mimeType, file, url.Values{})
}

func (c *Client) AssignUploadArgs(filename, mimeType string, file io.Reader, args url.Values) (fid string, size int64, err error) {
	fid, err = c.Master().AssignArgs(args)
	if err != nil {
		return
	}

	vol, err := c.Volume(fid, args.Get("collection"))
	if err != nil {
		return
	}
	size, err = vol.Upload(fid, 0, filename, mimeType, file)

	return
}

func (c *Client) Delete(fid string, count int, collection ...string) (err error) {
	col := ""
	if len(collection) > 0 {
		col = collection[0]
	}
	vol, err := c.Volume(fid, col)
	if err != nil {
		return
	}
	return vol.Delete(fid, count)
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

func createFormFile(writer *multipart.Writer, fieldname, filename, mime string) (io.Writer, error) {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition",
		fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			escapeQuotes(fieldname), escapeQuotes(filename)))
	if len(mime) == 0 {
		mime = "application/octet-stream"
	}
	h.Set("Content-Type", mime)
	return writer.CreatePart(h)
}

func makeFormData(filename, mimeType string, content io.Reader) (formData io.Reader, contentType string, err error) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	part, err := createFormFile(writer, "file", filename, mimeType)
	//log.Println(filename, mimeType)
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
	//log.Println(contentType)
	writer.Close()

	return
}

type uploadResp struct {
	Fid      string
	FileName string
	FileUrl  string
	Size     int64
	Error    string
}

func upload(url string, contentType string, formData io.Reader) (r *uploadResp, err error) {
	resp, err := http.Post(url, contentType, formData)
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	upload := new(uploadResp)
	if err = decodeJson(resp.Body, upload); err != nil {
		return
	}

	if upload.Error != "" {
		err = errors.New(upload.Error)
		return
	}

	r = upload

	return
}

func del(url string) error {
	client := http.Client{}
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(request)
	resp.Body.Close()
	return err
}

func decodeJson(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}
