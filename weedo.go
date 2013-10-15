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
	AssignUri             = "/dir/assign"
	UploadUri             = "/submit"
	LookupUri             = "/dir/lookup?volumeId="
	VolumeServerStatusUri = "/status"
	SystemStatusUri       = "/dir/status"
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

type systemStatus struct {
	Topology topology
	Version  string
	Error    string
}

type topology struct {
	DataCenters dataCenter
	Free        int
	Max         int
	Layouts     []layout
}

type dataCenter struct {
	Free  int
	Max   int
	Racks []rack
}

type rack struct {
	DataNodes []dataNode
	Free      int
	Max       int
}

type dataNode struct {
	Free      int
	Max       int
	PublicUrl string
	Url       string
	Volumes   int
}

type layout struct {
	Replication string
	Writables   []uint64
}

type volumeServerStatus struct {
	Version string
	volumes []volume
	Error   string
}

type volume struct {
	Id               uint64
	Size             uint64
	RepType          string
	Version          int
	FileCount        uint64
	DeleteCount      uint64
	DeletedByteCount uint64
	ReadOnly         bool
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
		//log.Println(err)
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

// Upload file directly to default master server: localhost:9333
// It is same as: curl -F file=@/home/chris/myphoto.jpg http://localhost:9333/submit
func MasterUpload(filename string, file io.Reader) (fid string, size int, err error) {
	return defaultClient.MasterUpload(filename, file)
}

// First, contact with master server and assign a fid, then upload to volume server
// It is same as the follow steps
// curl http://localhost:9333/dir/assign
// curl -F file=@example.jpg http://127.0.0.1:8080/3,01637037d6
func AssignUpload(filename string, file io.Reader) (fid string, size int, err error) {
	return defaultClient.AssignUpload(filename, file)
}

func Download(fid string) (file io.ReadCloser, length int64, err error) {
	return defaultClient.Download(fid)
}

func Delete(fid string) (err error) {
	return defaultClient.Delete(fid)
}

// Get a fresh new weed client, with it's master url is ip:port
func NewClient(ip string, port int) *Client {
	masterUrl := ip + ":" + strconv.Itoa(port)
	if !strings.HasPrefix(masterUrl, "http://") {
		masterUrl = "http://" + masterUrl
	}
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
	id, _, _, err := ParseFid(fid)
	if err != nil {
		log.Println(err)
		return
	}
	//log.Printf("id:%d, key:%x, cookie:%x\n", id, key, cookie)

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

func (c *Client) Download(fid string) (file io.ReadCloser, length int64, err error) {
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
		return nil, 0, errors.New("File Not Found")
	}

	return resp.Body, resp.ContentLength, nil
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

	return
}

func (c *Client) VolumeServerStatus(ip string, port uint16) (err error) {
	url := ip + ":" + strconv.Itoa(int(port))
	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}
	resp, err := http.Get(url + VolumeServerStatusUri)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	status := new(volumeServerStatus)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(status); err != nil {
		log.Println(err)
		return
	}

	if status.Error != "" {
		err = errors.New(status.Error)
		log.Println(err)
		return
	}
	return
}

func (c *Client) SystemStatus() (err error) {
	resp, err := http.Get(c.Url + SystemStatusUri)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	status := new(systemStatus)
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(status); err != nil {
		log.Println(err)
		return
	}

	if status.Error != "" {
		err = errors.New(status.Error)
		log.Println(err)
		return
	}
	return
}
