// weed master
package weedo

import (
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Master struct {
	Url string
}

func NewMaster(url string) *Master {
	if !strings.HasPrefix(url, "http:") {
		url = "http://" + url
	}
	return &Master{
		Url: url,
	}
}

// Assign a file key
func (m *Master) Assign() (string, error) {
	return m.AssignN(1)
}

type assignResp struct {
	Count     int
	Fid       string
	Url       string
	PublicUrl string
	Size      int64
	Error     string
}

// Assign multi file keys
func (m *Master) AssignN(count int) (fid string, err error) {
	if count <= 0 {
		count = 1
	}
	url := m.Url + "/dir/assign"
	if count > 1 {
		url = url + "?count=" + strconv.Itoa(count)
	}
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	assign := new(assignResp)
	if err = decodeJson(resp.Body, assign); err != nil {
		log.Println(err)
		return
	}

	if assign.Error != "" {
		err = errors.New(assign.Error)
		log.Println(err)
		return
	}

	fid = assign.Fid

	return
}

type lookupResp struct {
	Locations []location
	Error     string
}

type location struct {
	Url       string
	PublicUrl string
}

// Lookup Volume
func (m *Master) lookup(volumeId, collection string) (*Volume, error) {
	v := url.Values{}
	v.Add("volumeId", volumeId)
	if len(collection) > 0 {
		v.Add("collection", collection)
	}
	resp, err := http.Get(m.Url + "/dir/lookup?" + v.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	lookup := new(lookupResp)
	if err = decodeJson(resp.Body, lookup); err != nil {
		log.Println(err)
		return nil, err
	}

	if lookup.Error != "" {
		return nil, errors.New(lookup.Error)
	}

	return NewVolume(lookup.Locations[0].Url, lookup.Locations[0].PublicUrl), nil
}

// Force Garbage Collection
func (m *Master) GC(threshold float64) error {
	resp, err := http.Get(m.Url + "/vol/vacuum?garbageThreshold=" +
		strconv.FormatFloat(threshold, 'f', -1, 64))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// TODO: handle result
	return nil
}

// Pre-Allocate Volumes
func (m *Master) Grow(count int, collection, replica, dataCenter string) error {
	v := url.Values{}
	v.Set("count", strconv.Itoa(count))
	if len(collection) > 0 {
		v.Set("collection", collection)
	}
	if len(replica) > 0 {
		v.Set("replication", replica)
	}
	if len(dataCenter) > 0 {
		v.Set("dataCenter", dataCenter)
	}

	_, err := http.Get(m.Url + "/vol/grow?" + v.Encode())
	return err
}

// Upload File Directly
func (m *Master) Submit(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	data, contentType, err := makeFormData(filename, mimeType, file)
	if err != nil {
		return
	}
	resp, err := upload(m.Url+"/submit", contentType, data)
	if err == nil {
		fid = resp.Fid
		size = resp.Size
	}

	return
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

// Check System Status
func (m *Master) Status() (err error) {
	resp, err := http.Get(m.Url + "/dir/status")
	if err != nil {
		return
	}

	defer resp.Body.Close()

	status := new(systemStatus)
	if err = decodeJson(resp.Body, status); err != nil {
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
