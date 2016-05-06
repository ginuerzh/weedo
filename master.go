// weed master
package weedo

import (
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

type Master struct {
	Url string
}

func NewMaster(addr string) *Master {
	return &Master{
		Url: addr,
	}
}

// Assign a file key
func (m *Master) Assign() (string, error) {
	return m.AssignArgs(url.Values{})
}

// Assign multi file keys
func (m *Master) AssignN(count int) (fid string, err error) {
	args := url.Values{}
	if count > 0 {
		args.Set("count", strconv.Itoa(count))
	}

	return m.AssignArgs(args)
}

type assignResp struct {
	Count     int
	Fid       string
	Url       string
	PublicUrl string
	Size      int64
	Error     string
}

// v0.4 or later only
func (m *Master) AssignArgs(args url.Values) (fid string, err error) {
	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/dir/assign",
		RawQuery: args.Encode(),
	}

	resp, err := http.Get(u.String())
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
	Locations []Location
	Error     string
}

type Location struct {
	Url       string
	PublicUrl string
}

// Lookup Volume
func (m *Master) lookup(volumeId, collection string) (*Volume, error) {
	args := url.Values{}
	args.Set("volumeId", volumeId)
	args.Set("collection", collection)

	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/dir/lookup",
		RawQuery: args.Encode(),
	}
	resp, err := http.Get(u.String())
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

	return NewVolume(lookup.Locations), nil
}

// Force Garbage Collection
func (m *Master) GC(threshold float64) error {
	args := url.Values{}
	args.Set("garbageThreshold", strconv.FormatFloat(threshold, 'f', -1, 64))
	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/vol/vacuum",
		RawQuery: args.Encode(),
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// TODO: handle result
	return nil
}

// Pre-Allocate Volumes
func (m *Master) Grow(count int, collection, replication, dataCenter string) error {
	args := url.Values{}
	if count > 0 {
		args.Set("count", strconv.Itoa(count))
	}
	args.Set("collection", collection)
	args.Set("replication", replication)
	args.Set("dataCenter", dataCenter)

	return m.GrowArgs(args)
}

// v0.4 or later only
func (m *Master) GrowArgs(args url.Values) error {
	u := url.URL{
		Scheme:   "http",
		Host:     m.Url,
		Path:     "/vol/grow",
		RawQuery: args.Encode(),
	}
	resp, err := http.Get(u.String())
	resp.Body.Close()

	return err
}

func (m *Master) Submit(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	return m.SubmitArgs(filename, mimeType, file, url.Values{})
}

// Upload File Directly
func (m *Master) SubmitArgs(filename, mimeType string, file io.Reader, args url.Values) (fid string, size int64, err error) {
	data, contentType, err := makeFormData(filename, mimeType, file)
	if err != nil {
		return
	}

	u := url.URL{
		Scheme: "http",
		Host:   m.Url,
		Path:   "/submit",
		RawQuery: args.Encode(),
	}

	resp, err := upload(u.String(), contentType, data)
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
	u := url.URL{
		Scheme: "http",
		Host:   m.Url,
		Path:   "/dir/status",
	}
	resp, err := http.Get(u.String())
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
