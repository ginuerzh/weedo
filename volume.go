// weed volume
package weedo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Volume struct {
	Locations []Location
}

func NewVolume(locations []Location) *Volume {
	for i, _ := range locations {
		if !strings.HasPrefix(locations[i].Url, "http:") {
			locations[i].Url = fmt.Sprintf("http://%s", locations[i].Url)
		}
		if !strings.HasPrefix(locations[i].PublicUrl, "http:") {
			locations[i].PublicUrl = fmt.Sprintf("http://%s", locations[i].PublicUrl)
		}
	}
	return &Volume{Locations: locations}
}

// Upload File
func (v *Volume) Upload(fid string, version int, filename, mimeType string, file io.Reader) (size int64, err error) {
	url := fmt.Sprintf("%s/%s", v.PublicUrl(), fid) // http://localhost:8080/3,7363da54ae
	if version > 0 {
		url = fmt.Sprintf("%s_%d", url, version) // http://localhost:8080/3,7363da54ae_1
	}

	formData, contentType, err := makeFormData(filename, mimeType, file)
	if err != nil {
		return
	}

	resp, err := upload(url, contentType, formData)
	if err == nil {
		size = resp.Size
	}

	return
}

// Upload File Directly
func (v *Volume) Submit(filename, mimeType string, file io.Reader) (fid string, size int64, err error) {
	data, contentType, err := makeFormData(filename, mimeType, file)
	if err != nil {
		return
	}
	resp, err := upload(v.PublicUrl()+"/submit", contentType, data)
	if err == nil {
		fid = resp.Fid
		size = resp.Size
	}

	return
}

// Delete File
func (v *Volume) Delete(fid string, count int) (err error) {
	if count <= 0 {
		count = 1
	}

	url := fmt.Sprintf("%s/%s", v.PublicUrl(), fid)
	if err := del(url); err != nil {
		return err
	}

	for i := 1; i < count; i++ {
		if err := del(fmt.Sprintf("%s_%d", url, i)); err != nil {
			log.Println(err)
		}
	}

	return nil
}

func (v *Volume) AssignVolume(volumeId uint64, replica string) error {
	values := url.Values{}
	values.Set("volume", strconv.FormatUint(volumeId, 10))
	if len(replica) > 0 {
		values.Set("replication", replica)
	}

	_, err := http.Get(fmt.Sprintf("%s/admin/assign_volume?%s", v.PublicUrl(), values.Encode()))
	return err
}

func (v *Volume) Url() string {
	if len(v.Locations) == 0 {
		return ""
	}
	return v.Locations[0].Url
}

func (v *Volume) PublicUrl() string {
	if len(v.Locations) == 0 {
		return ""
	}
	return v.Locations[0].PublicUrl
}

type volumeStatus struct {
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

// Check Volume Server Status
func (v *Volume) Status() (err error) {
	resp, err := http.Get(fmt.Sprintf("%s/status", v.PublicUrl()))
	if err != nil {
		return
	}

	defer resp.Body.Close()

	status := new(volumeStatus)
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
