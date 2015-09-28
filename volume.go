// weed volume
package weedo

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Volume struct {
	Url       string
	PublicUrl string
}

func NewVolume(url, publicUrl string) *Volume {
	if !strings.HasPrefix(url, "http:") {
		url = "http://" + url
	}
	if !strings.HasPrefix(publicUrl, "http:") {
		publicUrl = "http://" + publicUrl
	}
	return &Volume{
		Url:       url,
		PublicUrl: publicUrl,
	}
}

// Upload File
func (v *Volume) Upload(fid string, version int, filename, mimeType string, file io.Reader) (size int64, err error) {
	url := v.PublicUrl + "/" + fid
	if version > 0 {
		url = url + "_" + strconv.Itoa(version)
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
	resp, err := upload(v.PublicUrl+"/submit", contentType, data)
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

	url := v.PublicUrl + "/" + fid
	if err := del(url); err != nil {
		return err
	}

	for i := 1; i < count; i++ {
		if err := del(url + "_" + strconv.Itoa(i)); err != nil {
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

	_, err := http.Get(v.PublicUrl + "/admin/assign_volume?" + values.Encode())
	return err
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
	url := v.PublicUrl
	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}
	resp, err := http.Get(url + "/status")
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
