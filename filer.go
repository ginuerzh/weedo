// filer
package weedo

import (
	"bytes"
	"io"
	"net/http"
	"strings"
)

type File struct {
	Id   string `json:"fid"`
	Name string `json:"name"`
}

type Dir struct {
	Path    string `json:"Directory"`
	Files   []*File
	Subdirs []*File `json:"Subdirectories"`
}

func (dir Dir) String() string {
	b := bytes.Buffer{}
	b.WriteString("\n")
	b.WriteString(dir.Path + "\n")
	for _, d := range dir.Subdirs {
		b.WriteString("  " + d.Name + "/\n")
	}
	for _, f := range dir.Files {
		b.WriteString("  " + f.Name + "\n")
	}
	return b.String()
}

type Filer struct {
	Url string
}

func NewFiler(url string) *Filer {
	if !strings.HasPrefix(url, "http:") {
		url = "http://" + url
	}
	return &Filer{
		Url: url,
	}
}

func (f *Filer) Dir(pathname string) (*Dir, error) {
	if !strings.HasPrefix(pathname, "/") {
		pathname = "/" + pathname
	}
	if !strings.HasSuffix(pathname, "/") {
		pathname = pathname + "/"
	}
	resp, err := http.Get(f.Url + pathname)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	filerResp := new(Dir)
	if err = decodeJson(resp.Body, filerResp); err != nil {
		return nil, err
	}
	return filerResp, nil
}

func (f *Filer) Upload(pathname string, mimeType string, file io.Reader) error {
	formData, contentType, err := makeFormData(pathname, mimeType, file)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(pathname, "/") {
		pathname = "/" + pathname
	}

	_, err = http.Post(f.Url+pathname, contentType, formData)
	return err
}

func (f *Filer) Delete(pathname string) error {
	if !strings.HasPrefix(pathname, "/") {
		pathname = "/" + pathname
	}

	return del(f.Url + pathname)
}
