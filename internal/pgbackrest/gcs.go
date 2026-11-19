package pgbackrest

import (
	"fmt"
	"net/url"
	"strings"
)

// GCSFolderAnnotation requests access to a gs://bucket/folder/ location.
const GCSFolderAnnotation = "xata.io/gcs-managed-folder-uri"

// GCSRestoreFolderAnnotation requests read access to the gs://bucket/folder/
// location of another branch for a restore.
const GCSRestoreFolderAnnotation = "xata.io/restore-gcs-managed-folder-uri"

// GCSFolderURI is the Cloud Storage URI of a repository folder.
func GCSFolderURI(bucket, folder string) string {
	return (&url.URL{Scheme: "gs", Host: bucket, Path: "/" + strings.Trim(folder, "/") + "/"}).String()
}

// ParseGCSFolderURI returns the bucket and managed folder in a Cloud Storage URI.
func ParseGCSFolderURI(location string) (string, string, error) {
	u, err := url.Parse(location)
	if err != nil || u.Scheme != "gs" || u.Host == "" || u.Hostname() != u.Host || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", "", fmt.Errorf("invalid managed folder URI %q", location)
	}
	folder := strings.Trim(u.Path, "/")
	if folder == "" {
		return "", "", fmt.Errorf("invalid managed folder URI %q", location)
	}
	return u.Host, folder + "/", nil
}
