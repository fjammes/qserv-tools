/*
* LSST Data Management System
* See COPYRIGHT file at the top of the source tree.
*
* This product includes software developed by the
* LSST Project (http://www.lsst.org/).
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the LSST License Statement and
* the GNU General Public License along with this program. If not,
* see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Generate dbbench.ini file from Qserv integration tests's datasets
// See qserv/itest_src/datasets/case<ID>/queries
// Exemple to run it:
// go run itest/examples/dbbench.go && cat /tmp/dbbench.ini

package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Filetype int64

const (
	Csv Filetype = iota
	Chunk
	Json
	Overlap
	Unknown
)

type metadata struct {
	Database string `json:"database"`
	// map key is the schema file
	Tables map[string]table `json:"tables"`
}

type table struct {
	Indexes []string `json:"indexes"`
	// map key is the directory
	DataList map[string]data `json:"data"`
}

type data struct {
	Chunks   []int    `json:"chunks"`
	Overlaps []int    `json:"overlaps"`
	Files    []string `json:"files"`
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func newMetadata() *metadata {
	var metadata metadata
	metadata.Tables = make(map[string]table)
	return &metadata
}

func newTable() *table {
	var table table
	table.DataList = make(map[string]data)
	return &table
}

func update(data *data, filename string) {
	data.Files = append(data.Files, filename)
}

func filetype(filename string) (Filetype, int, error) {

	overlap := regexp.MustCompile(`^chunk_[0-9]+_overlap.txt$`)
	chunk := regexp.MustCompile(`^chunk_[0-9]+.txt$`)
	integer := regexp.MustCompile(`[0-9]+`)
	var ftype Filetype
	chunkId := -1
	var err error
	switch {
	case overlap.MatchString(filename):
		ftype = Overlap
		chunkId, err = strconv.Atoi(integer.FindString(filename))
	case chunk.MatchString(filename):
		ftype = Chunk
		chunkId, err = strconv.Atoi(integer.FindString(filename))
	case filepath.Ext(filename) == ".csv":
		ftype = Csv
	case filepath.Ext(filename) == ".json":
		ftype = Json
	default:
		log.Println("not recognized")
		ftype = Unknown
		err = fmt.Errorf("not recognized file %s", filename)
	}
	return ftype, chunkId, err
}

func appendMetadata(metadata *metadata, table string, directory string, filename string, filetype Filetype, chunkId int) error {

	var err error
	t := metadata.Tables[table]

	if t.DataList == nil {
		t.DataList = make(map[string]data)
	}

	d := t.DataList[directory]

	if len(d.Files) == 0 {
		d.Files = make([]string, 0, 20)
	}

	switch filetype {
	case Chunk:
		d.Chunks = append(d.Chunks, chunkId)
	case Overlap:
		d.Overlaps = append(d.Overlaps, chunkId)
	case Csv:
		d.Files = append(d.Files, filename)
	default:
		log.Println("not recognized")
		err = fmt.Errorf("not recognized file %s", filename)
	}

	t.DataList[directory] = d
	metadata.Tables[table] = t

	t.DataList[directory] = d
	metadata.Tables[table] = t
	return err
}

func Cmd() {

	input_dir := "/sps/lsst/groups/qserv/dataloader/stable/idf-dp0.2-catalog-chunked/PREOPS-905"

	metadata := newMetadata()

	visit := func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		//log.Printf("file %s %d", path, info.Size())
		if !info.IsDir() {
			rpath := strings.TrimPrefix(path, input_dir)
			dir, filename := filepath.Split(rpath)
			parts := strings.SplitN(dir, "/", 2)

			tablejson := parts[0]

			// fmt.Println("Dir:", dir) //Dir: /some/path/to/remove/
			// fmt.Println("File:", filename) //File: file.name
			// fmt.Println("Table:", tablejson)

			ftype, chunkId, err := filetype(filename)
			if err != nil {
				return err
			}

			err = appendMetadata(metadata, tablejson, dir, filename, ftype, chunkId)
			if err != nil {
				return err
			}

		}
		return nil
	}

	err := filepath.WalkDir(input_dir, visit)
	if err != nil {
		log.Println(err)
	}

	var f io.Writer = os.Stdout
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(metadata)
	check(err)
}
