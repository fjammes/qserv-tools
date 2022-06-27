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
	"strings"
)

type metadata struct {
	Database string `json:"database"`
	// map key is the schema file
	Tables map[string]table `json:"tables"`
}

type table struct {
	Indexes []string `json:"indexes"`
	// map key is the directory
	DataList map[string]Data `json:"data"`
}

type Data struct {
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
	table.DataList = make(map[string]Data)
	return &table
}

func update(data *Data, filename string) {
	data.Files = append(data.Files, filename)
}

func Cmd() {

	fmt.Println("XXXXXXXXXXXX")

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

			table := metadata.Tables[tablejson]
			if table.DataList == nil {
				table.DataList = make(map[string]Data)
			}

			data := table.DataList[dir]
			data.Files = append(data.Files, filename)

			table.DataList[dir] = data

			metadata.Tables[tablejson] = table

			//chunkFile := regexp.MustCompile(`\s+`)
			// //log.Printf("data1 '%v' '%s'\n  ", data, sql)
			// data = space.ReplaceAllString(data, " ")

			// if string
			//fmt.Printf("Data %v\n", data)
			//fmt.Printf("Data %v\n", table.DataList[dir])
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
