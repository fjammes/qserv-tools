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

package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
)

type Metadata struct {
	Database string `json:"database"`
	// map key is the schema file
	Tables map[string]Table `json:"tables"`
}

type Table struct {
	Indexes []string `json:"indexes"`
	// map key is the directory
	Data map[string]Data `json:"data"`
}

type Data struct {
	Chunks   []int    `json:"chunks"`
	Overlaps []int    `json:"overlaps"`
	Files    []string `json:"files"`
}

func NewMetadata() *Metadata {
	var metadata Metadata
	metadata.Tables = make(map[string]Table)
	return &metadata
}

func NewTable() *Table {
	var table Table
	table.Data = make(map[string]Data)
	return &table
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	input_dir := "/sps/lsst/groups/qserv/dataloader/stable/idf-dp0.2-catalog-chunked/PREOPS-905"

	metadata := NewMetadata()

	visit := func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		//log.Printf("file %s %d", path, info.Size())
		if !info.IsDir() {
			tmp := strings.TrimPrefix(path, input_dir)
			dir, file := filepath.Split(tmp)
			parts := strings.SplitN(dir, "/", 2)

			tablejson := parts[0]

			// fmt.Println("Dir:", dir)   //Dir: /some/path/to/remove/
			// fmt.Println("File:", file) //File: file.name
			// fmt.Println("Table:", tablejson)

			table, has_table := metadata.Tables[tablejson]
			if !has_table {
				table = *NewTable()
				metadata.Tables[tablejson] = table
			}

			data, has_dir := table.Data[dir]
			if !has_dir {
				data := Data{}
				table.Data[dir] = data
			}
			data.Files = append(data.Files, file)
		}
		return nil
	}

	err := filepath.WalkDir(input_dir, visit)
	if err != nil {
		log.Println(err)
	}

	u, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(u))

}
