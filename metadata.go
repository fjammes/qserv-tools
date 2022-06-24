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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Metadata struct {
	Database string `json:"database"`
	Tables   []struct {
		Schema  string   `json:"schema"`
		Indexes []string `json:"indexes"`
		Data    []struct {
			Directory string   `json:"directory"`
			Chunks    []int    `json:"chunks"`
			Overlaps  []int    `json:"overlaps"`
			Files     []string `json:"files"`
		} `json:"data"`
	} `json:"tables"`
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	input_dir := "/sps/lsst/groups/qserv/rubin/previews/dp0.2/idf-dp0.2-catalog-chunked/PREOPS-905/"

	err := filepath.Walk(input_dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			//log.Printf("file %s %d", path, info.Size())
			tmp := strings.TrimPrefix(path, input_dir)
			dir, file := filepath.Split(tmp)
			parts := strings.SplitN(dir, "/", 2)
			fmt.Println("Dir:", dir)   //Dir: /some/path/to/remove/
			fmt.Println("File:", file) //File: file.name
			fmt.Println("Table:", parts[0])
			return nil
		})
	if err != nil {
		log.Println(err)
	}

}
