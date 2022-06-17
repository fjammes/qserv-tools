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
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type SkippedQueries struct {
	CaseId string
	items  []string
}

func (t *SkippedQueries) descend(node *yaml.Node) error {
	switch node.Kind {
	case yaml.SequenceNode:
		for _, item := range node.Content {
			t.descend(item)
		}
	case yaml.MappingNode:
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]
			if key.Kind != yaml.ScalarNode || key.Value != "id" {
				log.Printf("%v", key.Value)
				t.descend(value)
				continue
			}
			if value.Kind != yaml.ScalarNode {
				return errors.New("encountered non-scalar task")
			}
			if value.Value == t.CaseId {
				log.Printf("FOUND")
			}
		}
	}
	return nil
}

func (t *SkippedQueries) UnmarshalYAML(value *yaml.Node) error {
	t.items = nil
	return t.descend(value)
}

func readConf(filename string, caseId string) error {

	var t SkippedQueries
	t.CaseId = caseId

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(buf, &t)
	if err != nil {
		return fmt.Errorf("in file %q: %v", filename, err)
	}

	for _, item := range t.items {
		fmt.Printf("%v", item)
	}

	return nil
}

func main() {

	var sqlFiles []fs.FileInfo

	qserv_src_path := "/home/fjammes/src/qserv/"
	caseId := "case01"

	query_path := filepath.Join(qserv_src_path, "itest_src", "datasets", caseId, "queries")

	conf_file := filepath.Join(qserv_src_path, "src", "admin", "etc", "integration_tests.yaml")
	err := readConf(conf_file, caseId)
	//	log.Printf("yaml: %v", c)
	check(err)

	files, err := ioutil.ReadDir(query_path)
	check(err)

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sql") {
			sqlFiles = append(sqlFiles, file)
			// log.Printf("Appending %v\n  ", file.Name())
		}
	}

	f, err := os.Create("/tmp/dbbench.ini")
	check(err)

	defer f.Close()
	w := bufio.NewWriter(f)
	i := 0
	for _, file := range sqlFiles {

		id := fmt.Sprintf("[%d]\n", i)
		_, err := w.WriteString(id)
		check(err)
		comment := fmt.Sprintf("; %s\n", file.Name())
		_, err = w.WriteString(comment)
		check(err)
		filename := filepath.Join(query_path, file.Name())

		sql := getSQL(filename)
		query := fmt.Sprintf("query=%s\n", sql)
		_, err = w.WriteString(query)
		check(err)

		query_results_file := fmt.Sprintf("query-results-file=/tmp/dbbench/%d.csv\n", i)
		_, err = w.WriteString(query_results_file)

		_, err = w.WriteString("count=1\n\n")

		i++
		w.Flush()
	}

	log.Println("WARNING: Remove manually queries listed in 'skip_number' section of https://github.com/lsst/qserv/blob/main/src/admin/etc/integration_tests.yaml")

}

func getSQL(filename string) string {
	sql := ""
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	check(err)
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text() // GET the line string
		data := strings.TrimSuffix(line, "\n")
		// Remove ending comment
		data = strings.Split(data, "--")[0]
		data = strings.Split(data, ";")[0]
		data = strings.TrimSpace(data)
		if len(data) != 0 {
			data += " "
			space := regexp.MustCompile(`\s+`)
			//log.Printf("data1 '%v' '%s'\n  ", data, sql)
			data = space.ReplaceAllString(data, " ")
			sql += data
		}
	}
	//log.Printf("SQL '%v'\n  ", sql)
	sql = strings.TrimSuffix(sql, " ")

	check(sc.Err())
	return sql
}
