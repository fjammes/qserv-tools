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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

type Filetype int64

const (
	Csv Filetype = iota
	Chunk
	Json
	Overlap
	Tsv
	Unknown
)

type Config struct {
	DbJsonFile    string
	OrderedTables []string
	IdxDir        string
}

type metadata struct {
	Database string `json:"database"`
	// map key is the schema file
	Tables []table `json:"tables"`
}

type table struct {
	Schema  string   `json:"schema"`
	Indexes []string `json:"indexes,omitempty"`
	// map key is the directory
	DataList map[string]data `json:"data"`
}

type data struct {
	Chunks   []int    `json:"chunks,omitempty"`
	Overlaps []int    `json:"overlaps,omitempty"`
	Files    []string `json:"files,omitempty"`
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func logTable(tables map[string]table) {
	for k, v := range tables {
		log.Debug().Str("key", k).Str("value", fmt.Sprintf("Table %v", v)).Msg("")
	}
}

func walkDirs(inputDir string, cfg Config) map[string]table {
	// Ensure inputDir has no trailing slash
	inputDir = filepath.Join(inputDir)

	tables := make(map[string]table)

	// zerolog.SetGlobalLevel(zerolog.Disabled)
	log.Info().Str("Path", inputDir).Msg("Add data files")
	visitData := func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rpath := strings.TrimPrefix(path, inputDir)
			rpath = strings.TrimPrefix(rpath, "/")
			dir, filename := filepath.Split(rpath)

			parts := strings.SplitN(dir, "/", 2)
			tablename := parts[0]

			log.Debug().Str("Directory", dir).Msg("")
			log.Debug().Str("File", filename).Msg("")
			log.Debug().Str("Table", tablename).Msg("")

			ftype, chunkId, err := filetype(filename)
			if err != nil {
				return err
			}
			if ftype == Unknown {
				err = fmt.Errorf("Not recognized file %s", path)
				return err
			}
			if isDataFile(ftype) {
				err = appendMetadata(tables, tablename, dir, filename, ftype, chunkId)
				if err != nil {
					return err
				}
			}

		}
		return nil
	}
	err := filepath.WalkDir(inputDir, visitData)
	if err != nil {
		log.Fatal().AnErr("WalkDir", err).Msg("Error while scanning path")
	}
	// zerolog.SetGlobalLevel(zerolog.DebugLevel)

	log.Info().Str("Path", cfg.IdxDir).Msg("Add index files")
	visitIdx := func(path string, info fs.DirEntry, err error) error {

		if err != nil {
			return err
		}
		found := false
		// log.Printf("file %s", path)
		if !info.IsDir() {
			_, filename := filepath.Split(path)

			log.Debug().Str("IndexFile", filename).Msg("")

			ftype, _, err := filetype(filename)
			if err != nil {
				return err
			}
			if ftype == Json {
				for tablename, tableSpec := range tables {
					// log.Trace().Str("TableName", tablename).Str("TableSpec", fmt.Sprintf("%v", tableSpec)).Msg("")
					re := regexp.MustCompile(fmt.Sprintf("^idx_%s.*\\.json$", tablename))
					if re.MatchString(filename) {
						log.Debug().Str("IndexFile", filename).Str("Regexp", re.String()).Msg("Recognized index file")
						tableSpec.Indexes = append(tableSpec.Indexes, filename)
						tables[tablename] = tableSpec
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("Unable to find a table for index file %s", path)
				}

			} else {
				return fmt.Errorf("Unable to recognize format for file %s", path)
			}

		}
		return nil
	}

	err = filepath.WalkDir(cfg.IdxDir, visitIdx)
	if err != nil {
		log.Fatal().AnErr("WalkDir", err).Msg("Error while scanning path")
	}
	return tables
}

func convert(metadata *metadata, tables map[string]table) {
	metadata.Tables = make([]table, 0, len(tables))
	for tableName, tableSpec := range tables {
		var is_partitioned, is_regular bool
		for dir, dataSpec := range tableSpec.DataList {
			// TODO Check a table does not have both chunk/overlap and files
			if len(dataSpec.Chunks) != 0 || len(dataSpec.Overlaps) != 0 {
				is_partitioned = true
			}
			if len(dataSpec.Files) != 0 {
				is_regular = true
			}
			// Remove Overlap list if equals Chunk list
			if len(dataSpec.Chunks) != 0 && slices.Equal(dataSpec.Chunks, dataSpec.Overlaps) {
				log.Info().Str("Table", tableName).Str("Path", dir).Msg("Remove Overlaps")
				dataSpec.Overlaps = []int(nil)
			}
			tableSpec.DataList[dir] = dataSpec
		}
		if is_partitioned == is_regular {
			log.Fatal().Str("Partitioned", strconv.FormatBool(is_partitioned)).Str("Regular", strconv.FormatBool(is_regular)).Str("Table", tableName).Msg("Error while checking data consistency")
		}
		metadata.Tables = append(metadata.Tables, tableSpec)
	}
}

func newMetadata(inputDir string, cfg Config) *metadata {
	var metadata metadata
	metadata.Database = cfg.DbJsonFile

	tables := walkDirs(inputDir, cfg)
	convert(&metadata, tables)
	return &metadata
}

func isDataFile(category Filetype) bool {
	switch category {
	case
		Csv,
		Chunk,
		Overlap,
		Tsv:
		return true
	}
	return false
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
	case filepath.Ext(filename) == ".tsv":
		ftype = Tsv
	default:
		log.Warn().Msg("not recognized")
		ftype = Unknown

	}
	return ftype, chunkId, err
}

func appendMetadata(tables map[string]table, table string, directory string, filename string, filetype Filetype, chunkId int) error {

	var err error
	t := tables[table]

	if len(t.Schema) == 0 {
		t.Schema = fmt.Sprintf("%s.json", table)
	}

	if t.DataList == nil {
		t.DataList = make(map[string]data)
	}

	d := t.DataList[directory]

	switch filetype {
	case Chunk:
		d.Chunks = append(d.Chunks, chunkId)
	case Overlap:
		d.Overlaps = append(d.Overlaps, chunkId)
	case Csv:
		d.Files = append(d.Files, filename)
	case Tsv:
		d.Files = append(d.Files, filename)
	default:
		msg := fmt.Sprintf("Not recognized file %s", filepath.Join(directory, filename))
		log.Warn().Msg(msg)
		err = fmt.Errorf(msg)
	}

	t.DataList[directory] = d
	tables[table] = t

	t.DataList[directory] = d
	tables[table] = t
	return err
}

func Cmd(inputDir string, outFile string, cfg Config) {

	log.Info().Str("Path", inputDir).Msg("Analyze data directory")

	metadata := newMetadata(inputDir, cfg)

	log.Info().Str("Path", outFile).Msg("Generate JSON file")

	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(metadata)
	check(err)
}
