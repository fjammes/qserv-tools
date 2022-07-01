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
	"flag"

	"github.com/fjammes/qserv-tools/v2/metadata"

	"github.com/rs/zerolog"
)

func main() {

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	debug := flag.Bool("debug", false, "sets log level to debug")
	defaultInputDir := "/sps/lsst/groups/qserv/dataloader/stable/idf-dp0.2-catalog-chunked/PREOPS-905"
	defaultIdxDir := "/sps/lsst/groups/qserv/dataloader/stable/idf-dp0.2-catalog-chunked/PREOPS-905/in2p3/config_indexes"
	defaultOutputFile := "/tmp/metadata.json"
	inputDir := flag.String("path", defaultInputDir, "Path to input data")
	outFile := flag.String("out", defaultOutputFile, "Path to output file")
	idxDir := flag.String("idx", defaultIdxDir, "Path to indexes configuration files")
	flag.Parse()

	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	cfg := metadata.Config{
		DbJsonFile:    "dp02_dc2_catalogs.json",
		OrderedTables: []string{"Object", "Source", "DiaObject", "DiaSource", "CcdVisit", "ForcedSource", "ForcedSourceOnDiaObject", "MatchesTruth", "Visit"},
		IdxDir:        *idxDir,
	}

	metadata.Cmd(*inputDir, *outFile, cfg)
}
