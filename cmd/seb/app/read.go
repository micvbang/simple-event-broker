package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/micvbang/go-helpy"
	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/spf13/cobra"
)

var dumpFlags DumpFlags

func init() {
	fs := dumpCmd.Flags()

	fs.StringVarP(&dumpFlags.path, "path", "p", "", "Path to a .record_batch file")
	fs.BoolVarP(&dumpFlags.dumpRecords, "dump-records", "a", false, "Whether to also dump record data")
	fs.IntVarP(&dumpFlags.dumpRecordBytes, "dump-record-bytes", "b", 64, "Number of bytes to dump for each record, 0 for all of them")
}

var dumpCmd = &cobra.Command{
	Use:   "read",
	Short: "Read .record_batch file",
	Long:  "Read contents of .record_batch file",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := serveFlags
		log := logger.NewWithLevel(ctx, logger.LogLevel(flags.logLevel))
		log.Debugf("flags: %+v", flags)

		// TODO: support for folders and multiple files

		absInputPath, err := filepath.Abs(dumpFlags.path)
		if err != nil {
			return fmt.Errorf("failed to get the absolute path: %w", err)
		}

		ext := filepath.Ext(absInputPath)
		if ext != ".record_batch" {
			return fmt.Errorf("only .record_batch files are supported, got '%s'", absInputPath)
		}

		f, err := os.Open(absInputPath)
		if err != nil {
			return fmt.Errorf("opening '%s': %w", absInputPath, err)
		}

		// first two bytes of gzip header are 0x1f8b
		bs := make([]byte, 2)
		err = binary.Read(f, binary.LittleEndian, &bs)
		if err != nil {
			return fmt.Errorf("reading file header of '%s': %w", absInputPath, err)
		}

		// TODO: support for gzipped files

		if slicey.Equal([]byte{0x1f, 0x8b}, bs) {
			return fmt.Errorf("gzipped files not supported yet; please gunzip and try again")
		}
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("seeking in file '%s': %w", absInputPath, err)
		}

		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat'ing file '%s': %w", absInputPath, err)
		}

		p, err := sebrecords.Parse(f)
		if err != nil {
			return fmt.Errorf("opening '%s': %w", absInputPath, err)
		}

		fmt.Printf("Seb file '%s'\n", absInputPath)
		fmt.Printf("Version:\t\t%v\n", p.Header.Version)
		fmt.Printf("Magic bytes:\t\t%v\n", sebrecords.FileFormatMagicBytes == p.Header.MagicBytes)
		fmt.Printf("NumRecords:\t\t%d\n", p.Header.NumRecords)
		fmt.Printf("Timestamp:\t\t%s\n", time.UnixMicro(p.Header.UnixEpochUs))

		batch := sebrecords.NewBatch(make([]uint32, 0, p.Header.NumRecords), make([]byte, 0, fi.Size()))
		fileSize := fi.Size()
		headerSize := p.Header.Size()
		dataSize := fileSize - int64(headerSize)
		fmt.Printf("Total file size:\t%v (%v B)\n", sizey.FormatBytes(fileSize), fileSize)
		fmt.Printf("Header size:\t\t%v (%d B)\n", sizey.FormatBytes(headerSize), headerSize)
		fmt.Printf("Data size: %v\t(%d B)\n", sizey.FormatBytes(dataSize), dataSize)
		err = p.Records(&batch, 0, p.Header.NumRecords)
		if err != nil {
			return fmt.Errorf("reading records: %w", err)
		}

		if dumpFlags.dumpRecords {
			fmt.Printf("Records:\n")
			records := batch.IndividualRecords()

			for i, record := range records {
				dumpBytes := helpy.Clamp(dumpFlags.dumpRecordBytes, 1, len(record))
				if dumpFlags.dumpRecordBytes == 0 {
					dumpBytes = len(record)
				}

				var tail string
				if dumpBytes != len(record) {
					tail = fmt.Sprintf("\t[+%d bytes]", len(record)-dumpBytes)
				}
				fmt.Printf("%d: %s%s\n", i, string(record[:dumpBytes]), tail)
			}
		}
		return nil
	},
}

type DumpFlags struct {
	path            string
	dumpRecords     bool
	dumpRecordBytes int
}
