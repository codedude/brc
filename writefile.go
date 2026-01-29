package main

import (
	"bytes"
	"fmt"
	"os"
)

func writeData(filename string, stationLst []*StationData) error {
	outFs, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o764)
	if err != nil {
		return err
	}
	defer outFs.Close()
	var buffer bytes.Buffer
	buffer.WriteByte('{')
	if len(stationLst) > 0 {
		for i, station := range stationLst {
			mean := station.Sum / float32(station.Size)
			min := station.Min
			max := station.Max
			if i < len(stationLst)-1 {
				buffer.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", station.Name, min, mean, max))
			} else {
				buffer.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station.Name, min, mean, max))
			}
		}
	}
	buffer.WriteByte('}')
	buffer.WriteByte('\n')
	outFs.Write(buffer.Bytes())
	return nil
}
