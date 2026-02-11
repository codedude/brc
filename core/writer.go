package brc

import (
	"bytes"
	"fmt"
	"math"
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
			mean := math.Round(station.Sum/float64(station.Size)*100.0) / 100.0
			mean = math.Round(mean*10.0) / 10.0
			min := station.Min
			max := station.Max
			if i < len(stationLst)-1 {
				fmt.Fprintf(&buffer, "%s=%.1f/%.1f/%.1f, ", station.Name, min, mean, max)
			} else {
				fmt.Fprintf(&buffer, "%s=%.1f/%.1f/%.1f", station.Name, min, mean, max)
			}
		}
	}
	buffer.WriteByte('}')
	buffer.WriteByte('\n')
	outFs.Write(buffer.Bytes())
	return nil
}
