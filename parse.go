package main

import (
	"bytes"
	"slices"

	"github.com/zeebo/xxh3"
)

type MapStation = map[uint64]*StationData

type StationData struct {
	Name []byte
	Min  float32
	Max  float32
	Sum  float32
	Size int
	// mean = Sum/size
}

func mergeMaps(allStationMaps []MapStation, stationLst *[]*StationData) MapStation {
	baseMap := allStationMaps[0]
	for _, v := range baseMap {
		*stationLst = append(*stationLst, v)
	}
	for i := 1; i < len(allStationMaps); i++ {
		newMap := allStationMaps[i]
		for newKey, newValue := range newMap {
			v, ok := baseMap[newKey]
			if !ok { // new
				*stationLst = append(*stationLst, newValue)
				baseMap[newKey] = newValue
			} else { // update
				v.Sum += newValue.Sum
				v.Size += newValue.Size
				if newValue.Min < v.Min {
					v.Min = newValue.Min
				}
				if newValue.Max > v.Max {
					v.Max = newValue.Max
				}
			}
		}
	}
	slices.SortFunc(*stationLst, func(a *StationData, b *StationData) int {
		return bytes.Compare((*a).Name, (*b).Name)
	})
	return baseMap
}

func ParseLines(line []byte, stationMap MapStation) {
	for name_start := 0; name_start < len(line); {
		// slices.Index takes most of the time, even with a simple for loop
		name_end := slices.Index(line[name_start:min(len(line), name_start+101)], ';') // label = 100 bytes + ;
		temp_start := name_end + 1
		temp_end := slices.Index(line[name_start+temp_start:name_start+temp_start+6], '\n') // temp = 5 bytes + \n
		if temp_end == -1 {
			temp_end = len(line)
		}
		nameSlice := line[name_start : name_start+name_end]
		temp := ParseF32(line[name_start+temp_start : name_start+temp_start+temp_end])

		// create/get structure
		nameHash := getHashFromBytes(nameSlice)
		v, ok := stationMap[nameHash]
		if !ok { // new
			r := StationData{
				Sum:  temp,
				Size: 1,
				Min:  temp,
				Max:  temp,
				Name: make([]byte, len(nameSlice)),
			}
			copy(r.Name, nameSlice)
			stationMap[nameHash] = &r
		} else { // update
			v.Sum += temp
			v.Size += 1
			if temp < v.Min {
				v.Min = temp
			}
			if temp > v.Max {
				v.Max = temp
			}
		}
		name_start += temp_start + temp_end + 1
	}
}

// getHashFromBytes uses xxh3 fast hash
func getHashFromBytes(data []byte) uint64 {
	return xxh3.Hash(data)
}
