package main

import (
	"slices"
	"sync"

	"github.com/zeebo/xxh3"
)

var counter int = 0
var mutDebug sync.Mutex

type MapStation = map[uint64]*StationData

type StationData struct {
	Name []byte
	Min  float32
	Max  float32
	Sum  float32
	Size int
	// mean = Sum/size
}

func mergeMaps(allStationMaps []MapStation) MapStation {
	baseMap := allStationMaps[0]
	for i := 1; i < len(allStationMaps); i++ {
		newMap := allStationMaps[i]
		for newKey, newValue := range newMap {
			v, ok := baseMap[newKey]
			if !ok { // new
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
	return baseMap
}

func ParseLines(line []byte, stationMap MapStation) {
	// var name_end int // the ';'
	// var temp_start int
	// var temp_end int // temp_start = name_end + 1 (name_end ends on ';', temp_end ends on '\n')

	// var dataMap MapStation = make(MapStation, 32)
	t := 0 // debug
	for name_start := 0; name_start < len(line); {
		// parse data
		name_end := slices.Index(line[name_start:min(len(line), name_start+100)], ';') // label = 100 bytes max
		temp_start := name_end + 1
		temp_end := slices.Index(line[name_start+temp_start:name_start+temp_start+8], '\n') // temp = 5 bytes + \n, round to 8
		if temp_end == -1 {
			temp_end = len(line)
		}
		nameSlice := line[name_start : name_start+name_end]
		// 10% can be won here at most
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
		t += 1 // debug
		name_start += temp_start + temp_end + 1
	}
	// debug
	mutDebug.Lock()
	counter += t
	mutDebug.Unlock()
}

// getHashFromBytes uses xxh3 fast hash
func getHashFromBytes(data []byte) uint64 {
	return xxh3.Hash(data)
}
