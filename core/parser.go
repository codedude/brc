package brc

import (
	"bytes"
	"slices"
)

type MapStation = map[uint64]*StationData

type StationData struct {
	Name []byte
	Min  float64
	Max  float64
	Sum  float64
	Size int
	// mean = Sum/size
}

var patternNl = compilePattern('\n')
var patternSemi = compilePattern(';')

func ParseLines(line []byte, stationMap MapStation) {
	for name_start := 0; name_start < len(line); {
		// slices.Index takes most of the time, even with a simple for loop
		name_end := findIndexOf(line[name_start:min(name_start+104, len(line))], patternSemi) // label = 100 bytes + ;, round to power of 2
		temp_start := name_end + 1
		temp_end := findIndexOf(line[name_start+temp_start:min(name_start+temp_start+8, len(line))], patternNl) // temp = 5 bytes + \n, round to power of 2
		nameSlice := line[name_start : name_start+name_end]
		temp := ParseF64(line[name_start+temp_start : name_start+temp_start+temp_end])

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

func mergeMaps(allStationMaps []MapStation, stationLst *[]*StationData) MapStation {
	baseMap := allStationMaps[0]
	// add unseen station pointer to an array to sort them later
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
