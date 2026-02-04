package main

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"slices"

	"github.com/zeebo/xxh3"
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

func ParseLines(line []byte, stationMap MapStation) {
	for name_start := 0; name_start < len(line); {
		// slices.Index takes most of the time, even with a simple for loop
		name_end := findIndexOf(line[name_start:min(name_start+104, len(line))], ';') // label = 100 bytes + ;
		temp_start := name_end + 1
		temp_end := findIndexOf(line[name_start+temp_start:min(name_start+temp_start+8, len(line))], '\n') // temp = 5 bytes + \n
		nameSlice := line[name_start : name_start+name_end]
		temp := ParseF64(line[name_start+temp_start : name_start+temp_start+temp_end])

		// create/get structure
		nameHash := getHashFromBytes(nameSlice)
		v, ok := stationMap[nameHash]
		if !ok { // new
			r := StationData{
				Sum:  float64(temp),
				Size: 1,
				Min:  temp,
				Max:  temp,
				Name: make([]byte, len(nameSlice)),
			}
			copy(r.Name, nameSlice)
			stationMap[nameHash] = &r
		} else { // update
			v.Sum += float64(temp)
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

// findIndexOf is more efficient when haystack is a divisible by 2
func findIndexOf(haystack []byte, needle byte) int {
	// return slices.Index(haystack, needle)
	pattern := compilePattern(needle)
	var uintSlice []byte
	tmpUint := make([]byte, 8)
	for i := 0; i < len(haystack); i += 8 {
		if i+8 >= len(haystack) { // dont buffer overflow
			// tmpUint used once per function call, is already 0 initilized
			copy(tmpUint, haystack[i:])
			uintSlice = tmpUint
		} else {
			uintSlice = haystack[i : i+8]
		}
		index := firstInstance(binary.BigEndian.Uint64(uintSlice), pattern)
		if index != 8 {
			return i + index
		}
	}
	return -1
}

// https://richardstartin.github.io/posts/finding-bytes
func compilePattern(byteToFind byte) uint64 {
	var pattern uint64 = uint64(byteToFind & 0xFF)
	return pattern |
		(pattern << 8) |
		(pattern << 16) |
		(pattern << 24) |
		(pattern << 32) |
		(pattern << 40) |
		(pattern << 48) |
		(pattern << 56)
}

func firstInstance(word, pattern uint64) int {
	var input uint64 = word ^ pattern
	var tmp uint64 = (input & 0x7F7F7F7F7F7F7F7F) + 0x7F7F7F7F7F7F7F7F
	tmp = ^(tmp | input | 0x7F7F7F7F7F7F7F7F)
	return bits.LeadingZeros64(tmp) >> 3
}
