package iavl

import (
	"fmt"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start).Nanoseconds()
	fmt.Println(fmt.Sprintf("%s,%d", name, elapsed))
}
