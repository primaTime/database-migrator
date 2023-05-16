package progress

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Data map[string]*DataItem

type DataItem struct {
	Migrated int64
	Total    int64
}

func Print(tableName string, progress Data, startTime time.Time) {
	p := progress[tableName]
	migratedRows := atomic.LoadInt64(&p.Migrated)
	remainingRows := p.Total - migratedRows
	completionPercentage := float64(migratedRows) / float64(p.Total) * 100

	if migratedRows != 0 {
		elapsedTime := time.Since(startTime)
		timePerRow := elapsedTime / time.Duration(migratedRows)
		estimatedRemainingTime := time.Duration(remainingRows) * timePerRow

		fmt.Printf("Data migration progress for table %s: %d/%d rows migrated (%.2f%%), Estimated time left: %v\n",
			tableName, migratedRows, p.Total, completionPercentage, estimatedRemainingTime)
	} else {
		fmt.Printf("Data migration progress for table %s: %d/%d rows migrated (%.2f%%)",
			tableName, migratedRows, p.Total, completionPercentage)
	}
}
