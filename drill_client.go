package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

func doStuff(db *sql.DB) {
	rows, err := db.Query("show files in hdfs.charts")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	defer func() {
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}()

	// cols, _ := rows.Columns()
	// formatStr := "|%-15s|%-12s|%-7s|%-8s|%-10s|%-15s|%-20s|%-30s|%-30s|\n"
	// fmt.Printf(formatStr, cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8])
	// fmt.Print("|")
	// for i := 0; i < 155; i++ {
	// 	fmt.Print("-")
	// }
	// fmt.Println("|")
	dataFormatStr := "|%-15s|%-12v|%-7v|%-8v|%-10v|%-15v|%-20s|%-30s|%-30s|\n"
	for rows.Next() {

		r := struct {
			name   string
			isDir  bool
			isFile bool
			length int64
			owner  string
			group  string
			perms  string
			access time.Time
			mod    time.Time
		}{}

		if err := rows.Scan(&r.name, &r.isDir, &r.isFile, &r.length, &r.owner, &r.group, &r.perms, &r.access, &r.mod); err != nil {
			log.Fatal(err)
		}

		fmt.Printf(dataFormatStr, r.name, r.isDir, r.isFile, r.length, r.owner, r.group, r.perms, r.access.String(), r.mod.String())
	}
}

func main() {
	db, err := sql.Open("drill", "zk=zookeeper1.example.com,zookeeper2.example.com,zookeeper3.example.com;schema=hdfs;auth=kerberos;service=nidrill;encrypt=true;user=user@EXAMPLE.COM;cluster=drillbits1")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// metadata := dc.MakeMetaRequest()
	// fmt.Println(metadata.GetSqlKeywords())

	doStuff(db)

	time.Sleep(30 * time.Second)

	doStuff(db)
}
