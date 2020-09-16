// +build smoke,kerberos

package drill_test

import (
	"context"
	"fmt"
	"log"

	"github.com/zeroshade/go-drill"
)

func Example_kerberos() {
	cl := drill.NewClient(drill.Options{Schema: "dfs.sample", Auth: "kerberos", ClusterName: "drillbits1", SaslEncrypt: true, ServiceName: "drill"}, "localhost:2181", "localhost:2182", "localhost:2183")

	err := cl.Connect(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	dh, err := cl.SubmitQuery(drill.TypeSQL, "SELECT * FROM `nation.parquet`")
	if err != nil {
		log.Fatal(err)
	}

	batch, err := dh.Next()
	for ; err == nil; batch, err = dh.Next() {
		for i := int32(0); i < batch.Def.GetRecordCount(); i++ {
			for _, v := range batch.Vecs {
				val := v.Value(uint(i))
				switch t := val.(type) {
				case []byte:
					fmt.Print("|", string(t))
				default:
					fmt.Print("|", t)
				}
			}
			fmt.Println("|")
		}
	}

	// Output:
	// |0|ALGERIA|0| haggle. carefully f|
	// |1|ARGENTINA|1|al foxes promise sly|
	// |2|BRAZIL|1|y alongside of the p|
	// |3|CANADA|1|eas hang ironic, sil|
	// |4|EGYPT|4|y above the carefull|
	// |5|ETHIOPIA|0|ven packages wake qu|
	// |6|FRANCE|3|refully final reques|
	// |7|GERMANY|3|l platelets. regular|
	// |8|INDIA|2|ss excuses cajole sl|
	// |9|INDONESIA|2| slyly express asymp|
	// |10|IRAN|4|efully alongside of |
	// |11|IRAQ|4|nic deposits boost a|
	// |12|JAPAN|2|ously. final, expres|
	// |13|JORDAN|4|ic deposits are blit|
	// |14|KENYA|0| pending excuses hag|
	// |15|MOROCCO|0|rns. blithely bold c|
	// |16|MOZAMBIQUE|0|s. ironic, unusual a|
	// |17|PERU|1|platelets. blithely |
	// |18|CHINA|2|c dependencies. furi|
	// |19|ROMANIA|3|ular asymptotes are |
	// |20|SAUDI ARABIA|4|ts. silent requests |
	// |21|VIETNAM|2|hely enticingly expr|
	// |22|RUSSIA|3| requests against th|
	// |23|UNITED KINGDOM|3|eans boost carefully|
	// |24|UNITED STATES|1|y final packages. sl|
}
