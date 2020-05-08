package main

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go"
)

/*
 	Aerospike component 	Traditional RDBMS concept
	namespace 				tablespace
	set 					table
	record 					row
	bin 					column

	BUT

	Component 	Description
	physical storage 	You can choose the specific type of storage you want for each namespace: NVMEe Flash, DRAM, or PMEM.
						Namespaces can use different types of storage.
						You can also combine them as hybrid storage. The physical storage medium is also called the storage engine.
	namespace 			A namespace is a collection of records. A database can contain multiple namespaces.
						Each namespace can have its own physical storage type.
	record 				A record has a primary key. The primary key is also transformed into a corresponding digest.
	set 				Records can be optionally grouped into sets.
	bin 				A record also has bins. The data in the bin determines the data type of the bin.
						A record can have bins of varying data types. Bins can also be used to create secondary indexes.
*/

/*
LINK: https://www.aerospike.com/docs/architecture/data-model.html

Limitation: The maximum of concurrent unique bin names in a namespace is 32K.

Secondary index: https://www.aerospike.com/docs/architecture/secondary-index.html
*/

func main() {
	client, err := as.NewClient("10.10.0.3", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ns := "test"
	set := "CH2"
	binPath := "path"
	binMeta := "metadata"

	/* drop previous index */
	err = client.DropIndex(nil, ns, set, "IDX_PATH")
	if err != nil {
		log.Fatal("DropIndex ", err)
	}

	/* create secondary index */
	idxTask, err := client.CreateIndex(nil, ns, set, "IDX_PATH", binPath, as.STRING)
	if err != nil {
		log.Fatal("CreateIndex ", err)
	}
	/* TODO use channel ? */
	for {
		done, err := idxTask.IsDone()
		if err != nil {
			log.Fatal("IsDone ", err)
		}
		if done {
			break
		}
		fmt.Println(".")
	}

	/* TODO simulate lot of entries like a directory */
	path := []string{
		"subA/",
		"subA/testA/finalA/",
		"subA/testB/",
		"subB/",
		"subC/testZ/testA/testX/finalC/"}

	for i := range path {
		fmt.Println("i ", i, " ", path[i])
		/* TODO: how create a new namespace */
		key, err := as.NewKey(ns, set, path[i])
		if err != nil {
			log.Fatal("NewKey ", err)
		}
		bin1 := as.NewBin(binPath, path[i])
		bin2 := as.NewBin(binMeta, path[i])
		client.PutBins(nil, key, bin1, bin2)
		if err != nil {
			log.Fatal("PutObject ", err)
		}
	}

	/* retrieve a key */
	key, err := as.NewKey(ns, set, path[2])
	if err != nil {
		log.Fatal("NewKey ", err)
	}
	record, err := client.Get(nil, key, binPath)
	if err != nil {
		log.Fatal("Get ", err)
	}
	fmt.Println("record ", record)

	fmt.Println("======= ScanAll")
	/* scan */
	recordset, err := client.ScanAll(nil, ns, set, binPath)
	if err != nil {
		log.Fatal("ScanAll ", err)
	}
	for rec := range recordset.Results() {
		fmt.Println("> ", rec)
	}
	recordset.Close()

	fmt.Println("======= Query (secondary index, unsorted)")
	/* using secondary index */
	stmt := as.NewStatement(ns, set, binPath)
	/* how set IndexName ? */
	stmt.IndexName = "IDX_PATH"
	recordset, err = client.Query(nil, stmt)
	if err != nil {
		log.Fatal("Query ", err)
	}
	for rec := range recordset.Results() {
		fmt.Println("> ", rec.Record.Bins[binPath])
	}
	recordset.Close()

	fmt.Println("======= Query (secondary index, sorted ?)")
	/* MapPolicy ? */
	// mapPolicy := as.NewMapPolicyWithFlags(as.MapOrder.KEY_ORDERED, 0)
	// client.Operate(null, "xx")
	stmt = as.NewStatement(ns, set, binPath)
	/* how set IndexName ? */
	stmt.IndexName = "IDX_PATH"
	recordset, err = client.Query(nil, stmt)
	/* We should use instead ListWriteFlagsAddUnique instead of FlagDefault */
	listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsDefault)
	tempKey, err := as.NewKey(ns, "temp", "xxx")
	client.Delete(nil, tempKey)
	var tmpRec *as.Record
	for rec := range recordset.Results() {
		tmpRec, err = client.Operate(nil, tempKey, as.ListAppendWithPolicyOp(listPolicy, "xxx", rec.Record.Bins[binPath]))
		if err != nil {
			log.Fatal("Operate 1 ", err)
		}
	}

	if tmpRec != nil {
		/* TODO estimate size */

		if err != nil {
			log.Fatal("Operate 2 ", err)
		}
		/* start at index 2 and return one item */
		tmpRec, err = client.Operate(nil, tempKey, as.ListGetByIndexRangeCountOp("xxx", 2, 1, as.ListReturnTypeValue))
		if err != nil {
			log.Fatal("ListGet ", err)
		}
		fmt.Println(tmpRec.Bins["xxx"])

	}
}
