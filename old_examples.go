package main

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go"
)

func oldMain() {
	client, err := as.NewClient("10.10.0.3", 3000)
	if err != nil {
		log.Fatal(err)
	}
	/* TODO: how create a new namespace */
	key, err := as.NewKey("test", "set",
		"key value goes here and can be any supported primitive")
	if err != nil {
		log.Fatal("NewKey ", err)
	}

	fmt.Println(key)

	k1 := as.NewBin("key1", "val1")
	k2 := as.NewBin("key2", "val2")

	err = client.PutBins(nil, key, k1, k2)
	if err != nil {
		log.Fatal("PutBins ", err)
	}

	record, err := client.Get(nil, key)
	if err != nil {
		log.Fatal("Get ", err)
	}
	fmt.Println(record.String())
	client.Close()
}

func oldMain2() {
	/* minimal example to set severals keys and read one */
	client, err := as.NewClient("10.10.0.3", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ns := "test"
	set := "CH"

	/* TODO simulate lot of entries like a directory */
	path := []string{
		"subA/",
		"subA/testA/finalA/",
		"subA/testB/",
		"subB/"}

	for i := range path {
		fmt.Println("i ", i, " ", path[i])
		/* TODO: how create a new namespace */
		key, err := as.NewKey(ns, set, path[i])
		if err != nil {
			log.Fatal("NewKey ", err)
		}
		bin1 := as.NewBin("path", path[i])
		bin2 := as.NewBin("metadata", path[i])
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
	client.Delete(nil, key)
	record, err := client.Get(nil, key, "path")
	if err != nil {
		log.Fatal("Get ", err)
	}
	fmt.Println("record ", record)

	/* scan */
	recordset, err := client.ScanAll(nil, ns, set, "path")
	for rec := range recordset.Results() {
		fmt.Println("> ", rec)
	}
	recordset.Close()
}

/* insert few (k, v), reparse them them to create a list (GetListByRange not used yet)) */
func oldMain3() {
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
		recordset, err := client.ScanAll(nil, ns, "temp")
		if err != nil {
			log.Fatal("ScanAll ", err)
		}
		fmt.Println(tmpRec)
		for rec := range recordset.Results() {
			fmt.Println("> ", rec)
		}
	}
}
