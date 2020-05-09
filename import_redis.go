package main

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/go-redis/redis"
)

/*
convert Redis to Aerospike
CS:AUTH_b0871d19e6554a60ac07c43bc4ccafbb:raw-1563052750:cnt
*/
func convertRedis2Aerospike_as_bins(aero *as.Client, ns, set string) {
	redisCnx := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	zkey := "ZKEY1"
	slc := redisCnx.ZRangeByLex(zkey, redis.ZRangeBy{Min: "-", Max: "+"})
	result, err := slc.Result()
	if err != nil {
		log.Fatal("Result ", err)
	}
	fmt.Println("=> ", len(result))

	key, err := as.NewKey(ns, set, "PATH")
	if err != nil {
		log.Fatal("NewKey ", err)
	}
	aero.Delete(nil, key)
	var tmpRec *as.Record
	for idx, val := range result {
		k1, err := as.NewKey(ns, set, val)
		bin1 := as.NewBin("path", val)
		err = aero.PutBins(nil, k1, bin1)
		if err != nil {
			log.Fatal("PutBins ", idx, " ", err)
		}
	}

	if tmpRec != nil {
		tmpRec.Node.Close()
	}

	redisCnx.Close()
}

func convertRedis2Aerospike_as_list(aero *as.Client, ns, set string) {
	redisCnx := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	zkey := "ZKEY2"
	slc := redisCnx.ZRangeByLex(zkey, redis.ZRangeBy{Min: "-", Max: "+"})
	result, err := slc.Result()
	if err != nil {
		log.Fatal("Result ", err)
	}
	fmt.Println("=> ", len(result))

	key, err := as.NewKey(ns, set, "PATH")
	if err != nil {
		log.Fatal("NewKey ", err)
	}
	aero.Delete(nil, key)
	var tmpRec *as.Record
	/* TODO replace as.ListWriteFlagsDefault to have only uniq keys */
	listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsDefault)
	for idx, val := range result {
		tmpRec, err = aero.Operate(nil, key,
			as.ListAppendWithPolicyOp(listPolicy, "path", val))
		if err != nil {
			/* TODO rework when backend is under pressure */
			fmt.Println("Operate at ", idx, " ", err)
			time.Sleep(time.Duration(1000))
			tmpRec, err = aero.Operate(nil, key,
				as.ListAppendWithPolicyOp(listPolicy, "path", val))
			if err != nil {
				log.Fatal("Operate at ", idx, " ", err)
			}
		}
	}

	if tmpRec != nil {
		tmpRec.Node.Close()
	}

	redisCnx.Close()
}
