package main

import (
    "log"
    "fmt"
    "strings"

    "apiserver/libs3/goamz/aws"
    "apiserver/libs3/goamz/s3"
)

func main() {
    //auth, err := aws.EnvAuth()
    //if err != nil {
    //    log.Fatal(err)
    //}
    auth := aws.Auth{
        AccessKey: "OE4YUWD85Q2P2AQH1B53",
        SecretKey: "ufxV5NrvQSR1cV5wcUSrB2gF5HN4CiRl3A13W7ZB",
        Token: "",
    }
    s := s3.New(auth, aws.USEast)
    b := s.Bucket("app_E785F82AE89348FE930142910D464039_mybucket123_bucket")

    //keys := []string{
    //    "a001173e2bdc30f575652835cb912159",
    //}
    //for _, key := range keys {
    //    testGetMulti(b, key)
    //}

    const objkey = "40破解版@23"
    //uploadid := "aaa"
    ////testListBuckets(s)
    var metadata map[string][]string
    uploadid := testInitMultiUpload(b, objkey, metadata)

    //testGetMulti(b)

    testMulti(b, objkey, uploadid)
}

func testGetMulti(b *s3.Bucket, key string) {
    multis, prefixs, err := b.ListMulti(key, "")
    if err != nil {
        log.Fatalf("in testGetMulti: ListMulti error: %s", err.Error())
    }
    log.Printf("in testGetMulti: prefixs=%#v, len(multis)=%d", prefixs, len(multis))
    for _, m := range multis {
        log.Printf("in testGetMulti: multi.Key=%v, multi.UploadId=%v", m.Key, m.UploadId)
    }
}

func testMulti(b *s3.Bucket, objkey, uploadid string) {
    m := &s3.Multi {
        Bucket: b,
        Key: objkey,
        UploadId: uploadid,
    }

    testPutParts(m)
    //parts := testListParts(m)
    //testComplete(m, parts)

    //testListParts(m)
    //testAbort(m)
    //testListParts(m)
}

func testComplete(m *s3.Multi, parts []s3.Part) {
    err := m.Complete(parts)
    if err != nil {
        log.Fatalf("in testComplete, Complete error: %s", err.Error())
    }
    log.Println("in testComplete, Complete ok")
}

func testAbort(m *s3.Multi) {
    err := m.Abort()
    if err != nil {
        log.Fatalf("in testAbort, Abort error: %s", err.Error())
    }
    log.Println("in testAbort, Abort ok")
}

func testPutParts(m *s3.Multi) {
    for i := 0; i < 1; i++ {
        p, err := m.PutPart(i + 1, strings.NewReader(fmt.Sprintf("<part %d>", i + 1)))
        if err != nil {
            log.Fatalln("in testPutParts, PutPart error: %s", err.Error())
        }
        log.Printf("in testPutParts, no:%d, size:%d, etag:%s", p.N, p.Size, p.ETag)
    }
}

func testListParts(m *s3.Multi) []s3.Part {
    parts, err := m.ListParts()
    if err != nil {
        log.Fatalf("in testListParts, ListParts error: %s", err.Error())
    }
    for _, p := range parts {
        log.Printf("in testListParts, no:%d, size:%d, etag:%s", p.N, p.Size, p.ETag)
    }
    return parts
}

func testInitMultiUpload(b *s3.Bucket, objkey string,metadata map[string][]string) string {
    multi, err := b.InitMulti(objkey, "binary/octet-stream", s3.PublicReadWrite,metadata)
    if err != nil {
        log.Fatalln(err.Error())
    }
    log.Printf("testInitMultiUpload ok: %#v", multi)
    return multi.UploadId
}

func testListBuckets(client *s3.S3) {
    resp, err := client.ListBuckets()

    if err != nil {
        log.Fatal(err)
    }

    log.Print(fmt.Sprintf("%T %+v", resp.Buckets[0], resp.Buckets[0]))
}
