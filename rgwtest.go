package main

import (
	"logging"
	"sync"
	"flag"
	"encoding/json"
	"rgw-interface-test/rgw_client/rgw_client"
	"strings"
	"fmt"
	"runtime"
	"os"
	"time"
)

var wg sync.WaitGroup
const ACLPrivate = "private"
var RgwAdmin *rgw_client.RgwClient

func CreateUser(uid string){
	const DEFAULT_USER_CAPS = "buckets=read;"
	userInfo, err := RgwAdmin.CreateUser(uid, "app_" + uid, DEFAULT_USER_CAPS,100)
	if err != nil {
		logging.Error("errors occur while RgwAdmin.CreateUser(%s): %s", uid, err.Error())
		return
	}

	if len(userInfo.Keys) == 0 {
		// create a key
		keys, err := RgwAdmin.CreateKey(uid)
		if err != nil {
			logging.Error("errors occur while RgwAdmin.CreateKey(%s): %s", uid, err.Error())
			return
		}
		if len(keys) > 0 {
			userInfo.Keys = keys
		} else {
			logging.Error("RgwAdmin.CreateKey(%s) return 0 keys", uid)
			return
		}
	}
}

func GetUserInfo(uid string){
	resp,_ := RgwAdmin.GetUserInfo(uid)
	resp1,_ := json.Marshal(resp)
	logging.Debug("resp",string(resp1))
}

func CreateKey(uid string)error{
	keys, err := RgwAdmin.CreateKey(uid)
	if err != nil {
		logging.Error("errors occur while RgwAdmin.CreateKey(%s): %s", uid, err.Error())
		return err
	}
	logging.Debug("keys:",keys)
	return nil
}

func GetUsage(uid,start,end string)error{
	stats, err := RgwAdmin.GetBucketStatisticUsage(uid,start,end)
	if err != nil {
		if err == rgw_client.ERR_NOT_FOUND {
			logging.Debug("uid not found")
			return err
		}
		logging.Error("errors occur while RgwAdmin.GetBucketStatisticUsage(%s): %s", uid, err.Error())
		return err
	}
	stat,err := json.Marshal(stats)
	logging.Debug("uid usage",string(stat))
	return nil
}

func CreateBucket(rgwClientUser rgw_client.RgwClient,uid,bucket string)error{
	acl := ACLPrivate
	err = rgwClientUser.CreateBucket(bucket, acl)
	if err != nil {
		if err == rgw_client.ERR_BUCKET_ALREADY_EXISTS {
			logging.Error("bucket is exist")
			return err
		} else {
			logging.Error("errors occur while RgwAdmin.CreateBucket(%s, %s), ak/sk(%s, %s): %s",
				bucket, acl, rgwClientUser.Auth.AccessKey, rgwClientUser.Auth.SecretKey, err.Error())
			return err
		}
	}
	return nil
}

func DeleteBucket(rgwClientUser rgw_client.RgwClient,bucket string)error{
	if err := rgwClientUser.RemoveBucket(bucket); err != nil {
		logging.Error("delete bucket fail")
		return err
	}
	return nil
}

func GetBucketStatistic(bucket string)error{
	stats, err := RgwAdmin.GetBucketStatistic(bucket)
	if err != nil {
		if err == rgw_client.ERR_NOT_FOUND {
			logging.Debug("bucket not found")
			return err
		}
		logging.Error("errors occur while RgwAdmin.GetBucketStatistic(%s): %s", bucket, err.Error())
		return err
	}
	stat,err := json.Marshal(stats)
	logging.Debug("bucket Statisic",string(stat))
	return nil
}

func UploadObject(rgwClientUser rgw_client.RgwClient,bucket,object string)error{
	content := "chuck-is-handsome"
	if err := rgwClientUser.PutObject(bucket,object,&content,true); err != nil{
		logging.Error("upload object fail")
		return err
	}
	return nil
}

func DownloadObject(rgwClientUser rgw_client.RgwClient,bucket,object string){
	result,err := rgwClientUser.GetObject(bucket,object)
	if err != nil {
		logging.Error("GetObject failed.error:" + err.Error())
	} else {
		logging.Debug("GetObject success")
		f, err := os.Create(object)
		defer f.Close()
		if err != nil{
			logging.Debug("create object file error: %v\n", err)
			return
		}
		f.Write(result)
	}
}

func DeleteObject(rgwClientUser rgw_client.RgwClient,bucket,object string) error {
	if err := rgwClientUser.RemoveObject(bucket, object); err != nil {
		logging.Error("delete object fail")
		return err
	}
	return nil
}

func InitMultiUpload(rgwClientUser rgw_client.RgwClient,uid,bucket,object string)string{
	metadata :=map[string][]string{
		"x-amz-meta-1":{"cc","bb","cb"},
		"x-amz-meta-2":{"yy"},
	}
	uploadId := rgwClientUser.InitMultiUpload(uid,bucket,object,metadata)
	logging.Debug("uploadId = ",uploadId)
	return uploadId
}

func MultiUpload(rgwClientUser rgw_client.RgwClient,uid,bucket,object,objPath string){
	uploadId := InitMultiUpload(rgwClientUser,uid,bucket,object)
	rgwClientUser.UploadParts(uid,bucket,object,uploadId,objPath)
	parts := rgwClientUser.ListParts(uid,bucket,object,uploadId)

	parameter := ""
	fmt.Println("please input paramenter : CompleteMultiUpload, AbortMultiUpload")
	fmt.Scanln(&parameter)

	switch parameter{
	case "CompleteMultiUpload":
		rgwClientUser.CompleteUpload(uid,bucket,object,uploadId,parts)
	case "AbortMultiUpload":
		rgwClientUser.Abort(uid,bucket,object,uploadId)
	}
}

func UploadDownload(rgwClientUser rgw_client.RgwClient,uid,bucket,object,objPath string){
	uploadId := InitMultiUpload(rgwClientUser,uid,bucket,object)
	rgwClientUser.UploadParts(uid,bucket,object,uploadId,objPath)
	parts := rgwClientUser.ListParts(uid,bucket,object,uploadId)
	rgwClientUser.CompleteUpload(uid,bucket,object,uploadId,parts)
	time.Sleep(10*time.Millisecond)
	DownloadObject(rgwClientUser,bucket,object)
}

func ImpressUploadDownload(rgwClientUser rgw_client.RgwClient,uid,bucket,object,objPath string){
	runtime.GOMAXPROCS(runtime.NumCPU())

	for i := 0; i< runtime.NumCPU();i++{
		wg.Add(1)
		go func (){
			UploadDownload(rgwClientUser,uid,bucket,object,objPath)
			wg.Done()
		}()
	}
	wg.Wait()
}

func GetHead(rgwClientUser rgw_client.RgwClient,uid,bucket,object string){
	headers := rgwClientUser.GetHead(uid,bucket,object)
	m := map[string][]string{}
	for k,v := range headers{
		k = strings.ToLower(k)
		if len(k) >= 10{
			if k[:11] == "x-amz-meta-"{
				var headContent string
				for i,item := range v{
					if i != 0{
						headContent += ", "
					}
					headContent += item
				}
				m[k] = append(m[k],headContent)
			}
		}
	}
	logging.Debug("head:=",m)
}

func main(){
	rgwClientUser := rgw_client.RgwClient{}
	// /libs3/gozma/s3/s3.go 767 line need to alter endpoint
	endpoint := flag.String("endpoint","","endpoint")
	ak := flag.String("userAk","","user ak")
	sk := flag.String("userSk","","user sk")

	AdminAk := flag.String("AdminAk","","rgw admin ak")
	AdminSk := flag.String("AdminSk","","rgw admin sk")

	uid := flag.String("uid","","uid")
	start := flag.String("start","","start time")
	end := flag.String("end","","end time")

	bucket := flag.String("bucket","","bucket")
	object := flag.String("object","","object")
	objPath := flag.String("objPath","","object path")

	function := flag.String("func","","CreateUser,GetUserInfo,CreateKey,usage,CreateBucket,DeleteBucket," +
		"GetBucketStatistic,UploadObject,DownloadObject,DeleteObject,GetHead,MultiUpload,ImpressUploadDownload")

	flag.Parse()

	var rgwClientTmp rgw_client.RgwClient
	rgwClientTmp.Auth.AccessKey = *AdminAk
	rgwClientTmp.Auth.SecretKey = *AdminSk
	rgwClientTmp.Endpoint = "http://" + *endpoint
	RgwAdmin = &rgwClientTmp

	rgwClientUser.Endpoint = "http://" + *endpoint
	rgwClientUser.Auth.AccessKey = *ak
	rgwClientUser.Auth.SecretKey = *sk

	switch *function{
	case "CreateUser":
		CreateUser(*uid)
	case "GetUserInfo":
		GetUserInfo(*uid)
	case "CreateKey":
		CreateKey(*uid)
	case "usage":
		GetUsage(*uid,*start,*end)
	case "CreateBucket":
		CreateBucket(rgwClientUser,*uid,*bucket)
	case "DeleteBucket":
		DeleteBucket(rgwClientUser,*bucket)
	case "GetBucketStatistic":
		GetBucketStatistic(*bucket)
	case "UploadObject":
		UploadObject(rgwClientUser,*bucket,*object)
	case "DownloadObject":
		DownloadObject(rgwClientUser,*bucket,*object)
	case "DeleteObject":
		DeleteObject(rgwClientUser,*bucket,*object)
	case "GetHead":
		GetHead(rgwClientUser,*uid,*bucket,*object)
	case "MultiUpload":
		MultiUpload(rgwClientUser,*uid,*bucket,*object,*objPath)
	case "ImpressUploadDownload":
		ImpressUploadDownload(rgwClientUser,*uid,*bucket,*object,*objPath)
	}
}
