package rgw_client

import (
	"net/http"
	"fmt"
	"net"
	"time"
	"errors"
	"io/ioutil"
	"encoding/json"
	"encoding/xml"
	"strconv"
	"strings"
	"crypto/md5"
	"encoding/base64"
	"logging"
	"rgw-interface-test/rgw_client/sign"
	"rgw-interface-test/libs3/goamz/aws"
	"rgw-interface-test/libs3/goamz/s3"
	"os"
	"io"
	"bytes"
)

var dailTimeOut = time.Duration(10)
var b64 = base64.StdEncoding
var ERR_NOT_FOUND = errors.New("not found")
var ERR_BUCKET_ALREADY_EXISTS = errors.New("BucketAlreadyExists")

type RgwClient struct {
	Auth sign.Auth
	Endpoint string
}

type ListAllMyBucketsResult struct {
	xmlns string	`xml:"xmlns,attr"`
	Owner  Owner	`xml:"Owner"`
	Buckets Buckets	`xml:"Buckets"`
}

type Owner struct {
	ID string 	`xml:"ID"`
	DisplayName string	`xml:"DisplayName"`
}

type Buckets struct {
	Bucket []Bucket
}

type Bucket struct {
	Name string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}



func (rgw_client *RgwClient)request(method,  path string, headers, params map[string][]string, content *string) ([]byte , int, string, error) {
	
	headers["x-amz-date"] = []string{time.Now().In(time.FixedZone("GMT", 0)).Format(time.RFC1123)}
	
	sign.Sign(rgw_client.Auth, method, path, params, headers)
	path = sign.AmazonEscape(path)
	
	url := rgw_client.Endpoint + path
	if len(params) > 0 {
		url +="?"
	}
	first := true
	for k, v := range params {
		if !first {
			url += "&"
		}		
		if len(v) > 0{
			url = url + k + "=" + v[0]	
		} else {
			url = url + k
		}
		
		first = false
	}
	
	var req *http.Request
	var newReqErr error
	if content != nil {
		req, newReqErr = http.NewRequest(method, url, strings.NewReader(*content))
	} else {
		req, newReqErr = http.NewRequest(method, url, nil)
	}

	if newReqErr != nil {
		logging.Error("http.NewRequest error. error:" + newReqErr.Error() + ". method:" + method + ". url:" + url)
		return []byte{}, 0, "", errors.New("http.NewRequest error. error:" + newReqErr.Error())
	}

	//// 如果用户没有指定 connection, 就默认使用 Keep-Alive
	//connection := false
	//for k := range headers {
	//	if strings.ToLower(k) == "connection" {
	//		connection = true
	//	}
	//}
	//if !connection {
	//	headers["connection"] = []string{"Keep-Alive"}
	//}
	for k, v := range headers {
		var headcontent string
		for i,item := range v{
			if i != 0{
				headcontent += ", "
			}
			headcontent += item
		}
		req.Header.Add(k,headcontent)		
	}
	
	client := &http.Client{
		Transport: &http.Transport{
        	Dial: func(netw, addr string) (net.Conn, error) {        	
        	c, err := net.DialTimeout(netw, addr, time.Second*dailTimeOut)
        	if err != nil {
            	return nil, err
        	}
        	
        	return c, nil
        	},
			
			DisableKeepAlives: true,
        },
	}
	logging.Debug("rgw recv req:",req)
	response,doReqErr := client.Do(req)
	if doReqErr != nil {
		logging.Error("request error.error:" + doReqErr.Error() + ". method:" + method + ". url:" + url)
		return []byte{}, 0, "", errors.New("request error." + doReqErr.Error())
	}
	logging.Debug("rgw send resp:",response)
	defer response.Body.Close()
	body, readErr := ioutil.ReadAll(response.Body)
	if readErr != nil {		
		logging.Error("ioutil.ReadAll failed.error:" + readErr.Error() + ". method:" + method + ". url:" + url)
		return []byte{}, 0, "", errors.New("ioutil.ReadAll failed.error:" + readErr.Error())
	}
	return body, response.StatusCode, response.Status, nil
}

func newS3(ak, sk string) (*s3.S3, error) {
	auth := aws.Auth{
		AccessKey: ak,
		SecretKey: sk,
		Token: "",
	}
	return s3.New(auth, aws.USEast), nil
}

func newBucket(rgwUid, bucketName, ak, sk string) (*s3.Bucket, error) {
	s, err := newS3(ak, sk)
	if err != nil {
		logging.Error("errors occur while newS3, rgwUid=%s, bukectName=%s, err=%s", rgwUid, bucketName, err.Error())
		return nil, err
	}
	return s.Bucket(bucketName), nil
}

func (rgw_client *RgwClient)GetHead(rgwUid, bucketName ,objKey string)map[string][]string{
	b, err := newBucket(rgwUid, bucketName, rgw_client.Auth.AccessKey,rgw_client.Auth.SecretKey)
	if err != nil {
		logging.Error("errors occur while newBucket, rgwUid=%s, bukectName=%s, objKey=%s, err=%s", rgwUid, bucketName, objKey, err.Error())
		return nil
	}
	head,err := b.Head(objKey)
	if err !=nil{
		logging.Error("error",err)
		return nil
	}
	logging.Debug("head",head)
	return head.Header
}

func (rgw_client *RgwClient)InitMultiUpload(rgwUid, bucketName, objkey string,metadata map[string][]string) string {
	b, err := newBucket(rgwUid, bucketName, rgw_client.Auth.AccessKey,rgw_client.Auth.SecretKey)
	multi, err := b.InitMulti(objkey, "binary/octet-stream", s3.Private,metadata)
	if err != nil {
		logging.Error(err.Error())
	}
	logging.Debug("testInitMultiUpload ok: %#v", multi)
	return multi.UploadId
}

func (rgw_client *RgwClient)UploadParts(rgwUid, bucketName, objkey, uploadid ,path string){
	b,_:= newBucket(rgwUid, bucketName, rgw_client.Auth.AccessKey,rgw_client.Auth.SecretKey)
	m := &s3.Multi {
		Bucket: b,
		Key: objkey,
		UploadId: uploadid,
	}

	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()

	fileInfo,_ := os.Stat(path)
	size := fileInfo.Size()

	partSize := 4 *1024*1024
	partCounts := size/int64(partSize)
	partCount := int(partCounts)
	if (size % int64(partSize)) != 0{
		partCount = partCount + 1
	}

	for i := 0; i < partCount; i++ {
		offset := int64(partSize * i)
		fd.Seek(offset,0)

		var realSize int64
		if int64(partSize) < size - int64(offset) {
			realSize = int64(partSize)
		}else{
			realSize = size - offset
		}

		body := io.LimitReader(fd,realSize)
		buf := new(bytes.Buffer)
		buf.ReadFrom(body)
		data := buf.String()

		p, err := m.PutPart(i + 1, strings.NewReader(data))
		if err != nil {
			logging.Error("in testPutParts, PutPart error: %s", err.Error())
		}
		logging.Debug("in testPutParts, no:%d, size:%d, etag:%s", p.N, p.Size, p.ETag)
	}

	/*
	for i := 0; i < 10; i++ {
		p, err := m.PutPart(i + 1, strings.NewReader(fmt.Sprintf("<part %d>", i + 1)))
		if err != nil {
			logging.Error("in testPutParts, PutPart error: %s", err.Error())
		}
		logging.Debug("in testPutParts, no:%d, size:%d, etag:%s", p.N, p.Size, p.ETag)
	}
	*/
}

func (rgw_client *RgwClient)ListParts(rgwUid, bucketName, objkey, uploadid string)[]s3.Part{
	b,_:= newBucket(rgwUid, bucketName, rgw_client.Auth.AccessKey,rgw_client.Auth.SecretKey)
	m := &s3.Multi {
		Bucket: b,
		Key: objkey,
		UploadId: uploadid,
	}
	parts, err := m.ListParts()
	if err != nil {
		logging.Error("in testListParts, ListParts error: %s", err.Error())
	}
	for _, p := range parts {
		logging.Debug("in testListParts, no:%d, size:%d, etag:%s", p.N, p.Size, p.ETag)
	}
	return parts
}

func (rgw_client *RgwClient)Abort(rgwUid, bucketName, objkey, uploadid string){
	b,_:= newBucket(rgwUid, bucketName, rgw_client.Auth.AccessKey,rgw_client.Auth.SecretKey)
	m := &s3.Multi {
		Bucket: b,
		Key: objkey,
		UploadId: uploadid,
	}
	err := m.Abort()
	if err != nil {
		logging.Error("in testAbort, Abort error: %s", err.Error())
	}
	logging.Debug("in testAbort, Abort ok")
}

func (rgw_client *RgwClient)CompleteUpload(rgwUid, bucketName, objkey, uploadid string,parts []s3.Part){
	b,_:= newBucket(rgwUid, bucketName, rgw_client.Auth.AccessKey,rgw_client.Auth.SecretKey)
	m := &s3.Multi {
		Bucket: b,
		Key: objkey,
		UploadId: uploadid,
	}
	err := m.Complete(parts)
	if err != nil {
		logging.Error("in testComplete, Complete error: %s", err.Error())
	}
	logging.Debug("in testComplete, Complete ok")
}

type UserInfo struct {
	User_id string 
	Display_name string 
	Email string	
	Suspended int	
	Max_buckets int	
	Keys []Key
	Caps []Caps	
}

type Key struct {
	User string
	Access_key string
	Secret_key string
}

type Caps struct {
	Type string 
	Perm string
}

type Quota struct {
	Enabled bool `xml:"enabled"`
	Max_size_kb int  `xml:"max_size_kb"`
	Max_objects int  `xml:"max_objects"`
}

type BucketUsage struct {
	Rgw_main struct {
		 Size_kb        int  `json:"size_kb, omitempty"`
		 Size_kb_actual int  `json:"size_kb_actual, omitempty"`
		 Num_objects    int  `json:"num_objects, omitempty"`
	} `json:"rgw.main, omitempty"`

	Rgw_multimeta struct {
		 Size_kb        int  `json:"size_kb, omitempty"`
		 Size_kb_actual int  `json:"size_kb_actual, omitempty"`
		 Num_objects    int  `json:"num_objects, omitempty"`
	} `json:"rgw.multimeta, omitempty"`
}

type BucketQuota struct {
	Enabled     bool    `json:"enabled, omitempty"`
	Max_size_kb int  `json:"max_size_kb, omitempty"`
	Max_objects int  `json:"max_objects, omitempty"`
}

type BucketStatistic struct {
	Bucket          string  `json:"bucket, omitempty"`
	Pool            string  `json:"pool, omitempty"`
	Index_pool      string  `json:"index_pool, omitempty"`
	Id              string  `json:"id, omitempty"`
	Marker          string  `json:"marker, omitempty"`
	Owner           string  `json:"owner, omitempty"`
	Ver             string  `json:"ver, omitempty"`
	Master_ver      string  `json:"master_ver, omitempty"`
	Mtime           string  `json:"mtime, omitempty"`
	Max_marker      string  `json:"max_marker, omitempty"`
	Usage           BucketUsage `json:"usage, omitempty"`
	Bucket_quota    BucketQuota `json:"bucket_quota, omitempty"`
}

type BucketStatisticUsage struct {
	Entries []struct{
		Owner string `json:"owner, omitempty"`
		Buckets []struct{
			Bucket string `json:"bucket, omitempty"`
			Time   string `json:"time,omitempty"`
			Epoch	int `json:"epoch, omitempty"`
			Categories	[]struct{
				Category string `json:"category, omitempty"`
				Bytes_sent int 	`json:"bytes_sent, omitempty"`
				Bytes_received int `json:"bytes_received, omitempty"`
				Ops int `json:"ops, omitempty"`
				Successful_ops int `json:"successful_ops,omitempty"`
			}`json:"categories, omitempty"`
		}`json:"buckets, omitempty"`
	}`json:"entries, omitempty"`
}

func (rgwClient *RgwClient) GetBucket()([]Bucket, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	
	body, statusCode, status, reqErr := rgwClient.request("GET", "/", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to GetBucket(): code=%d, msg=%s", statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	var bucketList ListAllMyBucketsResult
	xmlErr := xml.Unmarshal(body, &bucketList)
	
	if xmlErr != nil {
		logging.Error("xml.Unmarshal.content:" + string(body) + ".error:" + xmlErr.Error())
		return nil, errors.New("xml.Unmarshal.content:" + string(body) + ".error:" + xmlErr.Error())
	}
	
	return bucketList.Buckets.Bucket, nil
}

func (rgwClient *RgwClient) CreateBucket(bucket, acl string)( error) {
	headers := map[string][]string{}
	headers["x-amz-acl"] = []string{acl}
	params := map[string][]string{}
	_, statusCode, status, reqErr := rgwClient.request("PUT", "/" + bucket, headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode != 200 {
		if statusCode == 409 {
			return ERR_BUCKET_ALREADY_EXISTS
		}
		logging.Error("failed to CreateBucket(%s): code=%d, msg=%s", bucket, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	return nil
}

func (rgwClient *RgwClient)RemoveBucket(bucket string)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	
	
	_, statusCode, status, reqErr := rgwClient.request("DELETE", "/" + bucket, headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode != 204 {
		if statusCode == 404 {
			return ERR_NOT_FOUND
		} else {
			logging.Error("failed to RemoveBucket(%s): code=%d, msg=%s", bucket, statusCode, status)
			return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
		}
	}
		
	return nil
}

func (rgwClient *RgwClient)SetBucketAcl(bucket, acl string) error {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["acl"] = []string{acl}

	_, statusCode, status, reqErr := rgwClient.request("PUT", "/" + bucket, headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}

	if statusCode != 204 {
		if statusCode == 404 {
			return ERR_NOT_FOUND
		} else {
			logging.Error("failed to SetBucketAcl(%s): code=%d, msg=%s", bucket, statusCode, status)
			return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
		}
	}

	return nil
}

// 获取bucket使用统计
func (rgwClient *RgwClient)GetBucketStatistic(bucket string)(*BucketStatistic, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["bucket"] = []string{bucket}
	params["stats"] = []string{"True"}
	params["format"] = []string{"json"}


	body, statusCode, status, reqErr := rgwClient.request("GET", "/admin/bucket", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}

	if statusCode != 200 {
		if statusCode == 404 {
			return nil, ERR_NOT_FOUND
		} else {
			logging.Error("failed to GetBucketStatistic(%s): code=%d, msg=%s", bucket, statusCode, status)
			return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
		}
	}

	stat := &BucketStatistic{}
	if err := json.Unmarshal(body, stat); err != nil {
		logging.Error("failed to json.Unmarshal response body(%s), error:%s", string(body), err.Error())
		return nil, err
	}

	return stat, nil
}

func (rgwClient *RgwClient)GetBucketStatisticUsage(uid,start,end string)(*BucketStatisticUsage, error){
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["show-entries"] = []string{"true"}
	params["show-summary"] = []string{"false"}
	//params["format"] = []string{"json"}

	if len(start) != 0{
		start = sign.AmazonEscape(start)
		params["start"] = []string{start}
	}
	if len(end) != 0{
		end = sign.AmazonEscape(end)
		params["end"] = []string{end}
	}

	body, statusCode, status, reqErr := rgwClient.request("GET", "/admin/usage", headers, params, nil)
	if reqErr != nil {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}

	if statusCode != 200 {
		logging.Error("failed to GetUsage(%s): code=%d, msg=%s", uid, statusCode, status)
		return nil,fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}

	stat := &BucketStatisticUsage{}
	if err := json.Unmarshal(body, stat); err != nil {
		logging.Error("failed to json.Unmarshal response body(%s), error:%s", string(body), err.Error())
		return nil, err
	}
	return stat, nil
}

func (rgwClient *RgwClient)GetUserInfo(uid string)(*UserInfo, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	body, statusCode, status, reqErr := rgwClient.request("GET", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}

	if statusCode == 404 {
		return nil, nil
	} else if statusCode != 200 {
		logging.Error("failed to GetUserInfo(%s): code=%d, msg=%s", uid, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}

	var userinfo UserInfo
	jsonErr := json.Unmarshal([]byte(body), &userinfo)
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ". error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ". error:" + jsonErr.Error())
	}

	return &userinfo, nil
}

func (rgwClient *RgwClient)CreateUser(uid,display_name,caps string, max_bucket int)(*UserInfo, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["display-name"] = []string{display_name}
	params["user-caps"] = []string{caps}
	params["max-buckets"] = []string{string(strconv.Itoa(max_bucket) )}
	body, statusCode, status, reqErr := rgwClient.request("PUT", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to CreateUser(%s): code=%d, msg=%s", uid, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	var userinfo UserInfo
	jsonErr := json.Unmarshal([]byte(body), &userinfo)
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ". error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ". error:" + jsonErr.Error())
	}

	return &userinfo, nil
}

func (rgwClient *RgwClient)ActivityUser(uid, suspended string) (*UserInfo, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["suspended"] = []string{suspended}
	body, statusCode, status, reqErr := rgwClient.request("POST", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}

	if statusCode != 200 {
		logging.Error("failed to ActivityUser(%s): code=%d, msg=%s", uid, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}

	var userinfo UserInfo
	jsonErr := json.Unmarshal([]byte(body), &userinfo)
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}

	return &userinfo, nil
}

func (rgwClient *RgwClient)ModifyUser(uid,display_name,caps string, max_bucket int)(*UserInfo, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["display-name"] = []string{display_name}
	params["user-caps"] = []string{caps}
	params["max-buckets"] = []string{string(strconv.Itoa(max_bucket) )}
	body, statusCode, status, reqErr := rgwClient.request("POST", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to ModifyUser(%s): code=%d, msg=%s", uid, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	var userinfo UserInfo
	jsonErr := json.Unmarshal([]byte(body), &userinfo)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}

	return &userinfo, nil
}

func (rgwClient *RgwClient)RemoveUser(uid string,purge_data bool)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	if purge_data {
		params["purge-data"] = []string{"True"}
	}
	_, statusCode, status, reqErr := rgwClient.request("DELETE", "/admin/user", headers, params,nil )
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to RemoveUser(%s): code=%d, msg=%s", uid, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	return nil
}

func (rgwClient *RgwClient)CreateKey(uid string)([]Key, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["key"] = []string{}
	
	body, statusCode, status, reqErr := rgwClient.request("PUT", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to CreateKey(%s): code=%d, msg=%s", uid, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	var keys []Key
	jsonErr := json.Unmarshal(body, &keys)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}
	
	return keys, nil
}

func (rgwClient *RgwClient)RemoveKey(uid,access_key string)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["access-key"] = []string{access_key}
	params["key"] = []string{}

	_, statusCode, status, reqErr := rgwClient.request("DELETE", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to RemoveKey(%s, %s): code=%d, msg=%s", uid, access_key, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	return nil
}

func (rgwClient *RgwClient)AddUserCaps(uid, caps string)([]Caps, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["user-caps"] = []string{caps}
	params["caps"] = []string{}
	
	body, statusCode, status, reqErr := rgwClient.request("PUT", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil,reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to AddUserCaps(%s, %s): code=%d, msg=%s", uid, caps, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	var caplist []Caps
	jsonErr := json.Unmarshal(body, &caplist)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}
	
	return caplist, nil
}

func (rgwClient *RgwClient)RemoveUserCaps(uid, caps string) ([]Caps, error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["user-caps"] = []string{caps}
	params["caps"] = []string{}
	
	body, statusCode, status, reqErr := rgwClient.request("DELETE", "/admin/user", headers,params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil, reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to RemoveUserCaps(%s, %s): code=%d, msg=%s", uid, caps, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	var caplist []Caps
	jsonErr := json.Unmarshal(body, &caplist)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return nil, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}
	
	return caplist, nil
}

func (rgwClient *RgwClient)SetUserQuota(uid string, userQuota Quota)( error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["quota-type"] = []string{"user"}
	params["quota"] = []string{}
	
	var reqBody string
	reqBody += "{\"enabled\":"
	if userQuota.Enabled {
		reqBody += "true"
	} else {
		reqBody += "false"
	}
	reqBody += ",\"max_size_kb\":"
	reqBody += strconv.Itoa(userQuota.Max_size_kb)
	reqBody += ",\"max_objects\":"
	reqBody += strconv.Itoa(userQuota.Max_objects)
	reqBody += "}"

	_, statusCode, status, reqErr := rgwClient.request("PUT", "/admin/user", headers, params, &reqBody)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}	
	
	if statusCode != 200 {
		logging.Error("failed to SetUserQuota(%s): code=%d, msg=%s", uid, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	return nil
}

func (rgwClient *RgwClient)GetUserQuota(uid string)( userQuota Quota, err error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["quota-type"] = []string{"user"}
	params["quota"] = []string{}	
	
	body, statusCode, status, err := rgwClient.request("GET", "/admin/user", headers, params, nil)
	if (err != nil) {
		logging.Error("rgwClient.request error.error:" + err.Error())
		return userQuota, err
	}
	
	if statusCode != 200 {
		logging.Error("failed to GetUserQuota(%s): code=%d, msg=%s", uid, statusCode, status)
		return userQuota, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	jsonErr := json.Unmarshal(body, &userQuota)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return userQuota, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}
	return userQuota,nil
}

type BucketUsage1 struct {
	Bucket string `json:"bucket"`
	Usage struct {
		RgwMain struct{
			SizeKb int64 `json:"size_kb"`
			SizeKbActual int64 `json:"size_kb_actual"`
			NumObject int64 `json:"num_objects"`
		} `json:"rgw.main"`
	} `json:"usage"`
}

type UserUsage struct {
	SizeKb int64 `xml:"size_kb"`
	SizeKbActual int64 `xml:"size_kb_actual"`
	NumObject int64 `xml:"num_objects"`
}


func (rgwClient *RgwClient)GetUserUsage(uid string)(userUage UserUsage, err error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["stats"] = []string{"true"}

	
	body, statusCode, status, reqErr := rgwClient.request("GET", "/admin/bucket", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return userUage,reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to GetUserUsage(%s): code=%d, msg=%s", uid, statusCode, status)
		return userUage, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	
	//logging.Error("GetUserUsage body", string(body))
	var bucketUsageList []BucketUsage1
	jsonErr := json.Unmarshal(body, &bucketUsageList)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return userUage,errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}
	
	userUage.NumObject = 0
	userUage.SizeKb = 0
	userUage.SizeKbActual = 0
	
	for  _, bucketUsage := range bucketUsageList {
		//logging.Error("bucket name:",bucketUsage.Bucket, "size:", bucketUsage.Usage.RgwMain.SizeKb)
		userUage.NumObject += bucketUsage.Usage.RgwMain.NumObject
		userUage.SizeKb += bucketUsage.Usage.RgwMain.SizeKb
		userUage.SizeKbActual += bucketUsage.Usage.RgwMain.SizeKbActual
	}
	
	//logging.Error("NumObject:",userUage.NumObject, ",SizeKb:", userUage.SizeKb, ",SizeKbActual:", userUage.SizeKbActual)
	return userUage,nil
}

func (rgwClient *RgwClient)SetBucketQuota(uid string, bucketQuota Quota)( error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["quota-type"] = []string{"bucket"}
	params["quota"] = []string{}
	
	var reqBody string
	reqBody += "{\"enabled\":"
	if bucketQuota.Enabled {
		reqBody += "true"
	} else {
		reqBody += "false"
	}
	reqBody += ",\"max_size_kb\":"
	reqBody += strconv.Itoa(bucketQuota.Max_size_kb)
	reqBody += ",\"max_objects\":"
	reqBody += strconv.Itoa(bucketQuota.Max_objects)
	reqBody += "}"

	_, statusCode, status, reqErr := rgwClient.request("PUT", "/admin/user", headers, params, &reqBody)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to SetBucketQuota(%s): code=%d, msg=%s", uid, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}

	return nil
}

func (rgwClient *RgwClient)GetBucketQuota(uid string)( bucketQuota Quota,err error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	params["uid"] = []string{uid}
	params["quota-type"] = []string{"bucket"}
	params["quota"] = []string{}	
	
	body, statusCode, status, reqErr := rgwClient.request("GET", "/admin/user", headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return bucketQuota,reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to GetBucketQuota(%s): code=%d, msg=%s", uid, statusCode, status)
		return bucketQuota, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
	
	jsonErr := json.Unmarshal(body, &bucketQuota)  
	if jsonErr != nil {
		logging.Error("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
		return bucketQuota, errors.New("json.Unmarshal.content:" + string(body) + ".error:" + jsonErr.Error())
	}
		
	return bucketQuota,nil
}

func (rgwClient *RgwClient)GetObject(bucket, object string)( []byte, error) {
	headers := map[string][]string{}
	params := map[string][]string{}

	body, statusCode, status, reqErr := rgwClient.request("GET", "/" + bucket + "/" + object, headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return nil,reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to GetObject(%s, %s): code=%d, msg=%s", bucket, object, statusCode, status)
		return nil, fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
		
	return body, nil
}

func (rgwClient *RgwClient)PutObject(bucket, object string, content *string, useMd5 bool)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	
	if (useMd5) {
		md5Ctx := md5.New()
    	md5Ctx.Write([]byte(*content))
    	
		contentMd5 := make([]byte, b64.EncodedLen(md5Ctx.Size()))
		b64.Encode(contentMd5, md5Ctx.Sum(nil))
		
		headers["content-md5"]= []string{string(contentMd5)}
	}

	_, statusCode, status, reqErr := rgwClient.request("PUT", "/" + bucket + "/" + object, headers, params, content)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode != 200 {
		logging.Error("failed to PutObject(%s, %s): code=%d, msg=%s", bucket, object, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
		
	return nil
}

func (rgwClient *RgwClient)RemoveObject(bucket, object string)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}


	_, statusCode, status, reqErr := rgwClient.request("DELETE", "/" + bucket + "/" + object, headers, params, nil)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}

	//logging.Info("statusCode=%d, body=%s", statusCode, string(body))
	
	if statusCode != 204 {
		logging.Error("failed to RemoveObject(%s, %s): code=%d, msg=%s", bucket, object, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
		
	return nil
}

func (rgwClient *RgwClient)SetObjectPublicRead(owner, bucket, object string)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	
	var reqBody string
	reqBody += "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
	reqBody += "<Owner>"
	reqBody += "<ID>" + owner + "</ID>"
	reqBody += "</Owner>"
	reqBody += "<AccessControlList>"
	reqBody += "<Grant>"
	reqBody += "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">"
	reqBody += "<ID>" + owner + "</ID>"
	reqBody += "</Grantee>"
	reqBody += "<Permission>FULL_CONTROL</Permission>"
	reqBody += "</Grant>"
	reqBody += "<Grant>"
	reqBody += "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">"
	reqBody += "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>"
	reqBody += "</Grantee>"
	reqBody += "<Permission>READ</Permission>"
	reqBody += "</Grant>"
	reqBody += "</AccessControlList>"
	reqBody += "</AccessControlPolicy>"

	_, statusCode, status, reqErr := rgwClient.request("PUT", "/" + bucket + "/" + object + "?acl", headers, params, &reqBody)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode > 300 {
		logging.Error("failed to SetObjectPublicRead(%s, %s, %s): code=%d, msg=%s", owner, bucket, object, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
		
	return nil
}

func (rgwClient *RgwClient)RevokeObjectPublicRead(owner, bucket, object string)(error) {
	headers := map[string][]string{}
	params := map[string][]string{}
	
	var reqBody string
	reqBody += "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
	reqBody += "<Owner>"
	reqBody += "<ID>" + owner + "</ID>"
	reqBody += "</Owner>"
	reqBody += "<AccessControlList>"
	reqBody += "<Grant>"
	reqBody += "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">"
	reqBody += "<ID>" + owner + "</ID>"
	reqBody += "</Grantee>"
	reqBody += "<Permission>FULL_CONTROL</Permission>"
	reqBody += "</Grant>"	
	reqBody += "</AccessControlList>"
	reqBody += "</AccessControlPolicy>"

	_, statusCode, status, reqErr := rgwClient.request("PUT", "/" + bucket + "/" + object + "?acl", headers, params, &reqBody)
	if (reqErr != nil) {
		logging.Error("rgwClient.request error.error:" + reqErr.Error())
		return reqErr
	}
	
	if statusCode > 300 {
		logging.Error("failed to RevokeObjectPublicRead(%s, %s, %s): code=%d, msg=%s", owner, bucket, object, statusCode, status)
		return fmt.Errorf("rgw error:code=%d, msg=%s", statusCode, status)
	}
		
	return nil
}
