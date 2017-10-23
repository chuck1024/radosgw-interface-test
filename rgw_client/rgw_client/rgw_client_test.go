package rgw_client
import (
	"testing"
	"fmt"

	"rgw-interface-test/rgw_client/sign"
)

var endpoint = "http://123.58.34.245:7481"
var auth = sign.Auth{"ATVXWYWYU6EI8YR4VIPF","sgOKC721nVZOHA09s38f707R3wEbh1GnmBdANvIC"}

func Test_CreateBucket(t *testing.T) {
	fmt.Println("Test_CreateKey")
	

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint
		
	err := rgwClient.CreateBucket("mytestbucket", "public-read-write")
	if (err != nil) {
		t.Error("CreateBucket failed.error:" + err.Error())
	} 	
}

func Test_GetBucket(t *testing.T) {
	fmt.Println("Test_GetBucket")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	buckList, err := rgwClient.GetBucket( )
	if (err != nil) {
		t.Error("GetBucket failed.error:" + err.Error())
	} else {		
		for i, bucket := range buckList {
			fmt.Println("index:",i, "name:", bucket.Name, ",CreationDate:", bucket.CreationDate)
		}
	}
}

func Test_CreateUser(t *testing.T) {
	fmt.Println("Test_CreateUser")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	userinfo, err := rgwClient.CreateUser("aa", "aa", "buckets=read;data=*;metadata=*", 1)
	if (err != nil) {
		t.Error("CreateUser failed.error:" + err.Error())
	} else {
		fmt.Println("userinfo:", userinfo)
	}
}

func Test_ModifyUser(t *testing.T) {
	fmt.Println("Test_ModifyUser")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	userinfo, err := rgwClient.ModifyUser("aa", "aa", "buckets=read;data=*;metadata=*", 1)
	if (err != nil) {
		t.Error("ModifyUser failed.error:" + err.Error())
	} else {
		fmt.Println("userinfo:", userinfo)
	}
}


func Test_AddUserCaps(t *testing.T) {
	fmt.Println("Test_AddUserCaps")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	caps,err := rgwClient.AddUserCaps("aa", "buckets=write")
	if (err != nil) {
		t.Error("AddUserCaps failed.error:" + err.Error())
	} else {
		fmt.Println("caps:", caps)
	}
}

func Test_RemoveUserCaps(t *testing.T) {
	fmt.Println("Test_RemoveUserCaps")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	caps,err := rgwClient.RemoveUserCaps("aa", "buckets=write")
	if (err != nil) {
		t.Error("RemoveUserCaps failed.error:" + err.Error())
	} else {
		fmt.Println("caps:", caps)
	}
}



func Test_CreateKey(t *testing.T) {
	fmt.Println("Test_CreateKey")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	keys,err := rgwClient.CreateKey("aa")
	if (err != nil) {
		t.Error("AddUserCaps failed.error:" + err.Error())
	} else {
		fmt.Println("keys:", keys)
	}
	
	for _, v := range keys {
		removeKeyErr := rgwClient.RemoveKey("aa", v.Access_key)
		if removeKeyErr != nil {
			fmt.Println("RemoveKey failed")
		} else {
			fmt.Println("RemoveKey ok.key:",v.Access_key)
		}			
	}
}

func Test_SetUserQuota(t *testing.T) {
	fmt.Println("Test_SetUserQuota")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint
	
	var userQuota Quota
	userQuota.Enabled = true
	userQuota.Max_size_kb = 10000
	userQuota.Max_objects = 100
	err := rgwClient.SetUserQuota("aa", userQuota)
	if (err != nil) {
		t.Error("SetUserQuota failed.error:" + err.Error())
	}
}

func Test_GetUserQuota(t *testing.T) {
	fmt.Println("Test_GetUserQuota")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	userQuota, err := rgwClient.GetUserQuota("aa")
	if (err != nil) {
		t.Error("GetUserQuota failed.error:" + err.Error())
	}
	
	fmt.Println("user quota enable:", userQuota.Enabled, ",Max_size_kb:", userQuota.Max_size_kb, ",Max_objects:", userQuota.Max_objects)
}

func Test_GetUserUsage(t *testing.T) {
	fmt.Println("Test_GetUserUsage")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	userUsage,err := rgwClient.GetUserUsage("admin")
	if (err != nil) {
		t.Error("Test_GetUserUsage failed.error:" + err.Error())
	}
	
	fmt.Println("userUsage.NumObject:", userUsage.NumObject, ",userUsage.SizeKb:", userUsage.SizeKb, ",userUsage.SizeKbActual:", userUsage.SizeKbActual)
}

func Test_SetBucketQuota(t *testing.T) {
	fmt.Println("Test_SetBucketQuota")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint
	
	var bucketQuota Quota
	bucketQuota.Enabled = true
	bucketQuota.Max_size_kb = 1000
	bucketQuota.Max_objects = 1000

	err := rgwClient.SetBucketQuota("aa", bucketQuota)
	if (err != nil) {
		t.Error("SetBucketQuota failed.error:" + err.Error())
	}
}

func Test_GetBucketQuota(t *testing.T) {
	fmt.Println("Test_GetBucketQuota")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	bucketQuota, err := rgwClient.GetBucketQuota("aa")
	if (err != nil) {
		t.Error("GetBucketQuota failed.error:" + err.Error())
	}
	
	fmt.Println("bucket quota enable:", bucketQuota.Enabled, ",Max_size_kb:", bucketQuota.Max_size_kb, ",Max_objects:", bucketQuota.Max_objects)
}


func Test_GetUserInfo(t *testing.T) {
	fmt.Println("Test_GetUserInfo")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	userinfo, err := rgwClient.GetUserInfo("aa")
	if (err != nil) {
		t.Error("GetUserInfo failed.error:" + err.Error())
	} else {
		fmt.Println("userinfo:", userinfo)
	}
    
	userinfo, err = rgwClient.GetUserInfo("ba")
	if (err != nil) {
		t.Error("GetUserInfo failed.error:" + err.Error())
	} else {
		fmt.Println("userinfo:", userinfo)
	}

}

func Test_PutObject(t *testing.T) {
	fmt.Println("Test_PutObject")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint
	var content string = "abcde"
	err := rgwClient.PutObject("mytestbucket", "test.txt", &content, true)
	if (err != nil) {
		t.Error("PutObject failed.error:" + err.Error())
	} else {
		fmt.Println("PutObject ok")
	}
}


func Test_GetObject(t *testing.T) {
	fmt.Println("Test_GetObject")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	result,err := rgwClient.GetObject("mytestbucket", "test.txt")
	if (err != nil) {
		t.Error("GetObject failed.error:" + err.Error())
	} else {
		fmt.Println("content:", string(result))
	}
}

func Test_SetObjectPublicRead(t *testing.T ) {
	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint
	err := rgwClient.SetObjectPublicRead("admin", "mytestbucket", "test.txt")
	if (err != nil) {
		t.Error("SetObjectPublicRead failed.error:" + err.Error())
	}
}


func Test_RevokeObjectPublicRead(t *testing.T ) {
	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint
	err := rgwClient.RevokeObjectPublicRead("admin", "mytestbucket", "test.txt")
	if (err != nil) {
		t.Error("RevokeObjectPublicRead failed.error:" + err.Error())
	}
}

func Test_RemoveObject(t *testing.T) {
	fmt.Println("Test_RemoveObject")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	err := rgwClient.RemoveObject("mytestbucket", "test.txt")
	if (err != nil) {
		t.Error("RemoveObject failed.error:" + err.Error())
	} else {
		fmt.Println("RemoveObject ok")
	}
}

func Test_RemoveBucket(t *testing.T) {
	fmt.Println("Test_RemoveBucket")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	err := rgwClient.RemoveBucket("mytestbucket")
	if (err != nil) {
		t.Error("RemoveBucket failed.error:" + err.Error())
	} else {
		fmt.Println("RemoveBucket ok")
	}
}

func Test_RemoveUser(t *testing.T) {
	fmt.Println("Test_RemoveUser")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	err := rgwClient.RemoveUser("aa", true)
	if (err != nil) {
		t.Error("RemoveUser failed.error:" + err.Error())
	}
}

func Test_ActivityUser(t *testing.T)  {
	fmt.Println("Test_ActivityUser")

	var rgwClient RgwClient
	rgwClient.Auth = auth
	rgwClient.Endpoint = endpoint

	_, err := rgwClient.ActivityUser("wuwei", "true")
	if (err != nil) {
		t.Error("ActivityUser failed.error:" + err.Error())
	}
}




