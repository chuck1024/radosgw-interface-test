package s3

import (
	//"crypto/hmac"
	//"crypto/sha1"
	//"encoding/base64"
	//"log"
	//"sort"
	//"strings"

	"rgw-interface-test/rgw_client/sign"
	"rgw-interface-test/libs3/goamz/aws"
)

//var b64 = base64.StdEncoding
//
//// ----------------------------------------------------------------------------
//// S3 signing (http://goo.gl/G1LrK)
//
//var s3ParamsToSign = map[string]bool{
//	"acl":                          true,
//	"delete":                       true,
//	"location":                     true,
//	"logging":                      true,
//	"notification":                 true,
//	"partNumber":                   true,
//	"policy":                       true,
//	"requestPayment":               true,
//	"torrent":                      true,
//	"uploadId":                     true,
//	"uploads":                      true,
//	"versionId":                    true,
//	"versioning":                   true,
//	"versions":                     true,
//	"response-content-type":        true,
//	"response-content-language":    true,
//	"response-expires":             true,
//	"response-cache-control":       true,
//	"response-content-disposition": true,
//	"response-content-encoding":    true,
//}

//----统一函数调用----
func Sign(auth aws.Auth, method, canonicalPath string, params, headers map[string][]string) {
	sign.SignNew(auth.AccessKey, auth.SecretKey, method, canonicalPath, params, headers)
}
