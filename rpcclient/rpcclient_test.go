package rpcclient

import (
	"fmt"
	"testing"
	//"github.com/vostrok/inmem/server/src/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/vostrok/inmem/service"
)

func init() {
	c := RPCClientConfig{
		DSN:     "localhost:50307",
		Timeout: 10,
	}
	Init(c)
}

func TestGetOperator(t *testing.T) {
	res, err := GetOperatorByCode(41001)
	fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	expected := service.Operator{
		Name:     "mobilink",
		Rps:      10,
		Settings: "{}",
		Code:     41001,
	}
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "operators differ")
	}

	res, err = GetOperatorByName("mobilink")
	fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "operators differ")
	}
}

//func TestGetIPInfo(t *testing.T) {
//	res, err := GetIPInfoByMsisdn("923005557326")
//	fmt.Printf("%#v %#v", res, err)
//
//	res, err = GetIPInfoByIps([]net.IP{})
//	fmt.Printf("%#v %#v", res, err)
//}
//
func TestGetCampaign(t *testing.T) {
	res, err := GetCampaignByHash("f90f2aca5c640289d0a29417bcb63a37")
	fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	expected := service.Campaign{
		Hash:             "f90f2aca5c640289d0a29417bcb63a37",
		Link:             "mobilink-p2",
		PageWelcome:      "9815a83cf640edd402983072a05b8312",
		Id:               290,
		ServiceId:        777,
		AutoClickRatio:   1,
		AutoClickEnabled: false,
		AutoClickCount:   0,
		CanAutoClick:     false,
	}
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Service differs")
	}

	res, err = GetCampaignByLink("mobilink-p2")
	fmt.Printf("%#v %#v\n", res, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Service differs")
	}

}

//
//func TestGetAllCampaigns(t *testing.T) {
//	res, err := GetAllCampaigns()
//	fmt.Printf("%#v %#v", res, err)
//}
//func TestGetServiceById(t *testing.T) {
//	res, err := GetServiceById(777)
//	fmt.Printf("%#v %#v", res, err)
//}
//func TestGetContentById(t *testing.T) {
//	res, err := GetContentById(42)
//	fmt.Printf("%#v %#v", res, err)
//}
//func TestGetPixelSettingByKey(t *testing.T) {
//	res, err := GetPixelSettingByKey("mobusi")
//	fmt.Printf("%#v %#v", res, err)
//}
//func TestGetPixelSettingByKeyWithRatio(t *testing.T) {
//	res, err := GetPixelSettingByKeyWithRatio("mobusi")
//	fmt.Printf("%#v %#v", res, err)
//}
//
//func TestSentContent(t *testing.T) {
//	res, err := SentContentGet("923005557326", 777)
//	fmt.Printf("%#v %#v", res, err)
//
//	err = SentContentPush("923005557326", 777, 42)
//	fmt.Printf("%#v", err)
//
//	res, err = SentContentGet("923005557326", 777)
//	fmt.Printf("%#v %#v", res, err)
//
//	err = SentContentClear("923005557326", 777)
//	fmt.Printf("%#v", err)
//}
//
//func TestPostPaid(t *testing.T) {
//	postPaid, err := IsPostPaid("923005557326")
//	assert.Nil(t, err)
//	assert.Equal(t, false, postPaid, "not postpaid")
//
//	err = PostPaidPush("923005557326")
//	assert.Nil(t, err)
//
//	postPaid, err = IsPostPaid("923005557326")
//	assert.Nil(t, err)
//	assert.Equal(t, true, postPaid, "not postpaid")
//
//	// otherwise will fail each time
//	err = PostPaidRemove("923005557326")
//	assert.Nil(t, err)
//}
//
//func TestBlackListed(t *testing.T) {
//	blackListed, err := IsBlackListed("923005557326")
//	assert.Nil(t, err)
//	assert.Equal(t, false, blackListed, "not blacklisted")
//}
