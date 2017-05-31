package rpcclient

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/linkit360/go-mid/service"
	go_utils_structs "github.com/linkit360/go-utils/structs"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
)

func init() {
	c := ClientConfig{
		DSN:     "localhost:50307",
		Timeout: 10,
	}
	if err := Init(c); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init client")
	}
}

func TestGetOperator(t *testing.T) {
	res, err := GetOperatorByCode(41001)
	assert.Nil(t, err)
	expected := xmp_api_structs.Operator{
		Name:        "mobilink",
		Code:        41001,
		CountryName: "pakistan",
	}
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "operators differ")
	}
}

func TestGetCampaign(t *testing.T) {
	res, err := GetCampaignByHash("f90f2aca5c640289d0a29417bcb63a37")
	//fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	ac := xmp_api_structs.Campaign{
		Hash:             "f90f2aca5c640289d0a29417bcb63a37",
		Link:             "mobilink-p2",
		PageWelcome:      "9815a83cf640edd402983072a05b8312",
		Code:             "290",
		ServiceCode:      "421924601",
		AutoClickRatio:   1,
		AutoClickEnabled: true,
	}
	expected := service.Campaign{}
	expected.Load(ac)
	expected.AutoClickCount = int64(0)
	expected.CanAutoClick = false

	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "GetCampaignByHash")
	}

	res, err = GetCampaignByLink("mobilink-p2")
	assert.Nil(t, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "GetCampaignByLink")
	}
	res, err = GetCampaignByKeyWord("play on")
	//fmt.Printf("%#v %#v\n", res, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "GetCampaignByKeyWord")
	}
	serviceKey := "450455555"
	res, err = GetCampaignByKeyWord(serviceKey[:4])
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "GetCampaignByKeyWord "+serviceKey[:4])
	}
	res, err = GetCampaignByServiceCode("421924601")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "GetCampaignByServiceCode")
	}
}

func TestGetAllCampaigns(t *testing.T) {
	res, err := GetAllCampaigns()
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get all campaigns")
	assert.Equal(t, 2, len(res), "campaigns count")
}

func TestGetServiceByCode(t *testing.T) {
	res, err := GetServiceByCode("421924601")
	////fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "Error while get service by id")
	expected := xmp_api_structs.Service{
		Id:                  "421924601",
		Code:                "421924601",
		Price:               10,
		InactiveDays:        3,
		RetryDays:           10,
		GraceDays:           3,
		PaidHours:           24,
		DelayHours:          1,
		PeriodicAllowedFrom: 510,
		PeriodicAllowedTo:   1410,
		PeriodicDays:        `["any"]`,
		ContentCodes:        []string{"55", "49"},
		SMSOnSubscribe:      "Thank you for subscribe!",
		SMSOnUnsubscribe:    "You have been unsubscribed",
		SMSOnContent:        "Your content here: %s",
	}

	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "421924601 services differ")
	}

	res, err = GetServiceByCode("888")
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "Error while get service by id")
	expected = xmp_api_structs.Service{
		Id:               "888",
		Code:             "888",
		Price:            10,
		InactiveDays:     3,
		RetryDays:        11,
		GraceDays:        3,
		PaidHours:        1,
		DelayHours:       22,
		PeriodicDays:     `[]`,
		ContentCodes:     []string{"56", "61"},
		SMSOnSubscribe:   "Thank you for subscribe!",
		SMSOnUnsubscribe: "You have been unsubscribed",
		SMSOnContent:     "Your content here: %s",
	}

	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "888 services differ")
	}

	resps, err := GetAllServices()
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get all services")
	assert.Equal(t, 2, len(resps), "services count")
}

func TestGetContentByCode(t *testing.T) {
	res, err := GetContentById("42")
	assert.Error(t, err, "Must be error 'Not found'")

	res, err = GetContentById("30")
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get content by id")
	expected := xmp_api_structs.Content{
		Id:    "30",
		Name:  "30.jpg",
		Title: "WWF WALLPAPER 1",
	}
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Content differ")
	}
}

func TestGetPixelSettingByKeyWithRatio(t *testing.T) {
	res, err := GetPixelSettingByKeyWithRatio("mobusi")
	assert.Error(t, err, "Must be error 'Not found' for incorrect key")

	res, err = GetPixelSettingByKeyWithRatio("290-mobusi")
	//fmt.Printf("%#v %#v", res, err)

	ap := xmp_api_structs.PixelSetting{
		Id:           "1",
		CampaignCode: "290",
		OperatorCode: 41001,
		Publisher:    "Mobusi",
		Endpoint:     "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:      30,
		Enabled:      true,
		Ratio:        1,
	}
	expected := service.PixelSetting{}
	expected.Load(ap)
	expected.Count = 0
	expected.SkipPixelSend = false

	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Content differ")
	}

	// ================================================
	// in case there are was a pointer error and it return same results
	// for different keys
	res, err = GetPixelSettingByKeyWithRatio("290-kimia")
	ap = xmp_api_structs.PixelSetting{
		Id:           "2",
		CampaignCode: "290",
		OperatorCode: 41001,
		Publisher:    "Kimia",
		Endpoint:     "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:      30,
		Enabled:      true,
		Ratio:        2,
	}
	expected = service.PixelSetting{}
	expected.Load(ap)
	expected.SkipPixelSend = res.SkipPixelSend
	expected.Count = res.Count
	log.WithField("skip", expected.SkipPixelSend).Debug("check")

	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "pixel settings differ")
	}
	// ================================================
	skipPixelSend := !expected.SkipPixelSend
	res, err = GetPixelSettingByKeyWithRatio("290-kimia")
	//fmt.Printf("%#v %#v", res, err)
	ap = xmp_api_structs.PixelSetting{
		Id:           "2",
		CampaignCode: "290",
		OperatorCode: 41001,
		Publisher:    "Kimia",
		Endpoint:     "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:      30,
		Enabled:      true,
		Ratio:        2,
	}
	expected.Load(ap)
	expected.Count = 0
	expected.SkipPixelSend = skipPixelSend
	log.WithField("skip", expected.SkipPixelSend).Debug("check")
	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "pixel settings differ")
	}
	// ================================================

	// in case there are was a pointer error and it return same results
	// for different keys
	res, err = GetPixelSettingByKeyWithRatio("41001-kimia")
	expected.SkipPixelSend = res.SkipPixelSend
	expected.Count = res.Count

	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "pixel settings differ")
	}

	// ================================================

}

func TestSentContent(t *testing.T) {
	res, err := SentContentGet("923005557326", "421924601")
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "Must be no error")
	expected := map[string]struct{}(nil)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Sent content differ")
	}

	err = SentContentPush("923005557326", "421924601", "42")
	assert.NoError(t, err, "Must be no error")
	//fmt.Printf("%#v", err)

	res, err = SentContentGet("923005557326", "421924601")
	assert.NoError(t, err, "Must be no error")
	expected = make(map[string]struct{})
	expected["42"] = struct{}{}
	//fmt.Printf("%#v %#v", res, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Sent content differ")
	}

	err = SentContentClear("923005557326", "421924601")
	assert.NoError(t, err, "Must be no error")
	//fmt.Printf("%#v", err)
}

func TestPostPaid(t *testing.T) {
	postPaid, err := IsPostPaid("923005557326")
	assert.Nil(t, err)
	assert.Equal(t, false, postPaid, "not postpaid")

	err = PostPaidPush("923005557326")
	assert.Nil(t, err)

	postPaid, err = IsPostPaid("923005557326")
	assert.Nil(t, err)
	assert.Equal(t, true, postPaid, "not postpaid")

	// otherwise will fail each time
	err = PostPaidRemove("923005557326")
	assert.Nil(t, err)
}

func TestBlackListed(t *testing.T) {
	blackListed, err := IsBlackListed("923005557326")
	assert.Nil(t, err)
	assert.Equal(t, false, blackListed, "not blacklisted")
}

func TestRejected(t *testing.T) {
	err := SetMsisdnCampaignCache("290", "923005557326")
	assert.Nil(t, err)

	cache, err := GetMsisdnCampaignCache("290", "923005557326")
	assert.Nil(t, err)
	assert.NotEqual(t, "421924601", cache, "is rejected")

	err = SetMsisdnServiceCache("421924601", "923005557326")
	assert.Nil(t, err)

	isRejected, err := IsMsisdnRejectedByService("421924601", "923005557326")
	assert.Nil(t, err)
	assert.Equal(t, true, isRejected, "is rejected")
}

func TestUniqUrl(t *testing.T) {
	req := go_utils_structs.ContentSentProperties{
		Msisdn:       "79997777777",
		Tid:          "test tid",
		ServiceCode:  "421924601",
		CampaignCode: "290",
		OperatorCode: 410,
		CountryCode:  92,
		UniqueUrl:    "cz3twmoynbq5",
	}
	err := SetUniqueUrlCache(req)
	//fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)

	got, err := GetUniqueUrlCache("cz3twmoynbq5")
	assert.Nil(t, err)

	req.SentAt = got.SentAt
	if !assert.ObjectsAreEqual(req, got) {
		assert.Equal(t, req, got, "ContentSentProperties differs")
	}

	err = DeleteUniqueUrlCache(req)
	assert.Nil(t, err)

	got, err = GetUniqueUrlCache("cz3twmoynbq5")
	assert.Nil(t, err)
	assert.True(t, !assert.ObjectsAreEqual(req, got), "Removed")
}

func TestGetAllPublisher(t *testing.T) {
	res, err := GetAllPublishers()
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get all publishers")
	assert.Equal(t, 3, len(res), "publishers count")
}

//
//func TestGetAllDestinations(t *testing.T) {
//	res, err := GetAllDestinations()
//	//fmt.Printf("%#v %#v", res, err)
//
//	assert.NoError(t, err, "No error to get all destinations")
//	assert.Equal(t, 2, len(res), "destinations count")
//
//	for _, v := range res {
//		d := service.Destination{
//			DestinationId: 1,
//			PartnerId:     1,
//			AmountLimit:   0x1e,
//			Destination:   "http://linkit360.ru",
//			RateLimit:     1,
//			PricePerHit:   1,
//			Score:         1,
//			CountryCode:   92,
//			OperatorCode:  41001,
//		}
//		assert.Equal(t, d, v, "got corect destinations")
//		break
//	}
//
//}
//
//func TestRedirectStatCounts(t *testing.T) {
//	err := IncRedirectStatCount(1)
//	assert.NoError(t, err, "No error at inc dest id")
//
//	res, err := GetAllRedirectStatCounts()
//	assert.NoError(t, err, "No error to get all redirect stats")
//	assert.Equal(t, 1, len(res), "get all redirect stats")
//
//}
