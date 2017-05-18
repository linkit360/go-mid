package rpcclient

import (
	"net"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/linkit360/go-inmem/service"
	"github.com/stretchr/testify/assert"
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
	//fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	expected := service.Operator{
		Name:          "mobilink",
		Rps:           10,
		Settings:      "{}",
		Code:          41001,
		CountryName:   "pakistan",
		MsisdnHeaders: []string{"HTTP_MSISDN"},
	}
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "operators differ")
	}

	res, err = GetOperatorByName("mobilink")

	assert.Nil(t, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "operators differ")
	}
}

func TestGetIPInfo(t *testing.T) {
	res, err := GetIPInfoByMsisdn("923005557326")
	expected := service.IPInfo{
		IP:            "",
		CountryCode:   92,
		OperatorCode:  41001,
		MsisdnHeaders: []string{"HTTP_MSISDN"},
		Supported:     true,
		Local:         false,
	}
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "ip info differ")
	}

	_, err = GetIPInfoByIps([]net.IP{})
	assert.Error(t, err, "must be error 'Not found'")
	//fmt.Printf("%#v %#v", ipInfos, err)

	realIP := net.ParseIP("182.232.47.163")
	result, err := GetIPInfoByIps([]net.IP{realIP})
	assert.NoError(t, err, "No error for AIS IP")
	expected = service.IPInfo{
		IP:            "182.232.47.163",
		CountryCode:   66,
		OperatorCode:  52001,
		MsisdnHeaders: []string{},
		Range: service.IpRange{
			Id:            222,
			OperatorCode:  52001,
			CountryCode:   66,
			IpFrom:        "182.232.0.0",
			IpTo:          "182.232.255.255",
			MsisdnHeaders: []string{},
		},
		Supported: true,
		Local:     false,
	}
	if !assert.ObjectsAreEqual(expected, result[0]) {
		assert.Equal(t, expected, result[0], "IP Info NOK")
	}
}

func TestGetCampaign(t *testing.T) {
	res, err := GetCampaignByHash("f90f2aca5c640289d0a29417bcb63a37")
	//fmt.Printf("%#v %#v\n", res, err)
	assert.Nil(t, err)
	expected := service.Campaign{
		Hash:             "f90f2aca5c640289d0a29417bcb63a37",
		Link:             "mobilink-p2",
		PageWelcome:      "9815a83cf640edd402983072a05b8312",
		Code:             290,
		ServiceCode:      777,
		AutoClickRatio:   1,
		AutoClickEnabled: true,
		AutoClickCount:   0,
		CanAutoClick:     false,
	}

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
	res, err = GetCampaignByServiceCode(777)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "GetCampaignByServiceId")
	}

}

func TestGetAllCampaigns(t *testing.T) {
	res, err := GetAllCampaigns()
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get all campaigns")
	assert.Equal(t, 2, len(res), "campaigns count")
}

func TestGetServiceById(t *testing.T) {
	res, err := GetServiceByCode(777)
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error while get service by id")
	expected := service.Service{
		Id:                  777,
		Price:               10,
		InactiveDays:        3,
		RetryDays:           10,
		GraceDays:           3,
		PaidHours:           24,
		DelayHours:          1,
		PeriodicAllowedFrom: 510,
		PeriodicAllowedTo:   1410,
		PeriodicDays:        `[]`,
		ContentIds:          []int64{56, 61},
	}

	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Services differ")
	}
	resps, err := GetAllServices()
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get all services")
	assert.Equal(t, 1, len(resps), "services count")
}

func TestGetContentById(t *testing.T) {
	res, err := GetContentById(42)
	assert.Error(t, err, "Must be error 'Not found'")

	res, err = GetContentById(30)
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "No error to get content by id")
	expected := service.Content{
		Id:   30,
		Path: "30.jpg",
		Name: "WWF WALLPAPER 1",
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
	expected := service.PixelSetting{
		Id:            1,
		CampaignCode:  290,
		OperatorCode:  41001,
		Publisher:     "Mobusi",
		Endpoint:      "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:       30,
		Enabled:       true,
		Ratio:         1,
		Count:         0,
		SkipPixelSend: false,
	}
	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Content differ")
	}

	// in case there are was a pointer error and it return same results
	// for different keys
	res, err = GetPixelSettingByKeyWithRatio("290-kimia")
	expected = service.PixelSetting{
		Id:            2,
		CampaignCode:  290,
		OperatorCode:  41001,
		Publisher:     "Kimia",
		Endpoint:      "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:       30,
		Enabled:       true,
		Ratio:         2,
		Count:         0,
		SkipPixelSend: false,
	}
	expected.SkipPixelSend = res.SkipPixelSend
	expected.Count = res.Count
	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "pixel settings differ")
	}
	// in case there are was a pointer error and it return same results
	// for different keys
	res, err = GetPixelSettingByKeyWithRatio("41001-kimia")
	expected.SkipPixelSend = res.SkipPixelSend
	expected.Count = res.Count
	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "pixel settings differ")
	}
	skipPixelSend := !expected.SkipPixelSend

	res, err = GetPixelSettingByKeyWithRatio("290-kimia")
	expected = service.PixelSetting{
		Id:            2,
		CampaignCode:  290,
		OperatorCode:  41001,
		Publisher:     "Kimia",
		Endpoint:      "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:       30,
		Enabled:       true,
		Ratio:         2,
		Count:         0,
		SkipPixelSend: skipPixelSend,
	}
	assert.NoError(t, err, "Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "pixel settings differ")
	}

	res, err = GetPixelSettingByKeyWithRatio("41001-kimia")
	expected = service.PixelSetting{
		Id:            2,
		CampaignCode:  290,
		OperatorCode:  41001,
		Publisher:     "Kimia",
		Endpoint:      "http://kbgames.net:10001/index.php?pixel=%pixel%&msisdn=%msisdn%&trxid=%trxid%&trxtime=%time%&country=%country_name%&operator=%operator_name%",
		Timeout:       30,
		Enabled:       true,
		Ratio:         2,
		Count:         0,
		SkipPixelSend: skipPixelSend,
	}
	assert.NoError(t, err, "41001-kimia Must be no error for correct key")
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "41001-kimia pixel settings differ")
	}
}

func TestSentContent(t *testing.T) {
	res, err := SentContentGet("923005557326", 777)
	//fmt.Printf("%#v %#v", res, err)
	assert.NoError(t, err, "Must be no error")
	expected := map[int64]struct{}(nil)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Sent content differ")
	}

	err = SentContentPush("923005557326", 777, 42)
	assert.NoError(t, err, "Must be no error")
	//fmt.Printf("%#v", err)

	res, err = SentContentGet("923005557326", 777)
	assert.NoError(t, err, "Must be no error")
	expected = make(map[int64]struct{})
	expected[42] = struct{}{}
	//fmt.Printf("%#v %#v", res, err)
	if !assert.ObjectsAreEqual(expected, res) {
		assert.Equal(t, expected, res, "Sent content differ")
	}

	err = SentContentClear("923005557326", 777)
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
	err := SetMsisdnCampaignCache(290, "923005557326")
	assert.Nil(t, err)

	cache, err := GetMsisdnCampaignCache(290, "923005557326")
	assert.Nil(t, err)
	assert.NotEqual(t, int64(290), cache, "is rejected")

	err = SetMsisdnServiceCache(777, "923005557326")
	assert.Nil(t, err)

	isRejected, err := IsMsisdnRejectedByService(777, "923005557326")
	assert.Nil(t, err)
	assert.Equal(t, true, isRejected, "is rejected")
}

func TestUniqUrl(t *testing.T) {
	req := service.ContentSentProperties{
		Msisdn:       "79997777777",
		Tid:          "test tid",
		ServiceCode:  777,
		CampaignCode: 290,
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

func TestGetAllDestinations(t *testing.T) {
	res, err := GetAllDestinations()
	//fmt.Printf("%#v %#v", res, err)

	assert.NoError(t, err, "No error to get all destinations")
	assert.Equal(t, 2, len(res), "destinations count")

	for _, v := range res {
		d := service.Destination{
			DestinationId: 1,
			PartnerId:     1,
			AmountLimit:   0x1e,
			Destination:   "http://linkit360.ru",
			RateLimit:     1,
			PricePerHit:   1,
			Score:         1,
			CountryCode:   92,
			OperatorCode:  41001,
		}
		assert.Equal(t, d, v, "got corect destinations")
		break
	}

}

func TestRedirectStatCounts(t *testing.T) {
	err := IncRedirectStatCount(1)
	assert.NoError(t, err, "No error at inc dest id")

	res, err := GetAllRedirectStatCounts()
	assert.NoError(t, err, "No error to get all redirect stats")
	assert.Equal(t, 1, len(res), "get all redirect stats")

}
