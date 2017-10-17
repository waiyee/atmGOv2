package main

import (
	"time"

	"sync"
	"atm/bittrex"

	"atm/db"

	"strings"
)

var minTotal = float64(0.00060000)
var satoshi = float64(0.00000001)
var FinalThresold = 0.4
var SpreadThresold = 0.005
/**
* Loop the order book limit to 8 markets per second
 */
func loopGetOrderBook()  {
	lenOfM := len(BTCMarkets)
	start := 0
	end := -1
	limit := 8
	for t := range time.NewTicker(time.Second).C {
		var OrderMarkets []string
		if end + 1 < lenOfM && end + limit< lenOfM {
			start = end + 1
			end += limit
			OrderMarkets = BTCMarkets[start:end+1]
		} else if end + 1 < lenOfM {
			start = end + 1
			end = lenOfM -1
			OrderMarkets = BTCMarkets[start:end+1]
			end = limit - ( end - start +1 ) -1
			start = 0
			for i:= start ; i <= end ; i ++ {
				OrderMarkets = append(OrderMarkets, BTCMarkets[i])
			}

			for true{
				result := refreshWallet()
				if result {
					break
				}
			}

		} else {
			start = 0
			end = -1 + limit
			OrderMarkets = BTCMarkets[start:end+1]

			for true{
				result := refreshWallet()
				if result {
					break
				}
			}
		}

		go periodicGetOrderBook(t, OrderMarkets)
		JobChannel <- t

	}

}


type WalletBalance struct {
	Available      float64    `json:"available" bson:"available"`
}

type LogOrderBook struct {
	UUID		string
	Market 		string
	LogTime		time.Time
	OrderType	string
	Remark		string
}

type LogMarketFinal struct {
	LogTime time.Time
	MarketName string
	Bid float64
	Ask float64
	Voi float64
	Oir float64
	SPREAD float64
	Mpb float64
	Final float64
	USDT float64
}

type DBHourlyMarket struct {
	MarketName string
	Detail		db.HourMarketRate
}

func periodicGetOrderBook(t time.Time, markets []string)  {
	wg := &sync.WaitGroup{}

	obRate := 0.1

	bapi := bittrex.New(API_KEY, API_SECRET)

	for i := 0; i < len(markets); i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			session := mydb.Session.Clone()
			defer session.Close()

			orderBook, err := bapi.GetOrderBook(markets[i], "both")
			if err != nil {

				e := session.DB("v4").C("ErrorLog").With(session)
				e.Insert(&db.ErrorLog{Description:"PeriodicGetOrderbook - API ", Error:err.Error(), Time:time.Now()})

			}else if len(orderBook.Buy) > 0 && len(orderBook.Sell) > 0 {
				bidVol := float64(0)

				midPrice := ( orderBook.Buy[0].Rate + orderBook.Sell[0].Rate) / 2.0

				quBidRate := midPrice * (1 - obRate)
				quAskRate := midPrice * (1 + obRate)

				for v := 0; v < len(orderBook.Buy); v++ {
					if orderBook.Buy[v].Rate >= quBidRate {
						bidVol += orderBook.Buy[v].Quantity * orderBook.Buy[v].Rate
					} else {
						break
					}
				}

				askVol := float64(0)

				for v := 0; v < len(orderBook.Sell); v++ {
					if orderBook.Sell[v].Rate <= quAskRate {
						askVol += orderBook.Sell[v].Quantity * orderBook.Sell[v].Rate
					} else {
						break
					}
				}
				VB := float64(0)
				VA := float64(0)
				thisSM.Lock.Lock()
				lastSM.Lock.Lock()
				Pbt := thisSM.Markets[markets[i]].Bid
				Pbt1 := lastSM.Markets[markets[i]].Bid
				Pat := thisSM.Markets[markets[i]].Ask
				Pat1 := lastSM.Markets[markets[i]].Ask
				thisSM.Lock.Unlock()
				lastSM.Lock.Unlock()
				if Pbt < Pbt1{
					VB = 0
				}else if Pbt > Pbt1{
					VB = bidVol
				}else{
					VB = 0
				}

				if Pat < Pat1{
					VA = askVol
				}else if Pat > Pat1{
					VA = 0
				}else{
					VA = 0
				}

				VOI := float64(0)
				VOI = VB - VA
				OIR := float64(0)
				OIR = (bidVol - askVol) / (bidVol + askVol)
				/*Spread := orderBook.Sell[0].Rate - orderBook.Buy[0].Rate
				Spread = Spread * 100000000
				Spread := (((orderBook.Sell[0].Rate - orderBook.Buy[0].Rate) / orderBook.Sell[0].Rate)* 10) + 1*/
				Spread := ((orderBook.Sell[0].Rate - orderBook.Buy[0].Rate) - 2*satoshi) / orderBook.Sell[0].Rate
				MMPB.Lock.Lock()
				MPB := MMPB.Markets[markets[i]]
				MMPB.Lock.Unlock()
				/*final := (VOI / Spread) + (OIR / Spread ) + (MPB / Spread)*/
				final := VOI + OIR + MPB

				MF[markets[i]].Lock.Lock()
				MF[markets[i]].Final = final
				MF[markets[i]].Lock.Unlock()

				//c := session.DB("v4").C("OwnOrderBook").With(session)
				f := session.DB("v4").C("LogMarketFinal").With(session)
				thisSM.Lock.Lock()
				err = f.Insert(&LogMarketFinal{LogTime:time.Now(), MarketName:markets[i],
				Bid: orderBook.Buy[0].Rate, Ask: orderBook.Sell[0].Rate, Voi:VOI, Oir:OIR, SPREAD:Spread, Mpb: MPB, Final:final, USDT: thisSM.Markets["USDT-BTC"].Last})
				thisSM.Lock.Unlock()

				/*h := session.DB("v4").C("LogHourly").With(session)
				BTCHourlyMarket[markets[i]].Lock.Lock()
				BTCHourlyMarket[markets[i]].HMR.InsertLog(orderBook.Buy[0].Rate, orderBook.Sell[0].Rate, final)


				errH := h.Update(bson.M{"marketname":markets[i]}, &db.RateWithMarketName{MarketName:markets[i], HMR:BTCHourlyMarket[markets[i]].HMR})

				if errH != nil{
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"Update Hourly Market in DB", Error:errH.Error(), Time:time.Now()})
				}

				/*fmt.Printf("Market Log: %v , Max A: %f, B: %f, F: %f, Min A: %f, B:%f, F: %f, Last A: %f, B: %f, F: %f \n", markets[i],
					BTCHourlyMarket[markets[i]].MaxAsk,BTCHourlyMarket[markets[i]].MaxBid,BTCHourlyMarket[markets[i]].MaxFinal,
					BTCHourlyMarket[markets[i]].MinAsk,BTCHourlyMarket[markets[i]].MinBid,BTCHourlyMarket[markets[i]].MinFinal,
					BTCHourlyMarket[markets[i]].LastAsk,BTCHourlyMarket[markets[i]].LastBid,BTCHourlyMarket[markets[i]].LastFinal,
					)
				BTCHourlyMarket[markets[i]].Lock.Unlock()*/
				if err!= nil{
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"Get Market Balance in DB", Error:err.Error(), Time:time.Now()})
				}

			/*	d := session.DB("v4").C("WalletBalance").With(session)
				var MarketBalance WalletBalance
				err = d.Find(bson.M{
					"currency" : strings.Split(markets[i], "-")[1],
				}).Select(bson.M{"available":1}).One(&MarketBalance)


				if err != nil && err.Error() != "not found"{
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"Get Market Balance in DB", Error:err.Error(), Time:time.Now()})
				}else if err !=nil{
					MarketBalance.Available = 0
				}



				var BTCBalance WalletBalance

				err = d.Find(bson.M{
					"currency" : "BTC",
				}).Select(bson.M{"available":1}).One(&BTCBalance)
				if err != nil{
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"Get BTC Balance in DB", Error:err.Error(), Time:time.Now()})
				}

*/
				//fmt.Printf("Market %v Final %f MarketBTC %f minSellRate %f \n", markets[i], final, MarketBTCEST, minSellRate)


				var temp bittrex.Balance
				if val , ok := MyOwnWallet[strings.Split(markets[i], "-")[1]]; ok{
					temp = val.Wallet
				}
				MyOwnWallet["BTC"].Lock.Lock()
				if final > FinalThresold && Spread > SpreadThresold && temp.Available == 0 && MyOwnWallet["BTC"].Wallet.Available >= minTotal && !MarketOrder[markets[i]].BuyOpening {
					go BuySellMarkets(markets[i], orderBook.Buy[0].Rate + satoshi , orderBook.Sell[0].Rate - satoshi)
				}
				MyOwnWallet["BTC"].Lock.Unlock()

			}
			//defer wgm.Done()
			wg.Done()
		}(wg, i)

	}

	wg.Wait()

}

func BuyMarket(market string, bidPrice float64, askPrice float64){
	bapi := bittrex.New(API_KEY, API_SECRET)
	betSize := minTotal

		CheckOrder(MarketOrder[market].BuyOrderUUID)
		if !MarketOrder[market].BuyOpening{
			// Buy filled = > sell order
			MyOwnWallet[strings.Split(market, "-")[1]].Lock.Lock()
			if MyOwnWallet[strings.Split(market, "-")[1]].Wallet.Available * askPrice > 0.0005 {
				Selluuid, errS := bapi.SellLimit(market, MyOwnWallet[strings.Split(market, "-")[1]].Wallet.Available, askPrice)
				MyOwnWallet[strings.Split(market, "-")[1]].Lock.Unlock()
				if errS != nil {
					session := mydb.Session.Clone()
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description: "Sell Limit API - ", Error: errS.Error(), Time: time.Now()})
					session.Close()
				} else {
					// selling = true
					MarketOrder[market].SellOrderUUID = Selluuid
					MarketOrder[market].SellOpening = true
					session := mydb.Session.Clone()
					e := session.DB("v4").C("OrderBooks").With(session)
					e.Insert(&LogOrderBook{UUID: Selluuid, Market: market, LogTime: time.Now(), OrderType: "Sell", Remark: "Buy filled , sell order"})
					session.Close()
				}
			}

		}else{
			// not yet fill
			ticker, err2:=bapi.GetTicker(market)
			if err2 != nil{
				session := mydb.Session.Clone()
				e := session.DB("v4").C("ErrorLog").With(session)
				e.Insert(&db.ErrorLog{Description:"Get Ticker API - ", Error:err2.Error(), Time:time.Now()})
				session.Close()
			}else{
				/*if buy signal & bid price up, cancel buy -> buying=false -> BUY
					  if !buy signal, cancel buy */
				MF[market].Lock.Lock()
				final := MF[market].Final
				MF[market].Lock.Unlock()
				spread := ( ticker.Ask - ticker.Bid - 2 * satoshi ) / ticker.Ask

				if ticker.Bid  > bidPrice +  satoshi && final > FinalThresold && spread > SpreadThresold{
					//cancel order
					err3 := bapi.CancelOrder(MarketOrder[market].BuyOrderUUID )
					if err3 != nil{
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description:"Cancel Order API - ", Error:err3.Error(), Time:time.Now()})
						session.Close()
					}else{
						MarketOrder[market].BuyOpening = false
						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{ Market:market, LogTime: time.Now(), OrderType: "Cancel Buy", Remark: "buy signal + bid price up, cancel buy"})
						session.Close()
						// buy again
						rate := ticker.Bid + satoshi
						quantity := (betSize * (1-fee)) / rate
						Buyuuid , errB := bapi.BuyLimit(market, quantity, rate)
						if errB != nil{
							session := mydb.Session.Clone()
							e := session.DB("v4").C("ErrorLog").With(session)
							e.Insert(&db.ErrorLog{Description:"Limit Buy price up API - ", Error:errB.Error(), Time:time.Now()})
							session.Close()
						}else{
							MarketOrder[market].BuyOrderUUID = Buyuuid
							MarketOrder[market].BuyOpening = true
							session := mydb.Session.Clone()
							e := session.DB("v4").C("OrderBooks").With(session)
							e.Insert(&LogOrderBook{ UUID:  Buyuuid, Market:market, LogTime: time.Now(), OrderType: "Buy", Remark: "buy signal + bid price up, cancel buy, then buy again"})
							session.Close()
						}

					}

				}else if final < FinalThresold{
					err3 := bapi.CancelOrder(MarketOrder[market].BuyOrderUUID )
					if err3 != nil{
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description:"Cancel Order API - ", Error:err3.Error(), Time:time.Now()})
						session.Close()
					}else {
						MarketOrder[market].BuyOrderUUID = ""
						MarketOrder[market].BuyOpening = false
						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{  Market:market, LogTime: time.Now(), OrderType: "Cancel Buy", Remark: "Not buy signal, cancel buy"})
						session.Close()
					}
				}
			}

	}

}

func SellMarket(market string, bidPrice  float64) {
	bapi := bittrex.New(API_KEY, API_SECRET)
	betSize := minTotal
	ot := CheckOrder(MarketOrder[market].SellOrderUUID)
	if time.Now().Sub(ot).Minutes() > 24*60 {
		// time up, cancel order, sell at bid
		err3 := bapi.CancelOrder(MarketOrder[market].SellOrderUUID )
		if err3 != nil{
			session := mydb.Session.Clone()
			e := session.DB("v4").C("ErrorLog").With(session)
			e.Insert(&db.ErrorLog{Description:"Cancel Order API - ", Error:err3.Error(), Time:time.Now()})
			session.Close()
		}else {
			session := mydb.Session.Clone()
			e := session.DB("v4").C("OrderBooks").With(session)
			e.Insert(&LogOrderBook{   Market:market, LogTime: time.Now(), OrderType: "Cancel Sell", Remark: "Times up sell, cancel sell"})
			session.Close()
			ticker, err2 := bapi.GetTicker(market)
			if err2 != nil {
				session := mydb.Session.Clone()
				defer session.Close()
				e := session.DB("v4").C("ErrorLog").With(session)
				e.Insert(&db.ErrorLog{Description: "Get Ticker API - ", Error: err2.Error(), Time: time.Now()})
			} else {
				MarketOrder[market].SellOpening = false
				// sell again
				rate := ticker.Bid
				MyOwnWallet[strings.Split(market, "-")[1]].Lock.Lock()
				Selluuid, errS := bapi.SellLimit(market, MyOwnWallet[strings.Split(market, "-")[1]].Wallet.Available, rate)
				MyOwnWallet[strings.Split(market, "-")[1]].Lock.Unlock()
				if errS != nil{
					session := mydb.Session.Clone()
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"Limit Sell Time up API - ", Error:errS.Error(), Time:time.Now()})
					session.Close()
				}else{
					MarketOrder[market].SellOrderUUID = Selluuid
					MarketOrder[market].SellOpening = true
					session := mydb.Session.Clone()
					e := session.DB("v4").C("OrderBooks").With(session)
					e.Insert(&LogOrderBook{ UUID:  Selluuid, Market:market, LogTime: time.Now(), OrderType: "Sell", Remark: "Times up, sell at bid"})
					session.Close()
				}
			}
		}
	}  else {
		// not yet fill
		ticker, err2 := bapi.GetTicker(market)
		if err2 != nil {
			session := mydb.Session.Clone()
			e := session.DB("v4").C("ErrorLog").With(session)
			e.Insert(&db.ErrorLog{Description: "Get Ticker API - ", Error: err2.Error(), Time: time.Now()})
			session.Close()
		} else {
			MF[market].Lock.Lock()
			final := MF[market].Final
			MF[market].Lock.Unlock()
			spread := ( ticker.Ask - ticker.Bid - 2*satoshi ) / ticker.Ask

			if final < 0 {
				// sell signal , cancel then sell it at ticker bid
				err3 := bapi.CancelOrder(MarketOrder[market].SellOrderUUID)
				if err3 != nil {
					session := mydb.Session.Clone()
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description: "Cancel Order API - ", Error: err3.Error(), Time: time.Now()})
					session.Close()
				} else {
					session := mydb.Session.Clone()
					e := session.DB("v4").C("OrderBooks").With(session)
					e.Insert(&LogOrderBook{  Market:market, LogTime: time.Now(), OrderType: "Cancel Sell", Remark: "sell signal, cancel sell"})
					session.Close()
					MarketOrder[market].SellOpening = false
					// sell again
					rate := ticker.Bid
					MyOwnWallet[strings.Split(market, "-")[1]].Lock.Lock()
					Selluuid, errS := bapi.SellLimit(market, MyOwnWallet[strings.Split(market, "-")[1]].Wallet.Available, rate)
					MyOwnWallet[strings.Split(market, "-")[1]].Lock.Unlock()
					if errS != nil {
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description: "Limit Sell Time up API - ", Error: errS.Error(), Time: time.Now()})
						session.Close()
					} else {
						MarketOrder[market].SellOrderUUID = Selluuid
						MarketOrder[market].SellOpening = true
						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{ UUID:  Selluuid, Market:market, LogTime: time.Now(), OrderType: "Sell", Remark: "sell signal, cancel sell then sell at bid"})
						session.Close()
					}

				}
			} else if ticker.Bid < bidPrice - satoshi && final > FinalThresold  && spread > SpreadThresold{
				/* signal & bid price down,1. cancel sell
					2. buy at bid, sell at ask (same time)
				*/
				err3 := bapi.CancelOrder(MarketOrder[market].SellOrderUUID )
				if err3 != nil{
					session := mydb.Session.Clone()
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"Cancel Order API - ", Error:err3.Error(), Time:time.Now()})
					session.Close()
				}else {

					session := mydb.Session.Clone()
					e := session.DB("v4").C("OrderBooks").With(session)
					e.Insert(&LogOrderBook{   Market:market, LogTime: time.Now(), OrderType: "Cancel Sell", Remark: "buy signal bid price down cancel sell"})
					session.Close()

					sellrate := ticker.Ask
					buyrate := ticker.Bid

					quantity := (betSize * (1-fee)) / buyrate
					Buyuuid , errB := bapi.BuyLimit(market, quantity, buyrate)
					MyOwnWallet[strings.Split(market, "-")[1]].Lock.Lock()

					Selluuid, errS := bapi.SellLimit(market, MyOwnWallet[strings.Split(market, "-")[1]].Wallet.Available, sellrate)
					MyOwnWallet[strings.Split(market, "-")[1]].Lock.Unlock()

					if errB != nil {
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description: "Limit Buy buy signal resell API - ", Error: errB.Error(), Time: time.Now()})
						session.Close()
					}else{
						MarketOrder[market].BuyOrderUUID = Buyuuid
						MarketOrder[market].BuyOpening = true

						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{ UUID:  Buyuuid, Market:market, LogTime: time.Now(), OrderType: "Buy", Remark: "buy signal bid price down buy and sell same time"})
						session.Close()
					}

					if errS != nil{
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description: "Limit Sell buy signal resell  API - ", Error: errS.Error(), Time: time.Now()})
						session.Close()
					}else {
						MarketOrder[market].SellOrderUUID = Selluuid
						MarketOrder[market].SellOpening = true

						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{ UUID:  Selluuid, Market:market, LogTime: time.Now(), OrderType: "Sell", Remark: "buy signal bid price down and sell same time"})
						session.Close()
					}

				}
			}
		}
	}
}


func BuySellMarkets(market string,  bidPrice, askPrice float64)  {
	bapi := bittrex.New(API_KEY, API_SECRET)
	betSize := minTotal
	rate := bidPrice
	quantity := (betSize * (1-fee)) / rate
	// buy order
	Buyuuid , errB := bapi.BuyLimit(market, quantity, rate)

	if errB != nil{
		session := mydb.Session.Clone()
		e := session.DB("v4").C("ErrorLog").With(session)
		e.Insert(&db.ErrorLog{Description:"Buy Limit API - ", Error:errB.Error(), Time:time.Now()})
		session.Close()
	}else{
		// buying=true
		MarketOrder[market].BuyOrderUUID = Buyuuid
		MarketOrder[market].BuyOpening = true

		session := mydb.Session.Clone()
		e := session.DB("v4").C("OrderBooks").With(session)
		e.Insert(&LogOrderBook{ UUID:  Buyuuid, Market:market, LogTime: time.Now(), OrderType: "Buy", Remark: "first: buy order"})
		session.Close()

		if _ , ok := MyOwnWallet[strings.Split(market, "-")[1]]; ok{
			MyOwnWallet[strings.Split(market, "-")[1]].Lock.Lock()
			MyOwnWallet[strings.Split(market, "-")[1]].Wallet.Available += quantity
			MyOwnWallet[strings.Split(market, "-")[1]].Lock.Unlock()
		}else {
			var temp bittrex.Balance
			temp.Available = quantity
			temp.Balance = quantity
			temp.Currency = strings.Split(market, "-")[1]
			MyOwnWallet[strings.Split(market, "-")[1]] = &OwnWallet{Wallet:temp}
		}

		go func() {
			for range time.NewTicker(time.Millisecond * 50).C {
				if MarketOrder[market].BuyOpening && MarketOrder[market].BuyOrderUUID != "" {
					go BuyMarket(market, bidPrice , askPrice)
				}
				if MarketOrder[market].SellOpening && MarketOrder[market].SellOrderUUID != ""{
					go SellMarket(market, bidPrice )
				}
				if !(MarketOrder[market].SellOpening && MarketOrder[market].SellOrderUUID != "") && !(MarketOrder[market].BuyOpening && MarketOrder[market].BuyOrderUUID != "" ) {
					break
				}
			}
		}()

	}
}

func CheckOrder(uuid string)(orderTime time.Time){

	bapi := bittrex.New(API_KEY, API_SECRET)
	order, err := bapi.GetOrder(uuid)
	if err != nil{
		session := mydb.Session.Clone()
		defer session.Close()
		e := session.DB("v4").C("ErrorLog").With(session)
		e.Insert(&db.ErrorLog{Description:"Check Order API - ", Error:err.Error(), Time:time.Now()})
	}else {
		a := strings.Split(order.Opened, ".")
		var LayoutLenMill string
		if len(a) > 1 {
			millise := len(a[1])
			LayoutLenMill = "."
			for i := 0; i < millise; i++ {
				LayoutLenMill += "0"
			}
		}
		t, err2 := time.Parse("2006-01-02T15:04:05" + LayoutLenMill, order.Opened)
		if err2 != nil {
			session := mydb.Session.Clone()
			defer session.Close()
			e := session.DB("v4").C("ErrorLog").With(session)
			e.Insert(&db.ErrorLog{Description: "Re-prase time error", Error: err2.Error(), Time: time.Now()})
		}else{
			orderTime = t
		}


		if order.Closed != ""{
			if order.Type == "LIMIT_BUY" {
				MarketOrder[order.Exchange].BuyOpening = false
				MarketOrder[order.Exchange].BuyOrderUUID = ""

			}else{
				MarketOrder[order.Exchange].SellOpening = false
				MarketOrder[order.Exchange].SellOrderUUID = ""
				if _ , ok := MyOwnWallet[strings.Split(order.Exchange, "-")[1]]; ok {
					MyOwnWallet[strings.Split(order.Exchange, "-")[1]].Lock.Lock()
					MyOwnWallet[strings.Split(order.Exchange, "-")[1]].Wallet.Available -= order.Quantity
					MyOwnWallet[strings.Split(order.Exchange, "-")[1]].Lock.Unlock()
				}
			}
		}else if order.Quantity != order.QuantityRemaining{
			// partially buy / sell
			if order.Type == "LIMIT_BUY" {
				if time.Now().Sub(t).Minutes() > 30 {
					err3 := bapi.CancelOrder(MarketOrder[order.Exchange].BuyOrderUUID )
					if err3 != nil{
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description:"Cancel Order Partial buy API - ", Error:err3.Error(), Time:time.Now()})
						session.Close()
					}else {
						MarketOrder[order.Exchange].BuyOrderUUID = ""
						MarketOrder[order.Exchange].BuyOpening = false
						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{   Market:order.Exchange, LogTime: time.Now(), OrderType: "Cancel Buy", Remark: "partially buy exist 30 min cancel buy"})
						session.Close()
					}
				}
			}else {
				if time.Now().Sub(t).Minutes() > 30 {
					err3 := bapi.CancelOrder(MarketOrder[order.Exchange].SellOrderUUID )
					if err3 != nil{
						session := mydb.Session.Clone()
						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description:"Cancel Order Partial sell API - ", Error:err3.Error(), Time:time.Now()})
						session.Close()
					}else {
						MarketOrder[order.Exchange].SellOrderUUID = ""
						MarketOrder[order.Exchange].SellOpening = false
						session := mydb.Session.Clone()
						e := session.DB("v4").C("OrderBooks").With(session)
						e.Insert(&LogOrderBook{  Market:order.Exchange, LogTime: time.Now(), OrderType: "Cancel Sell", Remark: "partially sell exist 30 min cancel sell"})
						session.Close()
					}
				}
			}
		}
	}
	return
}

/*func refreshOrder(){
	for t:= range time.NewTicker(time.Second * 5 ).C{

		session := mydb.Session.Clone()
		defer session.Close()
		c := session.DB("v4").C("OwnOrderBook2").With(session)
		sellbuyOrders := []db.Orders{}
		cerr := c.Find(bson.M{
			"status" : bson.M{"$in" : []string{"selling", "buying"}},
		}).All(&sellbuyOrders)

		if cerr != nil {
			e := session.DB("v4").C("ErrorLog").With(session)
			e.Insert(&db.ErrorLog{Description:"Get selling buying orders", Error:cerr.Error(), Time:time.Now()})

		}
		bapi := bittrex.New(API_KEY, API_SECRET)

		for _,v := range sellbuyOrders {

			var uuid string = ""

			if v.Status == "buying" {
				uuid = v.UUID
			} else if v.Status == "selling" {
				uuid = v.UUID
			}
			if uuid != "" {
				// Prepare to update order status from bittrex
				result, err := bapi.GetOrder(uuid)
				if err != nil {
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description: "Refresh order - API", Error: err.Error(), Time: time.Now()})
				} else {
					if !result.IsOpen {
						if v.Status == "buying" {
							v.Status = "bought"
							v.UpdatedAt = time.Now()
							v.Rate = result.PricePerUnit
							v.Fee = result.CommissionPaid
							v.Quantity = result.Quantity
							v.Total = result.Price
							a := strings.Split(result.Closed, ".")
							var LayoutLenMill string
							if len(a) > 1 {
								millise := len(a[1])
								LayoutLenMill = "."
								for i := 0; i < millise; i++ {
									LayoutLenMill += "0"
								}
							}
							t, err2 := time.Parse("2006-01-02T15:04:05" + LayoutLenMill, result.Closed)
							if err2 != nil {
								e := session.DB("v4").C("ErrorLog").With(session)
								e.Insert(&db.ErrorLog{Description: "Re-prase time error", Error: err2.Error(), Time: time.Now()})
								v.CompleteTime = time.Now()
							} else {
								v.CompleteTime = t
							}
						} else {
							v.Status = "sold"
							v.UpdatedAt = time.Now()
							v.Rate = result.PricePerUnit
							v.Fee = result.CommissionPaid
							v.Quantity = result.Quantity
							v.Total = result.Price
							a := strings.Split(result.Closed, ".")
							var LayoutLenMill string
							if len(a) > 1 {
								millise := len(a[1])
								LayoutLenMill = "."
								for i := 0; i < millise; i++ {
									LayoutLenMill += "0"
								}
							}
							t, err2 := time.Parse("2006-01-02T15:04:05" + LayoutLenMill, result.Closed)
							if err2 != nil {
								e := session.DB("v4").C("ErrorLog").With(session)
								e.Insert(&db.ErrorLog{Description: "Re-prase time error", Error: err2.Error(), Time: time.Now()})
								v.CompleteTime = time.Now()
							} else {
								v.CompleteTime = t
							}
						}
						err3 := c.Update(bson.M{"_id": v.Id}, v)
						if err3 != nil {
							e := session.DB("v4").C("ErrorLog").With(session)
							e.Insert(&db.ErrorLog{Description: "Update order - ", Error: err3.Error(), Time: time.Now()})
						}
					} else if result.QuantityRemaining != 0 && result.QuantityRemaining == result.Quantity || time.Now().Sub(v.OrderTime).Seconds() > 30 {
						// cancel buy order
						if v.Status == "buying" {
							canberr := bapi.CancelOrder(v.UUID)
							if canberr != nil {
								e := session.DB("v4").C("ErrorLog").With(session)
								e.Insert(&db.ErrorLog{Description: "Cancel order - API ", Error: canberr.Error(), Time: time.Now()})
							}
						} else  if v.Status == "selling"{
							canserr := bapi.CancelOrder(v.UUID)
							if canserr != nil {
								e := session.DB("v4").C("ErrorLog").With(session)
								e.Insert(&db.ErrorLog{Description: "Cancel order - API ", Error: canserr.Error(), Time: time.Now()})
							} else {
								price , err :=bapi.GetTicker(v.MarketName)
								if err!= nil{
									e := session.DB("v4").C("ErrorLog").With(session)
									e.Insert(&db.ErrorLog{Description: "Selling too long sell order get ticker - API ", Error: err.Error(), Time: time.Now()})
								} else {

									d := session.DB("v4").C("WalletBalance").With(session)
									var BTCBalance WalletBalance

									err = d.Find(bson.M{
										"currency": "BTC",
									}).Select(bson.M{"available": 1}).One(&BTCBalance)

									rate := price.Bid
									quantity := v.Quantity

									tradeHelper.SellHelper(rate, quantity, v.MarketName, BTCBalance.Available, 0, *bapi, mydb, "Resell non-sell order")
								}
							}
							ofee := (rate * quantity) * fee
							total := (rate*quantity) - ofee

							//uuid := "abc"
							uuid,placeOErr := bapi.SellLimit(v.MarketName,quantity, rate)
							if placeOErr!= err{
								e := session.DB("v4").C("ErrorLog").With(session)
								e.Insert(&db.ErrorLog{Description:"Selling too long sell Limit - API", Error:placeOErr.Error(), Time:time.Now()})

							} else {

								v.UUID = uuid
								v.Status = "selling"
								v.Rate = rate
								v.Quantity = quantity
								v.Fee = ofee
								v.Total = total

								sellerr := c.Update(bson.M{"_id": v.Id}, &v)

								if err != nil {

									e := session.DB("v4").C("ErrorLog").With(session)
									e.Insert(&db.ErrorLog{Description: "Place sell order", Error: sellerr.Error(), Time: time.Now()})

								}
							}
						}
					}
				}

			} else if time.Now().Sub(v.Buy.CompleteTime ).Hours() > 24 {
				price , err :=bapi.GetTicker(v.MarketName)
				if err!= nil{
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description: "24 hours sell order get ticker - API ", Error: err.Error(), Time: time.Now()})
				}

				rate := price.Ask
				quantity := v.Buy.Quantity
				ofee := (rate * quantity) * fee
				total := (rate*quantity) - ofee

				//uuid := "abc"
				uuid,placeOErr := bapi.SellLimit(v.MarketName,quantity, rate)
				if placeOErr!= err{
					e := session.DB("v4").C("ErrorLog").With(session)
					e.Insert(&db.ErrorLog{Description:"24 hours Place sell Limit - API", Error:placeOErr.Error(), Time:time.Now()})

				}else {
					v.Sell.UUID = uuid
					v.Sell.Status = "selling"
					v.Status = "selling"
					v.Sell.Rate = rate
					v.Sell.Quantity = quantity
					v.Sell.Fee = ofee
					v.Sell.Total = total

					sellerr := c.Update(bson.M{"_id": v.Id}, &v)

					if err != nil {

						e := session.DB("v4").C("ErrorLog").With(session)
						e.Insert(&db.ErrorLog{Description: "Place sell order", Error: sellerr.Error(), Time: time.Now()})

					}
				}

			}

		}
		JobChannel <- t
	}
}*/