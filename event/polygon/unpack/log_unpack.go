package unpack

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/FishcakeLab/fishcake-service/database"
	"github.com/FishcakeLab/fishcake-service/database/account_nft_info"
	"github.com/FishcakeLab/fishcake-service/database/activity"
	"github.com/FishcakeLab/fishcake-service/database/drop"
	"github.com/FishcakeLab/fishcake-service/database/event"
	"github.com/FishcakeLab/fishcake-service/database/token_nft"
	"github.com/FishcakeLab/fishcake-service/database/token_transfer"
	"github.com/FishcakeLab/fishcake-service/event/polygon/abi"
)

var (
	NftTokenUnpack, _ = abi.NewNftManagerFilterer(common.Address{}, nil)
	MerchantUnpack, _ = abi.NewFishcakeEventManagerFilterer(common.Address{}, nil)
)

func ActivityAdd(event event.ContractEvent, db *database.DB) error {
	rlpLog := event.RLPLog
	uEvent, unpackErr := MerchantUnpack.ParseActivityAdd(*rlpLog)
	if unpackErr != nil {
		return unpackErr
	}

	activityInfo := activity.ActivityInfo{
		ActivityId:         uEvent.ActivityId.Int64(),
		BusinessName:       uEvent.BusinessName,
		BusinessAccount:    uEvent.Who.String(),
		ActivityContent:    uEvent.ActivityContent,
		LatitudeLongitude:  uEvent.LatitudeLongitude,
		ActivityCreateTime: int64(event.Timestamp),
		ActivityDeadline:   uEvent.ActivityDeadLine.Int64(),
		DropType:           int8(uEvent.DropType),
		DropNumber:         uEvent.DropNumber.Int64(),
		MinDropAmt:         uEvent.MinDropAmt,
		MaxDropAmt:         uEvent.MaxDropAmt,
		TokenContractAddr:  uEvent.TokenContractAddr.String(),
		ActivityStatus:     1,
		AlreadyDropNumber:  0,
		ReturnAmount:       big.NewInt(0),
		MinedAmount:        big.NewInt(0),
	}

	tokenSent := token_transfer.TokenSent{
		Address:      activityInfo.BusinessAccount,
		TokenAddress: activityInfo.TokenContractAddr,
		Amount: new(big.Int).Mul(
			activityInfo.MaxDropAmt,
			uEvent.DropNumber,
		),
		Description: uEvent.ActivityContent,
		Timestamp:   uint64(activityInfo.ActivityCreateTime),
	}

	if err := db.Transaction(func(tx *database.DB) error {

		if err := db.ActivityInfoDB.StoreActivityInfo(activityInfo); err != nil {
			return err
		}

		if err := db.TokenSentDB.StoreTokenSent(tokenSent); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil
	}
	return nil

}

func ActivityFinish(event event.ContractEvent, db *database.DB) error {
	rlpLog := event.RLPLog
	uEvent, unpackErr := MerchantUnpack.ParseActivityFinish(*rlpLog)
	if unpackErr != nil {
		return unpackErr
	}
	ActivityId := uEvent.ActivityId.String()
	ReturnAmount := uEvent.ReturnAmount
	MinedAmount := uEvent.MinedAmount

	address := db.ActivityInfoDB.ActivityInfo(int(uEvent.ActivityId.Int64())).BusinessAccount
	content := db.ActivityInfoDB.ActivityInfo(int(uEvent.ActivityId.Int64())).ActivityContent

	tokenReceived := token_transfer.TokenReceived{
		Address:      address,
		TokenAddress: db.ActivityInfoDB.ActivityInfo(int(uEvent.ActivityId.Int64())).TokenContractAddr,
		Amount:       ReturnAmount,
		Description:  content,
		Timestamp:    event.Timestamp,
	}

	if err := db.Transaction(func(tx *database.DB) error {
		if err := db.ActivityInfoDB.ActivityFinish(ActivityId, ReturnAmount, MinedAmount); err != nil {
			return err
		}

		if err := db.TokenReceivedDB.StoreTokenReceived(tokenReceived); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil

}

func MintNft(event event.ContractEvent, db *database.DB) error {
	rlpLog := event.RLPLog
	uEvent, unpackErr := NftTokenUnpack.ParseCreateNFT(*rlpLog)
	if unpackErr != nil {
		return unpackErr
	}
	token := token_nft.TokenNft{
		TokenId:         uEvent.TokenId.Int64(),
		Who:             uEvent.Creator.String(),
		BusinessName:    uEvent.BusinessName,
		Description:     uEvent.Description,
		ImgUrl:          uEvent.ImgUrl,
		BusinessAddress: uEvent.BusinessAddress,
		WebSite:         uEvent.WebSite,
		Social:          uEvent.Social,
		ContractAddress: event.ContractAddress.String(),
		CostValue:       uEvent.Value,
		Deadline:        uEvent.Deadline.Uint64(),
		NftType:         int8(uEvent.Type),
	}
	accountNftInfo := account_nft_info.AccountNftInfo{
		Address: uEvent.Creator.String(),
	}
	if uEvent.Type == 1 {
		accountNftInfo.ProDeadline = uEvent.Deadline.Uint64()
	} else {
		accountNftInfo.BasicDeadline = uEvent.Deadline.Uint64()
	}
	if err := db.Transaction(func(tx *database.DB) error {
		if err := tx.TokenNftDB.StoreTokenNft(token); err != nil {
			return err
		}
		if err := tx.AccountNftInfoDB.StoreAccountNftInfo(accountNftInfo); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func Drop(event event.ContractEvent, db *database.DB) error {
	rlpLog := event.RLPLog
	uEvent, unpackErr := MerchantUnpack.ParseDrop(*rlpLog)
	if unpackErr != nil {
		return unpackErr
	}
	drop := drop.DropInfo{
		Address:         uEvent.Who.String(),
		DropAmount:      uEvent.DropAmt,
		ActivityId:      uEvent.ActivityId.Int64(),
		DropType:        1,
		Timestamp:       event.Timestamp,
		TransactionHash: event.TransactionHash.String(),
		EventSignature:  event.EventSignature.String(),
	}

	tokenReceived := token_transfer.TokenReceived{
		Address:      drop.Address,
		TokenAddress: db.ActivityInfoDB.ActivityInfo(int(uEvent.ActivityId.Int64())).TokenContractAddr,
		Amount:       uEvent.DropAmt,
		Description:  db.ActivityInfoDB.ActivityInfo(int(uEvent.ActivityId.Int64())).ActivityContent,
		Timestamp:    uint64(event.Timestamp),
	}

	if err := db.Transaction(func(tx *database.DB) error {
		resultErr, exist := tx.DropInfoDB.IsExist(drop.TransactionHash, drop.EventSignature, drop.DropType)
		if !exist && resultErr == nil {
			if err := tx.DropInfoDB.StoreDropInfo(drop); err != nil {
				return err
			}
			if err := tx.TokenReceivedDB.StoreTokenReceived(tokenReceived); err != nil {
				return err
			}
			// update activity info already drop number
			if err := tx.ActivityInfoDB.UpdateActivityInfo(uEvent.ActivityId.String()); err != nil {
				return err
			}
		} else {
			return resultErr
		}
		// create merchant drop record
		activityInfo := tx.ActivityInfoDB.ActivityInfo(int(drop.ActivityId))
		drop.Address = activityInfo.BusinessAccount
		drop.DropType = 2
		if err := tx.DropInfoDB.StoreDropInfo(drop); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func Transfer(event event.ContractEvent, db *database.DB, address string) error {
	rlpLog := event.RLPLog
	from := rlpLog.Topics[1].Hex()
	to := rlpLog.Topics[2].Hex()
	value := new(big.Int).SetBytes(rlpLog.Data)

	// 忽略给事件合约地址转账的记录，以及从事件合约地址转出的记录
	if from == "0x2CAf752814f244b3778e30c27051cc6B45CB1fc9" || to == "0x2CAf752814f244b3778e30c27051cc6B45CB1fc9" {
		return nil
	}

	tokenSent := token_transfer.TokenSent{
		Address:      from,
		TokenAddress: address,
		Amount:       value,
		Description:  "ERC20 Token Transfer",
		Timestamp:    uint64(event.Timestamp),
	}

	tokenReceived := token_transfer.TokenReceived{
		Address:      to,
		TokenAddress: address,
		Amount:       value,
		Description:  "ERC20 Token Transfer",
		Timestamp:    uint64(event.Timestamp),
	}

	if err := db.Transaction(func(tx *database.DB) error {
		if err := tx.TokenSentDB.StoreTokenSent(tokenSent); err != nil {
			return err
		}
		if err := tx.TokenReceivedDB.StoreTokenReceived(tokenReceived); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil

}
