package listener

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/0xPolygon/heimdall-v2/bridge/util"
	checkpointTypes "github.com/0xPolygon/heimdall-v2/x/checkpoint/types"
)

// stakeUpdate represents the StakeUpdate event.
type stakeUpdate struct {
	Nonce           string `json:"nonce"`
	LogIndex        string `json:"logIndex"`
	TransactionHash string `json:"transactionHash"`
}

// stateSynced represents the StateSynced event.
type stateSynced struct {
	StateId         string `json:"stateId"`
	LogIndex        string `json:"logIndex"`
	TransactionHash string `json:"transactionHash"`
}

// newHeaderBlock represents the NewHeaderBlock event.
type newHeaderBlock struct {
	HeaderBlockId   string `json:"headerBlockId"`
	Proposer        string `json:"proposer"`
	StartBlock      string `json:"start"`
	EndBlock        string `json:"end"`
	RootHash        string `json:"root"`
	Timestamp       string `json:"blockTimestamp"`
	LogIndex        string `json:"logIndex"`
	TransactionHash string `json:"transactionHash"`
}

type stakeUpdateResponse struct {
	Data struct {
		StakeUpdates []stakeUpdate `json:"stakeUpdates"`
	} `json:"data"`
}

type stateSyncedsResponse struct {
	Data struct {
		StateSynceds []stateSynced `json:"stateSynceds"`
	} `json:"data"`
}

type newHeaderBlocksResponse struct {
	Data struct {
		NewHeaderBlocks []newHeaderBlock `json:"newHeaderBlocks"`
	} `json:"data"`
}

func (rl *RootChainListener) querySubGraph(query []byte, ctx context.Context) (data []byte, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, rl.subGraphClient.graphUrl, bytes.NewBuffer(query))
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := rl.subGraphClient.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			fmt.Println("Error closing response body:", err)
		}
	}()

	return io.ReadAll(response.Body)
}

// getLatestStateID returns the state ID from the latest StateSynced event
func (rl *RootChainListener) getLatestStateID(ctx context.Context) (*big.Int, error) {
	query := map[string]string{
		"query": `
		{
			stateSynceds(first : 1, orderBy : stateId, orderDirection : desc) {
				stateId
			}
		}
		`,
	}

	byteQuery, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	data, err := rl.querySubGraph(byteQuery, ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch latest state id from graph with err: %w", err)
	}

	var response stateSyncedsResponse
	if err = json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("unable to unmarshal graph response: %w", err)
	}

	if len(response.Data.StateSynceds) == 0 {
		return big.NewInt(0), nil
	}

	stateId := big.NewInt(0)
	stateId.SetString(response.Data.StateSynceds[0].StateId, 10)
	rl.Logger.Info("Fetched latest stateId from subgraph", "stateId", stateId)

	return stateId, nil
}

// getCurrentStateID returns the current state ID handled by the polygon chain
func (rl *RootChainListener) getCurrentStateID(ctx context.Context) (*big.Int, error) {
	rootChainContext, err := rl.getRootChainContext()
	if err != nil {
		return nil, err
	}

	stateReceiverInstance, err := rl.contractCaller.GetStateReceiverInstance(
		rootChainContext.ChainmanagerParams.ChainParams.StateReceiverAddress,
	)
	if err != nil {
		return nil, err
	}

	stateId, err := stateReceiverInstance.LastStateId(&bind.CallOpts{Context: ctx})
	if err != nil {
		return nil, err
	}

	return stateId, nil
}

// getStateSynced returns the StateSynced event based on the given state ID
func (rl *RootChainListener) getStateSynced(ctx context.Context, stateId int64) (*types.Log, error) {
	query := map[string]string{
		"query": `
		{
			stateSynceds(where: {stateId: ` + strconv.Itoa(int(stateId)) + `}) {
				logIndex
				transactionHash
			}
		}
		`,
	}

	byteQuery, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	data, err := rl.querySubGraph(byteQuery, ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch latest stateId from graph with err: %w", err)
	}

	var response stateSyncedsResponse
	if err = json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("unable to unmarshal graph response: %w", err)
	}

	if len(response.Data.StateSynceds) == 0 {
		return nil, fmt.Errorf("no state synced event found for state id %d", stateId)
	}

	receipt, err := rl.contractCaller.MainChainClient.TransactionReceipt(ctx, common.HexToHash(response.Data.StateSynceds[0].TransactionHash))
	if err != nil {
		return nil, err
	}

	for _, log := range receipt.Logs {
		if strconv.Itoa(int(log.Index)) == response.Data.StateSynceds[0].LogIndex {
			rl.Logger.Info("Retrieved log for StateSynced event", "stateId", stateId, "logIndex", response.Data.StateSynceds[0].LogIndex, "txHash", response.Data.StateSynceds[0].TransactionHash)
			return log, nil
		}
	}

	return nil, fmt.Errorf("no log found for given log index %s and state id %d", response.Data.StateSynceds[0].LogIndex, stateId)
}

// getLatestNonce returns the nonce from the latest StakeUpdate event
func (rl *RootChainListener) getLatestNonce(ctx context.Context, validatorId uint64) (uint64, error) {
	query := map[string]string{
		"query": `
		{
			stakeUpdates(first:1, orderBy: nonce, orderDirection : desc, where: {validatorId: ` + strconv.Itoa(int(validatorId)) + `}){
				nonce
		   } 
		}   
		`,
	}

	byteQuery, err := json.Marshal(query)
	if err != nil {
		return 0, err
	}

	data, err := rl.querySubGraph(byteQuery, ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to fetch latest nonce from graph with err: %w", err)
	}

	var response stakeUpdateResponse
	if err = json.Unmarshal(data, &response); err != nil {
		return 0, err
	}

	if len(response.Data.StakeUpdates) == 0 {
		return 0, nil
	}

	latestValidatorNonce, err := strconv.Atoi(response.Data.StakeUpdates[0].Nonce)
	if err != nil {
		return 0, err
	}
	rl.Logger.Info("Fetched latest nonce from subgraph", "validatorId", validatorId, "latestNonce", uint64(latestValidatorNonce))

	return uint64(latestValidatorNonce), nil
}

// getStakeUpdate returns StakeUpdate event based on the given validator ID and nonce
func (rl *RootChainListener) getStakeUpdate(ctx context.Context, validatorId, nonce uint64) (*types.Log, error) {
	query := map[string]string{
		"query": `
		{
			stakeUpdates(where: {validatorId: ` + strconv.Itoa(int(validatorId)) + `, nonce: ` + strconv.Itoa(int(nonce)) + `}){
				transactionHash
				logIndex
		   } 
		}   
		`,
	}

	byteQuery, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	data, err := rl.querySubGraph(byteQuery, ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch stake update from graph with err: %w", err)
	}

	var response stakeUpdateResponse
	if err = json.Unmarshal(data, &response); err != nil {
		return nil, err
	}

	if len(response.Data.StakeUpdates) == 0 {
		return nil, fmt.Errorf("no stake update found for validator %d and nonce %d", validatorId, nonce)
	}

	receipt, err := rl.contractCaller.MainChainClient.TransactionReceipt(ctx, common.HexToHash(response.Data.StakeUpdates[0].TransactionHash))
	if err != nil {
		return nil, err
	}

	for _, log := range receipt.Logs {
		if strconv.Itoa(int(log.Index)) == response.Data.StakeUpdates[0].LogIndex {
			rl.Logger.Info("Retrieved StakeUpdate log from Ethereum", "validatorId", validatorId, "nonce", nonce, "txHash", log.TxHash.Hex())
			return log, nil
		}
	}

	return nil, fmt.Errorf("no log found for given log index %s ,validator %d and nonce %d", response.Data.StakeUpdates[0].LogIndex, validatorId, nonce)
}

// getLatestCheckpointFromL1 returns the latest checkpoint from L1 using subgraph
func (rl *RootChainListener) getLatestCheckpointFromL1(ctx context.Context) (*newHeaderBlock, error) {
	query := map[string]string{
		"query": `
		{
			newHeaderBlocks(first: 1, orderBy: headerBlockId, orderDirection: desc) {
				headerBlockId
				logIndex
				transactionHash
			}
		}
		`,
	}

	byteQuery, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	data, err := rl.querySubGraph(byteQuery, ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch latest header block event from subgraph with err: %w", err)
	}

	var response newHeaderBlocksResponse
	if err = json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("unable to unmarshal subgraph response: %w", err)
	}

	if len(response.Data.NewHeaderBlocks) == 0 {
		return nil, fmt.Errorf("no header block event found")
	}

	latestHeaderBlock := response.Data.NewHeaderBlocks[0]

	rl.Logger.Info("Fetched latest header block event from subgraph", "headerBlockId", latestHeaderBlock.HeaderBlockId, "logIndex", latestHeaderBlock.LogIndex, "transactionHash", latestHeaderBlock.TransactionHash)

	return &latestHeaderBlock, nil
}

// convertNewHeaderBlockToCheckpoint converts a newHeaderBlock to checkpointTypes.Checkpoint
func (rl *RootChainListener) convertNewHeaderBlockToCheckpoint(headerBlock *newHeaderBlock) (*checkpointTypes.Checkpoint, error) {
	// Get the root chain context to access BorChainId
	rootChainContext, err := rl.getRootChainContext()
	if err != nil {
		return nil, fmt.Errorf("failed to get root chain context: %w", err)
	}
	borChainId := rootChainContext.ChainmanagerParams.ChainParams.BorChainId

	// Get checkpoint parameters to access ChildChainBlockInterval
	checkpointParams, err := util.GetCheckpointParams(rl.cliCtx.Codec)
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint params: %w", err)
	}

	// Convert string fields to appropriate types
	headerBlockId, err := strconv.ParseUint(headerBlock.HeaderBlockId, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse header block id: %w", err)
	}

	// Calculate checkpoint ID by dividing HeaderBlockId by child chain block interval
	if checkpointParams.ChildChainBlockInterval == 0 {
		return nil, fmt.Errorf("child chain block interval cannot be zero")
	}
	id := headerBlockId / checkpointParams.ChildChainBlockInterval

	startBlock, err := strconv.ParseUint(headerBlock.StartBlock, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse start block: %w", err)
	}

	endBlock, err := strconv.ParseUint(headerBlock.EndBlock, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse end block: %w", err)
	}

	timestamp, err := strconv.ParseUint(headerBlock.Timestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Convert hex string to bytes for root hash
	rootHashHex := headerBlock.RootHash
	if len(rootHashHex) >= 2 && rootHashHex[:2] == "0x" {
		rootHashHex = rootHashHex[2:] // Remove "0x" prefix
	}
	rootHashBytes, err := hex.DecodeString(rootHashHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode root hash: %w", err)
	}

	// Create and return the checkpoint
	checkpoint := &checkpointTypes.Checkpoint{
		Id:         id,
		Proposer:   headerBlock.Proposer,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		RootHash:   rootHashBytes,
		BorChainId: borChainId,
		Timestamp:  timestamp,
	}

	return checkpoint, nil
}
