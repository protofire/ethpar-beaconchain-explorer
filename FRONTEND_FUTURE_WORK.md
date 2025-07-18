# Frontend Future Work - Execution Blocks Feature

## Current Status
The execution blocks feature has been implemented with the following components:
- ✅ Frontend templates for displaying execution blocks in slot pages
- ✅ Tab navigation for switching between overview and execution blocks
- ✅ Backend service structure for handling execution ranks
- ✅ BigTable method for fetching available ranks (`GetAvailableRanksForExecBlock`)
- ✅ Service integration that fetches real ranks from BigTable
- ⚠️ Currently overriding real rank data with debug/test data for visualization testing

## DEBUG DATA STATUS
The service currently:
1. Fetches real execution block ranks from BigTable using `GetAvailableRanksForExecBlock`
2. Logs the actual ranks found (check logs for "DEBUG: Slot X, exec block Y, found ranks: [...]")
3. **OVERRIDES** the real data with hardcoded parallel blocks for testing tree visualization
4. This allows frontend testing while backend developers work on the detailed data fetching

## Remaining Work

### 1. **Backend Database Implementation**

**PRIORITY: HIGH** - Required before removing debug data

**Existing BigTable Method (Already Implemented):**
```go
func (bigtable *Bigtable) GetAvailableRanksForExecBlock(number uint64) ([]int, error)
```
This method is already implemented and working. It returns the available execution block ranks for a given slot number.

**New BigTable Method Needed:**
```go
func (bigtable *Bigtable) GetExecutionBlockDetailsByRank(number uint64, rank int) (*ExecutionBlockDetails, error)
```

**Data Structure Needed:**
```go
type ExecutionBlockDetails struct {
    Rank              int       `json:"rank"`
    BlockNumber       uint64    `json:"block_number"`
    BlockHash         []byte    `json:"block_hash"`
    ParentHash        []byte    `json:"parent_hash"`
    FeeRecipient      []byte    `json:"fee_recipient"`
    GasUsed           uint64    `json:"gas_used"`
    GasLimit          uint64    `json:"gas_limit"`
    TransactionsCount uint64    `json:"transactions_count"`
    Timestamp         uint64    `json:"timestamp"`
    BaseFeePerGas     uint64    `json:"base_fee_per_gas"`
    ExtraData         []byte    `json:"extra_data"`
    StateRoot         []byte    `json:"state_root"`
    ReceiptsRoot      []byte    `json:"receipts_root"`
}
```

**Expected BigTable Row Key Format:**
```
<chainID>:<reversedPaddedBlockNumber>:<rank>
```

**Implementation Requirements:**
- Query BigTable using the row key format above
- Parse execution block data from BigTable columns
- Return structured data for each rank
- Handle missing/invalid data gracefully
- Performance: Should support batch fetching for multiple ranks

### 2. **Remove Debug Data**

**File:** `services/services.go` - `PopulateSlotExecutionRanks` function

**What to remove:**
```go
// Remove this entire section (lines ~1128-1155):
if bt == nil {
    log.Errorf("DEBUG: BigTable is nil, using fake data for testing execution ranks")
    // ... fake data generation
}

// Remove this section (lines ~1191-1209):
// ALWAYS add fake parallel blocks for testing tree visualization
if slotData.ExecBlockNumber.Int64 > 0 {
    // Add fake parallel blocks to test the tree view
    executionRanks = []types.ExecutionRankData{
        // ... fake execution ranks
    }
    log.Errorf("DEBUG: FORCED %d execution ranks for slot %d", len(executionRanks), slotData.Slot)
}
```

**Replace with real implementation:**
```go
// After getting ranks from GetAvailableRanksForExecBlock
var executionRanks []types.ExecutionRankData
var parallelBlocks []*types.BlockPageParallelBlock

for _, rank := range ranks {
    // Fetch detailed data for this rank
    details, err := bt.GetExecutionBlockDetailsByRank(uint64(slotData.ExecBlockNumber.Int64), rank)
    if err != nil {
        log.Errorf("failed to get details for exec block %d rank %d: %v", slotData.ExecBlockNumber.Int64, rank, err)
        continue
    }
    
    // Add to ExecutionRanks for overview display
    executionRanks = append(executionRanks, types.ExecutionRankData{
        Rank:        rank,
        BlockNumber: details.BlockNumber,
        GasUsed:     details.GasUsed,
    })
    
    // Add to ParallelBlocks for table display
    parallelBlocks = append(parallelBlocks, &types.BlockPageParallelBlock{
        Rank:              uint64(details.Rank),
        BlockNumber:       details.BlockNumber,
        BlockHash:         details.BlockHash,
        ParentHash:        details.ParentHash,
        FeeRecipient:      details.FeeRecipient,
        GasUsed:           details.GasUsed,
        GasLimit:          details.GasLimit,
        TransactionsCount: details.TransactionsCount,
        Time:              time.Unix(int64(details.Timestamp), 0),
        BaseFeePerGas:     details.BaseFeePerGas,
        ExtraData:         details.ExtraData,
        StateRoot:         details.StateRoot,
        ReceiptsRoot:      details.ReceiptsRoot,
    })
}
```

### 3. **Frontend Polish**

**Template Improvements:**
- Update `templates/slot/parallel_blocks.html` to handle empty states gracefully
- Add proper error messaging when execution blocks can't be loaded
- Improve formatting of block hashes, addresses, and gas values

**Tab Navigation:**
- Re-enable custom tab switcher: `activateTabbarSwitcher("tabContent", "tab", "overview")`
- Test URL fragment navigation (e.g., `/slot/950080#parallel-blocksTabPanel`)

### 4. **Testing Requirements**

**Backend Testing:**
- Test with various execution block numbers that have different numbers of ranks
- Verify performance with multiple ranks (e.g., blocks with 10+ execution ranks)
- Test error handling for missing or corrupted data

**Frontend Testing:**
- Test slots with no execution blocks (should hide execution blocks tab)
- Test slots with only beacon block (rank 0)
- Test slots with multiple execution blocks
- Verify tab switching works correctly
- Test responsive design on mobile devices
