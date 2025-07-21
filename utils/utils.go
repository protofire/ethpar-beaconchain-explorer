package utils

import (
	"bytes"
	"context"
	securerand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"image/color"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/protofire/ethpar-beaconchain-explorer/codec"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/price"
	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/params"
	"github.com/kataras/i18n"
	"github.com/lib/pq"
	"github.com/mvdan/xurls"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/signing"
	prysm_params "github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/shopspring/decimal"
	"github.com/skip2/go-qrcode"
	confusables "github.com/skygeario/go-confusable-homoglyphs"
)

var ErrRateLimit = errors.New("## RATE LIMIT ##")

var localiser *i18n.I18n

// making sure language files are loaded only once
func getLocaliser() *i18n.I18n {
	if localiser == nil {
		localiser, err := i18n.New(i18n.Glob("locales/*/*"), "en-US", "ru-RU")
		if err != nil {
			log.Println(err)
		}
		return localiser
	}
	return localiser
}

var HashLikeRegex = regexp.MustCompile(`^[0-9a-fA-F]{0,96}$`)

// GetTemplateFuncs will get the template functions
func GetTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"includeHTML":                             IncludeHTML,
		"includeSvg":                              IncludeSvg,
		"formatHTML":                              FormatMessageToHtml,
		"formatBalance":                           FormatBalance,
		"formatNotificationChannel":               FormatNotificationChannel,
		"formatBalanceSql":                        FormatBalanceSql,
		"formatCurrentBalance":                    FormatCurrentBalance,
		"formatElCurrency":                        FormatElCurrency,
		"formatClCurrency":                        FormatClCurrency,
		"formatEffectiveBalance":                  FormatEffectiveBalance,
		"formatBlockNumber":                       FormatBlockNumber,
		"formatBlockStatus":                       FormatBlockStatus,
		"formatBlockSlot":                         FormatBlockSlot,
		"formatSlotToTimestamp":                   FormatSlotToTimestamp,
		"formatDepositAmount":                     FormatDepositAmount,
		"formatEpoch":                             FormatEpoch,
		"fixAddressCasing":                        FixAddressCasing,
		"formatAddressLong":                       FormatAddressLong,
		"formatHashLong":                          FormatHashLong,
		"formatEth1Block":                         FormatEth1Block,
		"formatEth1BlockHash":                     FormatEth1BlockHash,
		"formatEth1Address":                       FormatEth1Address,
		"formatEth1AddressStringLowerCase":        FormatEth1AddressStringLowerCase,
		"formatEth1TxHash":                        FormatEth1TxHash,
		"formatGraffiti":                          FormatGraffiti,
		"formatHash":                              FormatHash,
		"formatDepositStatus":                     FormatDepositStatus,
		"formatConsolidationStatus":               FormatConsolidationStatus,
		"formatWithdawalCredentials":              FormatWithdawalCredentials,
		"formatAddressToWithdrawalCredentials":    FormatAddressToWithdrawalCredentials,
		"formatBitlist":                           FormatBitlist,
		"formatCommitteeBitList":                  FormatCommitteeBitList,
		"formatBitvectorValidators":               formatBitvectorValidators,
		"formatParticipation":                     FormatParticipation,
		"formatIncome":                            FormatIncome,
		"formatIncomeSql":                         FormatIncomeSql,
		"formatSqlInt64":                          FormatSqlInt64,
		"formatValidator":                         FormatValidator,
		"formatValidatorWithName":                 FormatValidatorWithName,
		"formatValidatorInt64":                    FormatValidatorInt64,
		"formatValidatorStatus":                   FormatValidatorStatus,
		"formatPercentage":                        FormatPercentage,
		"formatPercentageWithPrecision":           FormatPercentageWithPrecision,
		"formatPercentageWithGPrecision":          FormatPercentageWithGPrecision,
		"formatPercentageColoredEmoji":            FormatPercentageColoredEmoji,
		"formatPublicKey":                         FormatPublicKey,
		"formatSlashedValidator":                  FormatSlashedValidator,
		"formatSlashedValidatorInt64":             FormatSlashedValidatorInt64,
		"formatTimestamp":                         FormatTimestamp,
		"formatTsWithoutTooltip":                  FormatTsWithoutTooltip,
		"formatValidatorName":                     FormatValidatorName,
		"formatAttestationInclusionEffectiveness": FormatAttestationInclusionEffectiveness,
		"formatValidatorTags":                     FormatValidatorTags,
		"formatValidatorTag":                      FormatValidatorTag,
		"formatRPL":                               FormatRPL,
		"formatETH":                               FormatETH,
		"formatFloat":                             FormatFloat,
		"formatAmount":                            FormatAmount,
		"formatBytes":                             FormatBytes,
		"formatBlobVersionedHash":                 FormatBlobVersionedHash,
		"formatBigAmount":                         FormatBigAmount,
		"formatBytesAmount":                       FormatBytesAmount,
		"formatYesNo":                             FormatYesNo,
		"formatAmountFormatted":                   FormatAmountFormatted,
		"formatAddressAsLink":                     FormatAddressAsLink,
		"formatBuilder":                           FormatBuilder,
		"formatDifficulty":                        FormatDifficulty,
		"getCurrencyLabel":                        price.GetCurrencyLabel,
		"epochOfSlot":                             EpochOfSlot,
		"dayToTime":                               DayToTime,
		"contains":                                strings.Contains,
		"roundDecimals":                           RoundDecimals,
		"bigIntCmp":                               func(i *big.Int, j int) int { return i.Cmp(big.NewInt(int64(j))) },
		"mod":                                     func(i, j int) bool { return i%j == 0 },
		"sub":                                     func(i, j int) int { return i - j },
		"subUI64":                                 func(i, j uint64) uint64 { return i - j },
		"add":                                     func(i, j int) int { return i + j },
		"addI64":                                  func(i, j int64) int64 { return i + j },
		"addUI64":                                 func(i, j uint64) uint64 { return i + j },
		"addFloat64":                              func(i, j float64) float64 { return i + j },
		"addBigInt":                               func(i, j *big.Int) *big.Int { return new(big.Int).Add(i, j) },
		"mul":                                     func(i, j float64) float64 { return i * j },
		"div":                                     func(i, j float64) float64 { return i / j },
		"divInt":                                  func(i, j int) float64 { return float64(i) / float64(j) },
		"nef":                                     func(i, j float64) bool { return i != j },
		"gtf":                                     func(i, j float64) bool { return i > j },
		"ltf":                                     func(i, j float64) bool { return i < j },
		"round": func(i float64, n int) float64 {
			return math.Round(i*math.Pow10(n)) / math.Pow10(n)
		},
		"percent": func(i float64) float64 { return i * 100 },
		"formatThousands": func(i float64) string {
			p := message.NewPrinter(language.English)
			return p.Sprintf("%.0f\n", i)
		},
		"formatThousandsFancy": func(i float64) string {
			p := message.NewPrinter(language.English)
			return p.Sprintf("%v\n", i)
		},
		"formatThousandsInt": func(i int) string {
			p := message.NewPrinter(language.English)
			return p.Sprintf("%d", i)
		},
		"formatStringThousands": FormatThousandsEnglish,
		"derefString":           DerefString,
		"trLang":                TrLang,
		"firstCharToUpper":      func(s string) string { return cases.Title(language.English).String(s) },
		"eqsp": func(a, b *string) bool {
			if a != nil && b != nil {
				return *a == *b
			}
			return false
		},
		"stringsJoin":     strings.Join,
		"formatAddCommas": FormatAddCommas,
		"encodeToString":  hex.EncodeToString,

		"formatTokenBalance":      FormatTokenBalance,
		"formatAddressEthBalance": FormatAddressEthBalance,
		"toBase64":                ToBase64,
		"bytesToNumberString": func(input []byte) string {
			return new(big.Int).SetBytes(input).String()
		},
		"bigDecimalShift": func(num []byte, shift []byte) string {
			numDecimal := decimal.NewFromBigInt(new(big.Int).SetBytes(num), 0)
			denomDecimal := decimal.NewFromBigInt(new(big.Int).Exp(big.NewInt(10), new(big.Int).SetBytes(shift), nil), 0)
			res := numDecimal.DivRound(denomDecimal, 18)
			return res.String()
		},
		"trimTrailingZero": func(num string) string {
			if strings.Contains(num, ".") {
				return strings.TrimRight(strings.TrimRight(num, "0"), ".")
			}
			return num
		},
		// ETH1 related formatting
		"formatEth1TxStatus":    FormatEth1TxStatus,
		"formatEth1AddressFull": FormatEth1AddressFull,
		"byteToString": func(num []byte) string {
			return string(num)
		},
		"bigToInt": func(val *hexutil.Big) *big.Int {
			if val != nil {
				return val.ToInt()
			}
			return nil
		},
		"formatBigNumberAddCommasFormated": FormatBigNumberAddCommasFormated,
		"formatEthstoreComparison":         FormatEthstoreComparison,
		"formatPoolPerformance":            FormatPoolPerformance,
		"formatTokenSymbolTitle":           FormatTokenSymbolTitle,
		"formatTokenSymbol":                FormatTokenSymbol,
		"dict": func(values ...interface{}) (map[string]interface{}, error) {
			if len(values)%2 != 0 {
				return nil, errors.New("invalid dict call")
			}
			dict := make(map[string]interface{}, len(values)/2)
			for i := 0; i < len(values); i += 2 {
				key, ok := values[i].(string)
				if !ok {
					return nil, errors.New("dict keys must be strings")
				}
				dict[key] = values[i+1]
			}
			return dict, nil
		},
	}
}

// IncludeHTML adds html to the page
func IncludeHTML(path string) template.HTML {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Printf("includeHTML - error reading file: %v", err)
		return ""
	}
	return template.HTML(string(b))
}

func GraffitiToString(graffiti []byte) string {
	s := strings.Map(fixUtf, string(bytes.Trim(graffiti, "\x00")))
	s = strings.Replace(s, "\u0000", "", -1) // remove 0x00 bytes as it is not supported in postgres

	if !utf8.ValidString(s) {
		return "INVALID_UTF8_STRING"
	}

	return s
}

// FormatGraffitiString formats (and escapes) the graffiti
func FormatGraffitiString(graffiti string) string {
	return strings.Map(fixUtf, template.HTMLEscapeString(graffiti))
}

func fixUtf(r rune) rune {
	if r == utf8.RuneError {
		return -1
	}
	return r
}

func SyncPeriodOfEpoch(epoch uint64) uint64 {
	if epoch < config.AltairForkEpoch {
		return 0
	}
	return epoch / config.EpochsPerSyncCommitteePeriod
}

// FirstEpochOfSyncPeriod returns the first epoch of a given sync period.
//
// Please note that it will return the calculated first epoch of the sync period even if it is pre ALTAIR.
//
// Furthermore, for the very first actual sync period, it may return an epoch pre ALTAIR even though that is inccorect.
//
// For more information: https://eth2book.info/capella/annotated-spec/#sync-committee-updates
func FirstEpochOfSyncPeriod(syncPeriod uint64) uint64 {
	return syncPeriod * config.EpochsPerSyncCommitteePeriod
}

// EpochOfSlot returns the corresponding epoch of a slot
func EpochOfSlot(slot uint64) uint64 {
	return slot / config.SlotsPerEpoch
}

// SlotToTime returns a time.Time to slot
func SlotToTime(slot uint64) time.Time {
	return time.Unix(int64(config.MinGenesisTime+slot*config.SecondsPerSlot), 0)
}

// TimeToSlot returns time to slot in seconds
func TimeToSlot(timestamp uint64) uint64 {
	if config.MinGenesisTime > timestamp {
		return 0
	}
	return (timestamp - config.MinGenesisTime) / config.SecondsPerSlot
}

// EpochToTime will return a time.Time for an epoch
func EpochToTime(epoch uint64) time.Time {
	return time.Unix(int64(config.MinGenesisTime+epoch*config.SecondsPerSlot*config.SlotsPerEpoch), 0)
}

// TimeToDay will return a days since genesis for an timestamp
func TimeToDay(timestamp uint64) uint64 {
	const hoursInADay = float64(Day / time.Hour)
	return uint64(time.Unix(int64(timestamp), 0).Sub(time.Unix(int64(config.MinGenesisTime), 0)).Hours() / hoursInADay)
}

func DayToTime(day int64) time.Time {
	return time.Unix(int64(config.MinGenesisTime), 0).Add(Day * time.Duration(day))
}

// TimeToEpoch will return an epoch for a given time
func TimeToEpoch(ts time.Time) int64 {
	if int64(config.MinGenesisTime) > ts.Unix() {
		return 0
	}
	return (ts.Unix() - int64(config.MinGenesisTime)) / int64(config.SecondsPerSlot) / int64(config.SlotsPerEpoch)
}

func WeiToEther(wei *big.Int) decimal.Decimal {
	return decimal.NewFromBigInt(wei, 0).DivRound(decimal.NewFromInt(params.Ether), 18)
}

func WeiBytesToEther(wei []byte) decimal.Decimal {
	return WeiToEther(new(big.Int).SetBytes(wei))
}

// WaitForCtrlC will block/wait until a control-c is pressed
func WaitForCtrlC() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-c
}

// WaitForCtrlCAndCancelGoRoutines blocks until Ctrl+C or SIGTERM is received and then calls cancel().
func WaitForCtrlCAndCancelGoRoutines(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	sig := <-c
	fmt.Printf("received signal %s, shutting down...\n", sig)
	cancel()
}

// MustParseHex will parse a string into hex
func MustParseHex(hexString string) []byte {
	data, err := hex.DecodeString(strings.Replace(hexString, "0x", "", -1))
	if err != nil {
		log.Fatal(err)
	}
	return data
}

func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Headers", "*, Authorization")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func IsApiRequest(r *http.Request) bool {
	query, ok := r.URL.Query()["format"]
	return ok && len(query) > 0 && query[0] == "json"
}

var eth1AddressRE = regexp.MustCompile("^(0x)?[0-9a-fA-F]{40}$")
var withdrawalCredentialsRE = regexp.MustCompile("^(0x)?00[0-9a-fA-F]{62}$")
var withdrawalCredentialsAddressRE = regexp.MustCompile("^(0x)?(?:0[12]0{22})[0-9A-Fa-f]{40}$")
var eth1TxRE = regexp.MustCompile("^(0x)?[0-9a-fA-F]{64}$")
var zeroHashRE = regexp.MustCompile("^(0x)?0+$")
var hashRE = regexp.MustCompile("^(0x)?[0-9a-fA-F]{96}$")

// IsValidEth1Address verifies whether a string represents a valid eth1-address.
func IsValidEth1Address(s string) bool {
	return !zeroHashRE.MatchString(s) && eth1AddressRE.MatchString(s)
}

// IsEth1Address verifies whether a string represents an eth1-address.
// In contrast to IsValidEth1Address, this also returns true for the 0x0 address
func IsEth1Address(s string) bool {
	return eth1AddressRE.MatchString(s)
}

// IsValidEth1Tx verifies whether a string represents a valid eth1-tx-hash.
func IsValidEth1Tx(s string) bool {
	return !zeroHashRE.MatchString(s) && eth1TxRE.MatchString(s)
}

// IsEth1Tx verifies whether a string represents an eth1-tx-hash.
// In contrast to IsValidEth1Tx, this also returns true for the 0x0 address
func IsEth1Tx(s string) bool {
	return eth1TxRE.MatchString(s)
}

// IsHash verifies whether a string represents an eth1-hash.
func IsHash(s string) bool {
	return hashRE.MatchString(s)
}

// IsValidWithdrawalCredentials verifies whether a string represents valid withdrawal credentials.
func IsValidWithdrawalCredentials(s string) bool {
	return withdrawalCredentialsRE.MatchString(s) || withdrawalCredentialsAddressRE.MatchString(s)
}

// RoundDecimals rounds (nearest) a number to the specified number of digits after comma
func RoundDecimals(f float64, n int) float64 {
	d := math.Pow10(n)
	return math.Round(f*d) / d
}

// HashAndEncode digests the input with sha256 and returns it as hex string
func HashAndEncode(input string) string {
	codeHashedBytes := sha256.Sum256([]byte(input))
	return hex.EncodeToString(codeHashedBytes[:])
}

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

// RandomString returns a random hex-string
func RandomString(length int) string {
	b, _ := GenerateRandomBytesSecure(length)
	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b)
}

func GenerateRandomBytesSecure(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := securerand.Read(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func SqlRowsToJSON(rows *sql.Rows) ([]interface{}, error) {
	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return nil, fmt.Errorf("error getting column types: %w", err)
	}

	count := len(columnTypes)
	finalRows := []interface{}{}

	for rows.Next() {

		scanArgs := make([]interface{}, count)

		for i, v := range columnTypes {
			switch v.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "UUID":
				scanArgs[i] = new(sql.NullString)
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
			case "INT4", "INT8":
				scanArgs[i] = new(sql.NullInt64)
			case "FLOAT8":
				scanArgs[i] = new(sql.NullFloat64)
			case "TIMESTAMP":
				scanArgs[i] = new(sql.NullTime)
			case "_INT4", "_INT8":
				scanArgs[i] = new(pq.Int64Array)
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return nil, fmt.Errorf("error scanning rows: %w", err)
		}

		masterData := map[string]interface{}{}

		for i, v := range columnTypes {

			//log.Println(v.Name(), v.DatabaseTypeName())
			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				if z.Valid {
					masterData[v.Name()] = z.Bool
				} else {
					masterData[v.Name()] = nil
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				if z.Valid {
					if v.DatabaseTypeName() == "BYTEA" {
						if len(z.String) > 0 {
							masterData[v.Name()] = "0x" + hex.EncodeToString([]byte(z.String))
						} else {
							masterData[v.Name()] = nil
						}
					} else if v.DatabaseTypeName() == "NUMERIC" {
						nbr, _ := new(big.Int).SetString(z.String, 10)
						masterData[v.Name()] = nbr
					} else {
						masterData[v.Name()] = z.String
					}
				} else {
					masterData[v.Name()] = nil
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				if z.Valid {
					masterData[v.Name()] = z.Int64
				} else {
					masterData[v.Name()] = nil
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				if z.Valid {
					masterData[v.Name()] = z.Int32
				} else {
					masterData[v.Name()] = nil
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				if z.Valid {
					masterData[v.Name()] = z.Float64
				} else {
					masterData[v.Name()] = nil
				}
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullTime); ok {
				if z.Valid {
					masterData[v.Name()] = z.Time.Unix()
				} else {
					masterData[v.Name()] = nil
				}
				continue
			}

			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	return finalRows, nil
}

// Glob walks through a directory and returns files with a given extension
func Glob(dir string, ext string) ([]string, error) {
	files := []string{}
	err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if filepath.Ext(path) == ext {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

func BitAtVector(b []byte, i int) bool {
	bb := b[i/8]
	return (bb & (1 << uint(i%8))) > 0
}

func GetNetwork() string {
	return strings.ToLower(config.NetworkName)
}

func FormatThousandsEnglish(number string) string {
	runes := []rune(number)
	cnt := 0
	for _, rune := range runes {
		if rune == '.' {
			break
		}
		cnt += 1
	}
	amt := cnt / 3
	rem := cnt % 3

	if rem == 0 {
		amt -= 1
	}

	res := make([]rune, 0, amt+rem)
	if amt <= 0 {
		return number
	}
	for i := 0; i < len(runes); i++ {
		if i != 0 && i == rem {
			res = append(res, ',')
			amt -= 1
		}

		if amt > 0 && i > rem && ((i-rem)%3) == 0 {
			res = append(res, ',')
			amt -= 1
		}

		res = append(res, runes[i])
	}

	return string(res)
}

// Generates a QR code for an address
// returns two transparent base64 encoded img strings for dark and light theme
// the first has a black QR code the second a white QR code
func GenerateQRCodeForAddress(address []byte) (string, string, error) {
	q, err := qrcode.New(FixAddressCasing(fmt.Sprintf("%x", address)), qrcode.Medium)
	if err != nil {
		return "", "", err
	}

	q.BackgroundColor = color.Transparent
	q.ForegroundColor = color.Black

	png, err := q.PNG(320)
	if err != nil {
		return "", "", err
	}

	q.ForegroundColor = color.White

	pngInverse, err := q.PNG(320)
	if err != nil {
		return "", "", err
	}

	return base64.StdEncoding.EncodeToString(png), base64.StdEncoding.EncodeToString(pngInverse), nil
}

// sliceContains reports whether the provided string is present in the given slice of strings.
func SliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func FormatEthstoreComparison(pool string, val float64) template.HTML {
	prefix := ""
	textClass := "text-danger"
	ou := "underperforms"
	if val > 0 {
		prefix = "+"
		textClass = "text-success"
		ou = "outperforms"
	}

	return template.HTML(fmt.Sprintf(`<sub title="%s %s the ETH.STORE® indicator by %s%.2f%%" data-toggle="tooltip" class="%s">(%s%.2f%%)</sub>`, pool, ou, prefix, val, textClass, prefix, val))
}

func FormatPoolPerformance(val float64) template.HTML {
	return template.HTML(fmt.Sprintf(`<span data-toggle="tooltip" title=%f%%>%s%%</span>`, val, fmt.Sprintf("%.2f", val)))
}

func FormatTokenSymbolTitle(symbol string) string {
	if isMaliciousToken(symbol) {
		return fmt.Sprintf("The token symbol (%s) has been hidden because it contains a URL or a confusable character", symbol)
	}
	return ""
}

func FormatTokenSymbol(symbol string) string {
	if isMaliciousToken(symbol) {
		return "[hidden-symbol] ⚠️"
	}
	return symbol
}

func isMaliciousToken(symbol string) bool {
	containsUrls := len(xurls.Relaxed.FindAllString(symbol, -1)) > 0
	isConfusable := len(confusables.IsConfusable(symbol, false, []string{"LATIN", "COMMON"})) > 0
	isMixedScript := confusables.IsMixedScript(symbol, nil)
	return containsUrls || isConfusable || isMixedScript || strings.ToUpper(symbol) == "ETH"
}

func ReverseSlice[S ~[]E, E any](s S) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func AddBigInts(a, b []byte) []byte {
	return new(big.Int).Add(new(big.Int).SetBytes(a), new(big.Int).SetBytes(b)).Bytes()
}

// GetTimeToNextWithdrawal calculates the time it takes for the validators next withdrawal to be processed.
func GetTimeToNextWithdrawal(distance uint64) time.Time {
	minTimeToWithdrawal := time.Now().Add(time.Second * time.Duration((distance/config.MaxValidatorsPerWithdrawalSweep)*config.SecondsPerSlot))
	timeToWithdrawal := time.Now().Add(time.Second * time.Duration((float64(distance)/float64(config.MaxWithdrawalsPerPayload))*float64(config.SecondsPerSlot)))

	if timeToWithdrawal.Before(minTimeToWithdrawal) {
		return minTimeToWithdrawal
	}

	return timeToWithdrawal
}

func EpochsPerDay() uint64 {
	return (uint64(Day.Seconds()) / ChainParams.Time.SlotsPerEpoch) / ChainParams.Time.SecondsPerSlot
}

func GetFirstAndLastEpochForDay(day uint64) (firstEpoch uint64, lastEpoch uint64) {
	firstEpoch = day * EpochsPerDay()
	lastEpoch = firstEpoch + EpochsPerDay() - 1
	return firstEpoch, lastEpoch
}

func GetLastBalanceInfoSlotForDay(day uint64) uint64 {
	return ((day+1)*EpochsPerDay() - 1) * ChainParams.Time.SlotsPerEpoch
}

// ForkVersionAtEpoch returns the forkversion active a specific epoch
func ForkVersionAtEpoch(epoch uint64) *types.ForkVersion {
	if epoch >= Config.Chain.ClConfig.CappellaForkEpoch {
		return &types.ForkVersion{
			Epoch:           Config.Chain.ClConfig.CappellaForkEpoch,
			CurrentVersion:  MustParseHex(Config.Chain.ClConfig.CappellaForkVersion),
			PreviousVersion: MustParseHex(Config.Chain.ClConfig.BellatrixForkVersion),
		}
	}
	if epoch >= Config.Chain.ClConfig.BellatrixForkEpoch {
		return &types.ForkVersion{
			Epoch:           Config.Chain.ClConfig.BellatrixForkEpoch,
			CurrentVersion:  MustParseHex(Config.Chain.ClConfig.BellatrixForkVersion),
			PreviousVersion: MustParseHex(Config.Chain.ClConfig.AltairForkVersion),
		}
	}
	if epoch >= Config.Chain.ClConfig.AltairForkEpoch {
		return &types.ForkVersion{
			Epoch:           Config.Chain.ClConfig.AltairForkEpoch,
			CurrentVersion:  MustParseHex(Config.Chain.ClConfig.AltairForkVersion),
			PreviousVersion: MustParseHex(Config.Chain.ClConfig.GenesisForkVersion),
		}
	}
	return &types.ForkVersion{
		Epoch:           0,
		CurrentVersion:  MustParseHex(Config.Chain.ClConfig.GenesisForkVersion),
		PreviousVersion: MustParseHex(Config.Chain.ClConfig.GenesisForkVersion),
	}
}

func GetSigningDomain() ([]byte, error) {
	beaconConfig := prysm_params.BeaconConfig()
	genForkVersion, err := hex.DecodeString(strings.Replace(Config.Chain.ClConfig.GenesisForkVersion, "0x", "", -1))
	if err != nil {
		return nil, err
	}

	domain, err := signing.ComputeDomain(
		beaconConfig.DomainDeposit,
		genForkVersion,
		beaconConfig.ZeroHash[:],
	)

	if err != nil {
		return nil, err
	}

	return domain, err
}

// SlotsPerSyncCommittee returns the count of slots per sync committee period
// (might be wrong for the first sync period at atlair which might be shorter, see https://eth2book.info/capella/annotated-spec/#sync-committee-updates)
func SlotsPerSyncCommittee() uint64 {
	return Config.Chain.ClConfig.EpochsPerSyncCommitteePeriod * ChainParams.Time.SlotsPerEpoch
}

// GetRemainingScheduledSyncDuties returns the remaining count of scheduled slots given the stats of the current period, while also accounting for exported slots.
//
// Parameters:
//   - validatorCount: the count of validators associated with the stats.
//   - stats: the current sync committee stats of the validators
//   - lastExportedEpoch: the last epoch that was exported into the validator_stats table
//   - firstEpochOfPeriod: the first epoch of the current sync committee period
func GetRemainingScheduledSyncDuties(validatorCount int, stats types.SyncCommitteesStats, lastExportedEpoch, firstEpochOfPeriod uint64) uint64 {
	// check how many sync duties remain in the current sync committee based on firstEpochOfPeriod
	slotsPerSyncCommittee := SlotsPerSyncCommittee()
	if firstEpochOfPeriod <= Config.Chain.ClConfig.AltairForkEpoch {
		if firstEpochOfPeriod+SlotsPerSyncCommittee() < Config.Chain.ClConfig.AltairForkEpoch {
			// not a valid sync committee as altair comes after the complete sync committee period
			return 0
		}

		// the first sync period at altair might be shorter, see https://eth2book.info/capella/annotated-spec/#sync-committee-updates
		firstEpochOfNextSyncPeriod := FirstEpochOfSyncPeriod(SyncPeriodOfEpoch(Config.Chain.ClConfig.AltairForkEpoch) + 1)
		slotsPerSyncCommittee = (firstEpochOfNextSyncPeriod - Config.Chain.ClConfig.AltairForkEpoch) * ChainParams.Time.SlotsPerEpoch
	}
	dutiesPerSyncCommittee := slotsPerSyncCommittee * uint64(validatorCount)

	// check how many duties are already exported
	exportedEpochs := uint64(0)
	if lastExportedEpoch >= firstEpochOfPeriod {
		exportedEpochs = lastExportedEpoch - firstEpochOfPeriod + 1
	}
	exportedDuties := exportedEpochs * ChainParams.Time.SlotsPerEpoch * uint64(validatorCount)

	// calculate how many duties are remaining i.e. are scheduled
	totalStats := stats.MissedSlots + stats.ParticipatedSlots + stats.ScheduledSlots
	return (dutiesPerSyncCommittee - ((exportedDuties + totalStats) % dutiesPerSyncCommittee)) % dutiesPerSyncCommittee
}

// AddSyncStats adds the sync stats of a set of validators from a given syncDutiesHistory to the given stats, if stats is nil a new stats object is created.
// Parameters:
//   - validators: the validators to add the stats for
//   - syncDutiesHistory: the sync duties history of all queried validators
//   - stats: the stats object to add the stats to, if nil a new stats object is created
func AddSyncStats(validators []uint64, syncDutiesHistory map[uint64]map[uint64]*types.ValidatorSyncParticipation, stats *types.SyncCommitteesStats) types.SyncCommitteesStats {
	if stats == nil {
		stats = &types.SyncCommitteesStats{}
	}
	for _, validator := range validators {
		v := syncDutiesHistory[validator]
		for _, r := range v {
			slotTime := SlotToTime(r.Slot)
			if r.Status == 0 && time.Since(slotTime) <= time.Minute {
				r.Status = 2
			}
			switch r.Status {
			case 0:
				stats.MissedSlots++
			case 1:
				stats.ParticipatedSlots++
			case 2:
				stats.ScheduledSlots++
			}
		}
	}
	return *stats
}

// To remove all round brackets (including its content) from a string
func RemoveRoundBracketsIncludingContent(input string) string {
	openCount := 0
	result := ""
	for {
		if len(input) == 0 {
			break
		}
		openIndex := strings.Index(input, "(")
		closeIndex := strings.Index(input, ")")
		if openIndex == -1 && closeIndex == -1 {
			if openCount == 0 {
				result += input
			}
			break
		} else if openIndex != -1 && (openIndex < closeIndex || closeIndex == -1) {
			openCount++
			if openCount == 1 {
				result += input[:openIndex]
			}
			input = input[openIndex+1:]
		} else {
			if openCount > 0 {
				openCount--
			} else if openIndex == -1 && len(result) == 0 {
				result += input[:closeIndex]
			}
			input = input[closeIndex+1:]
		}
	}
	return result
}

func SortedUniqueUint64(arr []uint64) []uint64 {
	if len(arr) <= 1 {
		return arr
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})

	result := make([]uint64, 1, len(arr))
	result[0] = arr[0]
	for i := 1; i < len(arr); i++ {
		if arr[i-1] != arr[i] {
			result = append(result, arr[i])
		}
	}

	return result
}

type HttpReqHttpError struct {
	StatusCode int
	Url        string
	Body       []byte
}

func (err *HttpReqHttpError) Error() string {
	return fmt.Sprintf("error response: url: %s, status: %d, body: %s", err.Url, err.StatusCode, err.Body)
}

func HttpReq(ctx context.Context, method, url string, params, result interface{}) error {
	var err error
	var req *http.Request
	if params != nil {
		paramsJSON, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("error marshaling params for request: %w, url: %v", err, url)
		}
		req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(paramsJSON))
		if err != nil {
			return fmt.Errorf("error creating request with params: %w, url: %v", err, url)
		}
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %w, url: %v", err, url)
		}
	}
	req.Header.Set("Content-Type", "application/json")
	httpClient := &http.Client{Timeout: time.Minute}
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return &HttpReqHttpError{
			StatusCode: res.StatusCode,
			Url:        url,
			Body:       body,
		}
	}
	if result != nil {
		err = json.NewDecoder(res.Body).Decode(result)
		if err != nil {
			return fmt.Errorf("error unmarshaling response: %w, url: %v", err, url)
		}
	}
	return nil
}

func ReverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func GetCurrentFuncName() string {
	pc, _, _, _ := runtime.Caller(1)
	return runtime.FuncForPC(pc).Name()
}

func GetParentFuncName() string {
	pc, _, _, _ := runtime.Caller(2)
	return runtime.FuncForPC(pc).Name()
}

// Returns true if the given block number is 0 and if it is (according to its timestamp) included in slot 0
//
// This is only true for networks that launch with active PoS at block 0 which requires
//
//   - Belatrix happening at epoch 0 (pre condition for merged networks)
//   - Genesis for PoS to happen at the same timestamp as the first block
func IsPoSBlock0(number uint64, ts int64) bool {
	if number > 0 {
		return false
	}

	if Config.Chain.ClConfig.BellatrixForkEpoch > 0 {
		return false
	}

	return time.Unix(int64(ChainParams.Genesis.MinGenesisTime-Config.Chain.ClConfig.GenesisDelay), 0).UTC().Equal(time.Unix(ts, 0))
}

func FormatDepositStatus(queuedAtEpoch, processedAtEpoch int64) template.HTML {
	if queuedAtEpoch == -2 && processedAtEpoch == -2 {
		return `<span class="badge badge-pill bg-success text-white" style="font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The deposit was processed by the beaconchain">Processed</span>`
	}
	if queuedAtEpoch == -1 && processedAtEpoch == -1 {
		return `<span class="badge badge-pill bg-light text-dark" style="font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The deposit was included by the beaconchain but has not yet been queued for processing">Pending</span>`
	}

	if queuedAtEpoch >= 0 && processedAtEpoch == -1 {
		return `<span class="badge badge-pill text-dark" style="background: rgba(179, 159, 70, 0.8); font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The deposit is queued and will be processed soon">Queued</span>`
	}

	if queuedAtEpoch >= 0 && processedAtEpoch >= 0 {
		return `<span class="badge badge-pill bg-success text-white" style="font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The deposit was processed by the beaconchain">Processed</span>`
	}

	return ""
}

func FormatConsolidationStatus(queuedAtEpoch, processedAtEpoch int64, consolidationType string) template.HTML {
	if consolidationType == "Credentials Update" {
		return `<span class="badge badge-pill bg-success text-white" style="font-size: 12px; font-weight: 500;">Processed</span>`
	}

	if queuedAtEpoch == -1 && processedAtEpoch == -1 {
		return `<span class="badge badge-pill bg-light text-dark" style="font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The consolidation was included by the beaconchain but has not yet been queued for processing">Pending</span>`
	}

	if queuedAtEpoch >= 0 && processedAtEpoch == -1 {
		return `<span class="badge badge-pill text-dark" style="background: rgba(179, 159, 70, 0.8); font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The consolidation is queued and will be processed soon">Queued</span>`
	}

	if queuedAtEpoch >= 0 && processedAtEpoch >= 0 {
		return template.HTML(fmt.Sprintf(`<span class="badge badge-pill bg-success text-white" style="font-size: 12px; font-weight: 500;" data-toggle="tooltip" title="The consolidation was processed in epoch %d by the beaconchain">Processed</span>`, processedAtEpoch))
	}

	return ""
}

// SafeDivideFloat prevents division by zero and returns 0.0 in such cases
func SafeDivideFloat(numerator, denominator codec.Uint64Str) float32 {
	if denominator == 0 {
		return 0.0
	}
	return float32(numerator) / float32(denominator)
}

// BigMin returns the smaller of two big.Int values
func BigMin(a, b *big.Int) *big.Int {
	if a.Cmp(b) <= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}