package codec

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Uint64Str uint64

func (s *Uint64Str) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	v, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid uint64 string: %w", err)
	}
	*s = Uint64Str(v)
	return nil
}