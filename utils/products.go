package utils

const GROUP_API = "api"
const GROUP_MOBILE = "mobile"
const GROUP_ADDON = "addon"

var ProductsGroups = map[string]string{
	"sapphire":             GROUP_API,
	"emerald":              GROUP_API,
	"diamond":              GROUP_API,
	"plankton":             GROUP_MOBILE,
	"goldfish":             GROUP_MOBILE,
	"whale":                GROUP_MOBILE,
	"guppy":                GROUP_MOBILE,
	"dolphin":              GROUP_MOBILE,
	"orca":                 GROUP_MOBILE,
	"guppy.yearly":         GROUP_MOBILE,
	"dolphin.yearly":       GROUP_MOBILE,
	"orca.yearly":          GROUP_MOBILE,
	"vdb_addon_1k":         GROUP_ADDON,
	"vdb_addon_1k.yearly":  GROUP_ADDON,
	"vdb_addon_10k":        GROUP_ADDON,
	"vdb_addon_10k.yearly": GROUP_ADDON,
}

var ProductsMapV1ToV2 = map[string]string{
	"plankton": "guppy",
	"goldfish": "guppy",
	"whale":    "dolphin",
}

var ProductsMapV2ToV1 = map[string]string{
	"guppy":                "goldfish",
	"dolphin":              "whale",
	"orca":                 "whale",
	"guppy.yearly":         "goldfish",
	"dolphin.yearly":       "whale",
	"orca.yearly":          "whale",
	"vdb_addon_1k":         "",
	"vdb_addon_1k.yearly":  "",
	"vdb_addon_10k":        "",
	"vdb_addon_10k.yearly": "",
}

func MapProductV2ToV1(product string) string {
	if v, exists := ProductsMapV2ToV1[product]; exists {
		return v
	}
	if _, exists := ProductsMapV1ToV2[product]; exists {
		// just return it if its a v1 product
		return product
	}
	return ""
}
