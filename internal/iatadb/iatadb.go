// Package iatadb provides a static mapping from IATA airport codes to
// geographic metadata (country and continent).
//
// The data is generated from the OurAirports public dataset and compiled
// into the binary — no external calls at runtime.
//
// To refresh the dataset:
//
//	go generate ./internal/iatadb/
//
//go:generate go run ./gen
package iatadb

// Lookup returns the Entry for the given IATA code, and whether it was found.
func Lookup(iata string) (Entry, bool) {
	e, ok := DB[iata]
	return e, ok
}

// CountryFor returns the ISO 3166-1 alpha-2 country code for the given IATA,
// or empty string if not found.
func CountryFor(iata string) string {
	return DB[iata].Country
}

// ContinentFor returns the two-letter continent code for the given IATA,
// or empty string if not found.
func ContinentFor(iata string) string {
	return DB[iata].Continent
}

// BuildAllowedSet returns a set of IATA codes permitted by the given country
// and continent allowlists. If both slices are empty, returns nil (no filter).
// An IATA passes if it matches any entry in either list (OR semantics).
func BuildAllowedSet(allowCountries, allowContinents []string) map[string]struct{} {
	if len(allowCountries) == 0 && len(allowContinents) == 0 {
		return nil
	}
	countrySet := make(map[string]struct{}, len(allowCountries))
	for _, c := range allowCountries {
		countrySet[c] = struct{}{}
	}
	continentSet := make(map[string]struct{}, len(allowContinents))
	for _, c := range allowContinents {
		continentSet[c] = struct{}{}
	}
	allowed := make(map[string]struct{})
	for iata, entry := range DB {
		if _, ok := countrySet[entry.Country]; ok {
			allowed[iata] = struct{}{}
			continue
		}
		if _, ok := continentSet[entry.Continent]; ok {
			allowed[iata] = struct{}{}
		}
	}
	return allowed
}
