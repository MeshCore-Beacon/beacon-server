// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import "strings"

// compressesFor reports whether a client gets permessage-deflate. iOS WebKit (every iPhone and iPad
// browser) drops the socket on the first compressed message, and an iPad in desktop mode looks like
// Mac Safari, so only clients known to read it are compressed: Chromium, Firefox and the app.
func compressesFor(userAgent string) bool {
	return strings.HasPrefix(userAgent, "Dart/") ||
		strings.Contains(userAgent, "Chrome/") ||
		strings.Contains(userAgent, "Firefox/")
}
