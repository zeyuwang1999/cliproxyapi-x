package auth

type activeWindow struct {
	active         []string
	cursor         int
	fallbackCursor int
}

type activeWindowState struct {
	Active         []string
	Cursor         int
	FallbackCursor int
}

func snapshotActiveWindow(window activeWindow) activeWindowState {
	return activeWindowState{
		Active:         append([]string(nil), window.active...),
		Cursor:         window.cursor,
		FallbackCursor: window.fallbackCursor,
	}
}

func restoreActiveWindow(window *activeWindow, state activeWindowState) {
	if window == nil {
		return
	}
	window.active = append([]string(nil), state.Active...)
	window.cursor = state.Cursor
	window.fallbackCursor = state.FallbackCursor
}

func (w *activeWindow) sync(ordered []string, limit int) {
	if w == nil {
		return
	}
	if len(ordered) == 0 {
		w.active = nil
		w.cursor = 0
		w.fallbackCursor = 0
		return
	}
	if limit <= 0 || len(ordered) <= limit {
		w.active = append(w.active[:0], ordered...)
		w.cursor = normalizeCursor(w.cursor, len(w.active))
		w.fallbackCursor = normalizeCursor(w.fallbackCursor, len(ordered))
		return
	}

	orderedSet := make(map[string]struct{}, len(ordered))
	for _, key := range ordered {
		orderedSet[key] = struct{}{}
	}

	next := make([]string, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, key := range w.active {
		if _, ok := orderedSet[key]; !ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		next = append(next, key)
		seen[key] = struct{}{}
		if len(next) >= limit {
			break
		}
	}
	if len(next) < limit {
		for _, key := range ordered {
			if _, ok := seen[key]; ok {
				continue
			}
			next = append(next, key)
			seen[key] = struct{}{}
			if len(next) >= limit {
				break
			}
		}
	}
	w.active = next
	w.cursor = normalizeCursor(w.cursor, len(w.active))
	w.fallbackCursor = normalizeCursor(w.fallbackCursor, len(ordered))
}

func (w *activeWindow) pick(ordered []string, limit int, roundRobin bool, predicate func(string) bool) string {
	if len(ordered) == 0 {
		return ""
	}
	if limit <= 0 || len(ordered) <= limit {
		if roundRobin {
			selected, next := pickStringRoundRobin(ordered, w.fallbackCursor, predicate)
			if selected != "" {
				w.fallbackCursor = next
			}
			return selected
		}
		return pickStringFirst(ordered, predicate)
	}

	w.sync(ordered, limit)
	if roundRobin {
		selected, next := pickStringRoundRobin(w.active, w.cursor, predicate)
		if selected != "" {
			w.cursor = next
			return selected
		}
		selected, next = pickStringRoundRobin(ordered, w.fallbackCursor, predicate)
		if selected != "" {
			w.fallbackCursor = next
		}
		return selected
	}

	if selected := pickStringFirst(w.active, predicate); selected != "" {
		return selected
	}
	return pickStringFirst(ordered, predicate)
}

func pickStringFirst(keys []string, predicate func(string) bool) string {
	for _, key := range keys {
		if predicate == nil || predicate(key) {
			return key
		}
	}
	return ""
}

func pickStringRoundRobin(keys []string, start int, predicate func(string) bool) (string, int) {
	if len(keys) == 0 {
		return "", 0
	}
	start = normalizeCursor(start, len(keys))
	for offset := 0; offset < len(keys); offset++ {
		index := (start + offset) % len(keys)
		key := keys[index]
		if predicate != nil && !predicate(key) {
			continue
		}
		return key, index + 1
	}
	return "", start
}
