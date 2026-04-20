# P07 — Monitor Accessibility, Keyboard, and Mobile Pass

**Priority:** P2  
**Effort:** M  
**Status:** Specification / ready for design review  
**Dependencies:** C04 (source drilldown), C05 (session explorer)

## Objective

The monitor will become a daily tool. Treat it like one. Pass accessibility audits, enable full keyboard navigation, and make layouts responsive for narrow screens.

## Design Principles

1. **Accessibility is not a polish layer.** It is a core requirement that affects component choice, color system, and interaction design from the start.
2. **Keyboard navigation mirrors common patterns.** Arrow keys for lists, Enter to open, Escape to close, `/` for search, `?` for help.
3. **Mobile is a secondary pass, not primary.** The monitor is most valuable on desktop during development. Mobile should be usable for quick checks, not a full replacement.
4. **Color and motion respect user preferences.** Support `prefers-reduced-motion` and `prefers-color-scheme`.

## Scope

### Accessibility (A11y)

- WCAG 2.1 AA compliance target for all new and existing monitor views.
- Automated a11y tests using `axe-core` in CI.
- Manual keyboard smoke test checklist.

### Keyboard Navigation

| Key | Context | Action |
|---|---|---|
| `j` / `k` | Global list | Next / previous item |
| `Enter` | List | Open detail / drilldown |
| `Escape` | Detail panel / modal | Close |
| `/` | Global | Focus search input |
| `?` | Global | Show keyboard shortcuts cheat sheet |
| `g` then `s` | Global | Go to sessions |
| `g` then `h` | Global | Go to home / health |
| `g` then `o` | Global | Go to sources |
| `r` | Session list | Refresh |
| `n` | Session detail | Create note (P02) |

### Responsive Layouts

- Breakpoints: `sm: 640px`, `md: 768px`, `lg: 1024px`, `xl: 1280px`.
- Sidebar collapses to hamburger menu below `md`.
- Session transcript switches to single-column card layout below `lg`.
- Source drilldown table scrolls horizontally on narrow screens.
- Search results maintain readability with truncated previews and wrapped metadata.

## Component-Level Changes

### Session Explorer (C05)

- Add `role="list"` and `role="listitem"` to session list.
- Each session row has `tabindex="0"` and `aria-selected` state.
- Transcript uses `role="log"` with `aria-live="polite"` for dynamic content.
- Tool call/result pairs use `details`/`summary` elements (native keyboard support).

### Source Drilldown (C04)

- File list table uses semantic `<table>` with `<th scope="col">`.
- Error rows use `aria-describedby` linking to remediation text.
- Color-coded status chips include text labels (not color-only).

### Search / Workbench (C06)

- Search input has `aria-label="Search sessions and events"`.
- Results announce count via `aria-live` region.
- Filter tags are keyboard-removable (Backspace on focused tag).

## API Sketches

No new backend APIs. This is a frontend-only feature. However, some APIs may need metadata additions:

- `GET /api/health` should include `prefers_reduced_motion` and `theme` preferences if user-specific settings are stored server-side.

If user preferences are stored:

```sql
-- Optional: user preferences table (local single-user mode)
CREATE TABLE IF NOT EXISTS moraine.user_preferences (
  pref_key String,
  pref_value String,
  updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pref_key);
```

## Data Flow

1. **Preferences:** Stored in `localStorage` (frontend) or optional ClickHouse table (if multi-user later).
2. **Theme:** CSS variables switch based on `data-theme` attribute; respects `prefers-color-scheme` if no explicit override.
3. **Keyboard:** Global `keydown` listener in `+layout.svelte` dispatches to context-aware handlers.
4. **A11y:** `aria-*` attributes bound to component state; `axe-core` run in dev mode and CI.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Very long paths and unbroken tokens | `overflow-wrap: break-word; word-break: break-all;` on path cells. `title` attribute for full path on hover. |
| Dynamic content shifts layout | Reserve space for async content with skeleton loaders. Avoid layout thrashing. |
| Tool outputs contain code blocks and tables | Code blocks scroll horizontally. Tables use `overflow-x: auto` container. |
| Focus loss after async update | Restore focus to logical element (e.g., first new result) after refresh. |
| Screen reader verbosity on large transcripts | Virtual scrolling with `aria-setsize` and `aria-posinset` on visible items only. |

## Acceptance Contract

### Functional
- [ ] All interactive elements are reachable via keyboard alone.
- [ ] Keyboard shortcut cheat sheet is accessible via `?` and documents all shortcuts.
- [ ] Monitor UI is usable at 320px width (basic functionality, no data loss).
- [ ] Dark/light mode toggles correctly and persists across reloads.

### Operational
- [ ] A11y audit passes with zero critical or serious violations (`axe-core`).
- [ ] Lighthouse accessibility score ≥ 95 for all primary routes.
- [ ] Bundle size increase from a11y libraries < 20KB gzipped.

### Safety
- [ ] High-contrast mode does not rely on color alone for status indicators (icons + text).
- [ ] Focus indicators are visible and do not expose hidden content prematurely.

### Compatibility
- [ ] Works in latest Chrome, Firefox, Safari, and Edge.
- [ ] Keyboard shortcuts do not conflict with common screen reader keys (adjust if needed).

### Observability
- [ ] CI runs `axe-core` against monitor build artifacts.
- [ ] Manual keyboard smoke test checklist is documented in `docs/operations/monitor-a11y.md`.

## PR Sequencing

1. `refactor(monitor): introduce design tokens for color, spacing, and typography`  
   - CSS custom properties; no visual change yet.
2. `feat(monitor): add keyboard navigation framework and shortcut registry`  
   - Global keydown handler; context-aware routing.
3. `feat(monitor): add accessibility attributes to session explorer`  
   - ARIA roles, focus management, live regions.
4. `feat(monitor): add accessibility attributes to source drilldown and search`  
   - Table semantics, filter keyboard interaction.
5. `feat(monitor): implement responsive layouts and mobile breakpoints`  
   - Collapsible sidebar, card layouts, horizontal scroll containers.
6. `feat(monitor): add theme switching and reduced-motion support`  
   - Dark/light mode; `prefers-reduced-motion`.
7. `test(monitor): add axe-core CI job and keyboard smoke test fixtures`  
   - Automated a11y regression testing.

## Open Questions

1. **Should we use a UI component library?** Current monitor is custom Svelte. Evaluate Headless UI or similar only if it reduces a11y burden without increasing bundle size significantly.
2. **Virtual scrolling for large transcripts?** Yes, but ensure screen readers still announce correct position. Use `svelte-virtual-list` or similar with ARIA support.
3. **Mobile app vs responsive web?** Responsive web is sufficient for P2. Native app (S04) is P3.
