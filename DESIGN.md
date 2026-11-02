---
name: bth4j dashboard
description: The operator window of a background-task library, built as a printed departure timetable and station board.
colors:
  paper-ground: "#f6f3ec"
  paper-rail: "#efeadf"
  paper-surface: "#fffdf8"
  paper-sunk: "#f1ede3"
  ink-band: "#15181c"
  band-text: "#f3efe4"
  band-muted: "#a39e90"
  hairline: "#ddd6c7"
  hairline-strong: "#b9b1a0"
  ink-text: "#1d2025"
  muted-text: "#6b6960"
  ink: "#15181c"
  on-ink: "#f6f3ec"
  row-hover: "#f3efe4"
  selected-wash: "#e9f2f6"
  waiting-amber: "#a8650a"
  waiting-amber-wash: "#fdf0cf"
  waiting-amber-line: "#e3b659"
  live-teal: "#1c6f8c"
  live-teal-wash: "#e2eff4"
  arrived-green: "#1f7a3f"
  arrived-green-wash: "#e3f1e6"
  signal-red: "#b4231c"
  signal-red-wash: "#fbe7e4"
  state-queued: "#3b5b8f"
  state-scheduled: "#a8650a"
  state-leased: "#1c6f8c"
  state-succeeded: "#1f7a3f"
  state-dead: "#b4231c"
  state-cancelled: "#5d6773"
  state-expired: "#9a3412"
  state-none: "#7a776d"
typography:
  heading:
    fontFamily: "system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif"
    fontSize: "20px"
    fontWeight: 650
    lineHeight: 1.2
    letterSpacing: "-0.01em"
  title:
    fontFamily: "system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif"
    fontSize: "15px"
    fontWeight: 650
    lineHeight: 1.3
  body:
    fontFamily: "system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif"
    fontSize: "13.5px"
    fontWeight: 400
    lineHeight: 1.45
    fontFeature: "tnum"
  figure:
    fontFamily: "system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif"
    fontSize: "24px"
    fontWeight: 650
    lineHeight: 1
    letterSpacing: "-0.01em"
    fontFeature: "tnum"
  label:
    fontFamily: "system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif"
    fontSize: "11.5px"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "0.05em"
  badge:
    fontFamily: "system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif"
    fontSize: "10.5px"
    fontWeight: 700
    letterSpacing: "0.06em"
  mono:
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, \"Liberation Mono\", monospace"
    fontSize: "12.5px"
    fontWeight: 400
    lineHeight: 1.45
rounded:
  sm: "3px"
  md: "6px"
  lg: "8px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "24px"
components:
  button:
    backgroundColor: "{colors.paper-surface}"
    textColor: "{colors.ink-text}"
    rounded: "{rounded.md}"
    padding: "0 12px"
    height: "32px"
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.on-ink}"
    rounded: "{rounded.md}"
    padding: "0 12px"
    height: "32px"
  button-danger:
    backgroundColor: "transparent"
    textColor: "{colors.signal-red}"
    rounded: "{rounded.md}"
    padding: "0 12px"
    height: "32px"
  button-small:
    backgroundColor: "{colors.paper-surface}"
    textColor: "{colors.ink-text}"
    rounded: "{rounded.md}"
    padding: "0 10px"
    height: "28px"
  input:
    backgroundColor: "{colors.paper-surface}"
    textColor: "{colors.ink-text}"
    rounded: "{rounded.md}"
    padding: "0 10px"
    height: "32px"
  nav-link:
    textColor: "{colors.ink-text}"
    padding: "8px 18px 8px 15px"
  nav-link-active:
    backgroundColor: "{colors.selected-wash}"
    textColor: "{colors.ink-text}"
  state-badge:
    backgroundColor: "{colors.state-queued}"
    textColor: "#ffffff"
    typography: "{typography.badge}"
    rounded: "{rounded.sm}"
    padding: "2px 8px"
    width: "86px"
  tag-issue:
    backgroundColor: "{colors.signal-red-wash}"
    textColor: "{colors.signal-red}"
    typography: "{typography.badge}"
    rounded: "{rounded.sm}"
    padding: "2px 7px"
  board-strip-item:
    backgroundColor: "{colors.paper-surface}"
    textColor: "{colors.ink-text}"
    typography: "{typography.figure}"
    padding: "12px 18px"
  table-header:
    textColor: "{colors.muted-text}"
    typography: "{typography.label}"
    padding: "8px 12px"
  toast:
    backgroundColor: "{colors.ink-band}"
    textColor: "{colors.band-text}"
    rounded: "{rounded.md}"
    padding: "10px 12px"
  dialog:
    backgroundColor: "{colors.paper-surface}"
    textColor: "{colors.ink-text}"
    rounded: "{rounded.lg}"
    padding: "18px 20px"
---

# Design System: bth4j dashboard

## Overview

**Creative North Star: "The Departure Board"**

A background-task dashboard is a departure board, not a settings panel. Every row is something that will happen at a time, and the screen is organised around when, in what order, and whether it left on time. The surface is warm printed-timetable paper in light and a near-black Solari board in dark, under an ink-black station-header band whose clock ticks. It refuses the grey admin shell with a blue accent and equal-weight tables, and it refuses dark terminal neon.

Density is working density: 13.5px text, 32px controls, hairline-ruled tables, no cards inside cards. One system sans carries everything, with tabular numerals so counts line up from page to page; identifiers sit in the system mono. Colour is rationed by job: amber means only "waiting on time", one teal accent means selection and live state, and the signal colours are reserved for state. Motion is 150ms colour transitions and the once-a-second countdown tick; nothing enters or choreographs.

The system fonts are a product constraint (the CSP allows no font files), not a style choice. Character comes from the figures, the rules and the colour discipline instead.

**Key Characteristics:**
- Warm paper ground, ink band, hairline rules; flat everywhere except floating toasts and dialogs.
- Amber is reserved for "waiting on time"; teal owns selection and live state.
- Tabular numerals everywhere, right-aligned counts, fixed-width state badges.
- Identifiers in mono; times read as departures ("in 2:14", relative first, clock time after).
- Light and dark follow the OS with the same roles in both.

## Colors

A warm paper and ink palette with three jobs of colour: amber for time, teal for live, and a small set of signal colours for state.

### Primary
- **Live Teal** (#1c6f8c): selection and live state only: the side-rail active bar, the focus ring, the caret, link-hover underline, LEASED, "fire" kind labels, the text selection wash. It is the one accent.

### Secondary
- **Waiting Amber** (#a8650a): the single job of "waiting on time": future countdowns, late times, SCHEDULED counts and badge, the waiting-for-time figure, and the changed-cell tint (Waiting Amber Wash #fdf0cf). Amber Line (#e3b659) is the matching light edge.

### Tertiary
- **Arrived Green** (#1f7a3f): SUCCEEDED, the healthy and arrived colour (wash #e3f1e6).
- **Signal Red** (#b4231c): DEAD, issues, stale rows, destructive actions and alert counts (wash #fbe7e4). Expired uses Burnt Orange (#9a3412) and Cancelled uses Slate (#5d6773); Queued is Timetable Blue (#3b5b8f).

### Neutral
- **Paper Ground** (#f6f3ec): page background. **Paper Rail** (#efeadf): the side rail. **Paper Surface** (#fffdf8): panels, tables, inputs. **Paper Sunk** (#f1ede3): code blocks, banners, skeletons.
- **Ink Band** (#15181c): the top band, toasts; text on it Band Text (#f3efe4), secondary Band Muted (#a39e90). **Ink** (#15181c) is also the primary button and the reached stops of the route; Ink Text (#1d2025) is body text; Muted Text (#6b6960) is secondary text and zeros.
- **Hairline** (#ddd6c7) is the row rule; **Hairline Strong** (#b9b1a0) is control borders and header rules. Row Hover (#f3efe4) and Selected Wash (#e9f2f6) are the interaction fills.
- State greys: No-state (#7a776d).

Dark theme (OS preference) keeps every role and shifts values; see the sidecar colorMeta. Examples: ground #101214, surface #171a1e, text #ece6d8, amber #e5a21b, teal #4fb0d2, green #4fb577, red #e4675b; ink and on-ink swap so the primary button becomes warm white.

### Named Rules
**The One Job Amber Rule.** Amber means waiting on time and nothing else. It never marks focus, selection, hover, paused, stale or warning.
**The One Accent Rule.** Teal owns selection and live state. A second accent for navigation or decoration is not allowed.
**The Text Carries State Rule.** Colour is never the only carrier of a state; badges and labels always spell the library's own state word.

## Typography

**Display Font:** none; the system sans stack serves every role (system-ui, -apple-system, Segoe UI, Roboto, Helvetica Neue, Arial)
**Body Font:** the same system sans
**Label/Mono Font:** system mono (ui-monospace, SFMono-Regular, Menlo, Consolas) for identifiers, queue names, the clock

**Character:** Plain and instrument-like. Hierarchy comes from weight (400 / 550 / 650 / 700), size steps and case, never from a second family. Tabular figures are switched on at the body.

### Hierarchy
- **Heading** (650, 20px, 1.2, -0.01em): page title; on a task page the task id is set in mono at this size.
- **Title** (650, 15px, 1.3): panel and section headings; dialog title 16px.
- **Body** (400, 13.5px, 1.45): everything else; buttons are weight 550.
- **Figure** (650, 24px, 1, -0.01em): the six board-strip numbers (20px on phones).
- **Label** (600, 11.5px, 0.05em, uppercase): table headers, form labels, route stop names, strip captions (12px).
- **Badge** (700, 10.5px, 0.06em): state badges and tags.
- **Mono** (400, 12.5px): ids, queue names, payloads, the 13px band clock.

### Named Rules
**The Tabular Figures Rule.** Every number is tabular and right-aligned in its column; counts share one column width across pages.
**The Departure Time Rule.** A time leads with its relative part in weight 600 and follows with the clock time in muted 12px; late times turn amber.

## Layout

A fixed shell: a 52px band across the top, a 200px side rail left, a content column with 20px 24px padding. Pages are stacked panels and ruled tables on the ground; the overview puts the board strip first, then a 3:1 split of counts table and "Next to leave" board (stacking under 1100px). Spacing runs on a loose 4/8/12/16/24 rhythm (gaps of 10 and 14 also occur). Tables scroll horizontally inside their wrapper rather than squeezing columns.

Below 760px the rail collapses behind a menu button and becomes a wrapped row with a bottom bar for the active item, the band drops the clock, user and health sub-text, the board strip becomes a two-column grid, the route strip turns vertical, and secondary columns of the departures board fold into inline labels.

## Elevation & Depth

Flat and ruled. Depth is carried by tonal layers (ground, surface, sunk) and hairline borders, not shadows. The only shadow is the floating one on toasts and dialogs.

### Shadow Vocabulary
- **Float** (`box-shadow: 0 8px 24px rgba(30, 24, 10, 0.18)`; dark `0 8px 24px rgba(0, 0, 0, 0.5)`): toasts and the confirm dialog over a 55% dark scrim.

### Named Rules
**The Flat-By-Default Rule.** Panels, tables, strips and buttons carry borders, never shadows. Only things that float above the page get Float.

## Shapes

Small, quiet radii: 3px on badges and tags, 6px on buttons, inputs, panels and tables, 8px on the dialog and the login card. Rules are 1px hairlines; the strip and header rules use the strong hairline. The route strip is the one geometric device: a 2px line with 10px station circles, solid ink when reached and dashed when pending. The health indicator is a 9px circle.

## Components

### Buttons
- **Shape:** 6px radius, 32px high (small 28px), 1px strong-hairline border, weight 550.
- **Default:** surface fill, ink text; hover darkens the border to text colour and fills sunk.
- **Primary:** ink fill, on-ink text, 0.9 opacity on hover.
- **Danger:** transparent, red text and border; hover fills red with white text.
- **Press / disabled:** 1px downward nudge; 0.45 opacity, not-allowed cursor. Focus is a 2px teal outline offset 2px.

### State Badges
Fixed-width "platform" labels: 86px minimum (large 110px), 3px radius, 10.5px bold, white text on the state colour. They always carry the state word.

### Inputs / Fields
32px high, 6px radius, strong-hairline border, surface fill, muted placeholder. Labels sit above in the uppercase label style. Focus uses the global teal outline.

### Navigation
Side rail links: 8px 18px, a 3px transparent left bar that turns teal when active, with a selected wash and weight 650; hover is Row Hover. Counts sit right in muted 11.5px, red for Dead and Issues. On phones the rail becomes a wrapped row with a 3px bottom bar.

### Tables
Surface panel with 6px radius, hairline row rules, uppercase 11.5px muted headers over a strong rule, 8px 12px cells, row hover fill, a totals footer with a strong top rule. Zeros are muted, non-zero counts are links, changed cells hold an amber wash that fades over 600ms, stale rows take the red wash.

### Board Strip
One ruled line of figures between two strong hairlines on surface: 24px bold number beside a small uppercase caption, separated by hairline dividers. The waiting figure is amber, dead and issues turn red when non-zero, zeros mute.

### Route Strip (signature)
The task's route as stations on one line (created, due, claimed, finished): a 2px line, 10px circles, ink when reached, dashed when pending; vertical on phones.

### Toasts and Dialog
Toasts are ink-band cards bottom right with a small status dot (red or green) and an inline-SVG close. The dialog is a surface card, 8px radius, max 460px, on a dark scrim; the confirm button is primary or danger.

## Do's and Don'ts

### Do:
- **Do** reserve amber (#a8650a light, #e5a21b dark) for waiting on time.
- **Do** use teal for selection, focus and live state, and nothing else.
- **Do** set counts, times and ids in tabular figures, right-aligned, and ids in the system mono.
- **Do** keep state badges fixed width and always spelled out.
- **Do** build every new surface from the `:root` tokens so light and dark both work.
- **Do** keep tables hairline-ruled and flat; float only toasts and dialogs.
- **Do** keep icons as inline SVG.

### Don't:
- **Don't** add a second accent, a gradient, or decorative colour.
- **Don't** use amber for focus, hover, paused, stale or warnings.
- **Don't** invent trends, sparklines or history the API does not provide.
- **Don't** add entrance choreography or animation beyond 150ms colour transitions and the countdown tick.
- **Don't** hide counts or relabel the library's state words.
- **Don't** add font files, font CDNs or icon fonts.
