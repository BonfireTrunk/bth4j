---
version: 1
slug: "dashboard-index-html"
primary_target: "dashboard/index.html"
related_targets: ["dashboard/app.css","dashboard/app.js","dashboard/pages/task.js"]
---

# Surface: bth4j dashboard (dashboard/, all pages)

Scope: the whole static dashboard served by the admin HTTP server: shell, login, overview, queues, tasks, task, dead, schedules, workers, issues. Visitor mode: Operate.

Audience and job: developers of the host application diagnosing a task that did not run (primary); on-call operators glancing at health, workers and counts. Task: find a task, read its history, retry or cancel it with the revision seen; pause or resume queues; pause, trigger or delete schedules. Proof/content: live API data only; nothing invented. Constraints: no build step, Vue 3 global build vendored in dashboard/vendor, plain CSS, system fonts only, light and dark themes, CSP forbids external styles and fonts, inline SVG only.

Memorable moment: the departure board. Everything that waits on time (due, next fire, lease, heartbeat) reads like a departure: a live countdown in tabular figures, amber reserved for "waiting on time", and rows that keep their identity and hold a highlight when a poll changes them.

Interview (completed by the assistant at the owner's instruction, 2026-11-03): task and information as above; important states: DEAD and EXPIRED (something to act on), LEASED (running now), SCHEDULED (waiting), paused queues and schedules, stale workers, runner not RUNNING; frequency: polled every 5 s, open for minutes at a time during an incident, hours during development; what must remain untouched: every function, every label, the API client, the revision-carrying actions, the two roles; what would make a polished result feel wrong: decoration, animation for its own sake, hidden counts, a theme that only works in light mode. Owner's words: "polished, fast and easy on the eye".

## Direction contract

THESIS: a background-task dashboard is a departure board, not a settings panel. Every row is something that will happen at a time, and the screen is organised around when, in what order, and whether it left on time. It refuses the category default (grey admin shell, blue accent, equal-weight tables) and its opposite (dark terminal neon).

OWN-WORLD: the printed departure timetable and the station board. Light theme: a warm paper ground, an ink-black top band with the name set like a station header, amber reserved for anything waiting on time, deep green for arrived (succeeded), signal red for dead and expired, slate for cancelled. Dark theme: the Solari board, near-black ground, warm-white type, the same amber and signal colours. One system sans everywhere with tabular numerals and a tight 1.15 scale; identifiers in the system mono. Rows have the timetable's hairline rules, column headers in small caps-sized labels, numbers right-aligned on one shared column width across pages, state badges as fixed-width "platform" labels. Raises: amber reserved for one job only (from the airport sign system); changed cells hold a highlight for five seconds after a poll (from the gate board); one accent owns selection and live state (from the metro tiles); fixed column widths so counts align page to page (from the botanical folio); the task page's first viewport is one thing, the state and the actions (from the vertical feed); every number in tabular figures, right-aligned (from the data-sublime field).

STORY: the visitor sees at once whether the runner is up and whether anything is dead or late, reads the board in due order, opens a task, reads its route (created, due, claimed, finished) as stations on one line, and acts with the revision they saw.

FIRST VIEWPORT (overview, 1440x900): a 52px ink-black top band: the name at left set as a station header with a ticking clock in tabular mono, the runner health as a lit indicator with its worker id, the user and role, log out. Below, a 200px side rail on the paper ground with the seven pages, the current one marked by a bar in the live accent at the left edge (selection and live state share one accent; amber keeps its one job) and a count beside Dead and Issues. The main column opens with a board strip of six figures in one row (stored, running now, waiting for time, dead, issues, workers) in large tabular numbers; "waiting for time" is the amber figure and exists because the board is about time, and "workers" is the on-call glance PRODUCT.md names. Then the counts table: queues as rows in a fixed-width mono platform column, the seven states as fixed-width columns in the state order, totals row, each non-zero count a link, zeros dimmed, SCHEDULED counts in amber, cells that changed since the last poll holding an amber tint. Below it, "Next to leave": the delayed tasks and schedule fires due soonest, one row each with its amber countdown, event, queue and kind. The primary action on this page is reading; the first clickable thing is the Dead count.

FORM: departure timetable and station board, candidate 3 of 7 on the grounded list (after the commit graph and the parcel-tracking timeline, ahead of the flight-strip board, the ticket rail, the reference page and the SBB identity program). seed:c49f0806fddd1e1a.

SIGNATURE INTERACTION: live countdowns. Due, next fire, lease expiry and last heartbeat tick once a second in tabular figures ("in 2:14", "1:03 ago") without a request; changed rows after a poll keep their row identity and hold an amber tint for three seconds (one poll is five, so a quiet board settles between polls). Motion grammar: 150 ms colour transitions, no entrance choreography, reduced motion disables the tint fade.

Revision 2026-11-03 (after the finish review): amber removed from focus ring, caret, selection, link hover, the paused tag and the stale banner; those use the live accent or plain surfaces. Next-to-leave board and the band clock added; the task page sets its id in mono at heading size.

Challenger verdicts: airport yellow wayfinding declined (kept: one colour reserved for one job); boarding pass and gate board competitive (kept: changed rows hold their highlight); metro tiles declined (kept: one accent owns live state); botanical folio declined (kept: fixed scale across comparables); vertical feed declined (kept: one thing owns the first viewport of a task); data-sublime field declined (kept: tabular numeric density).

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance.
