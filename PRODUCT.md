# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Confirmed: developers of the application that embeds bth4j, looking at the dashboard while developing and when a task did not run as expected; and on-call operations people who do not know the task code, glance at health, workers and counts, and hand off to a developer. Developers are the primary audience. Support staff acting for customers are not a target audience today.

## Product Purpose

The dashboard is the operator window of bth4j, a Java background-task library with Redis and PostgreSQL stores. Confirmed jobs, in order: diagnose and fix a task (find it, read its history and failures, retry or cancel it), and day-to-day control of queues and schedules (pause, resume, trigger, delete). A monitoring wall with trends is a possible later addition; the API keeps no history today, so nothing may pretend to.

Success: a developer answers "why did task X not run" within a minute of opening the page, and an on-call person sees in one glance whether the runner is healthy and whether anything is dead or stuck.

## Operating Context

Served by the bth4j admin HTTP server (JDK httpserver) from a static directory, same origin as the JSON API under /api/v1. Login is a session cookie with a CSRF token; roles are ADMIN (read and act) and VIEWER (read only). Pages poll every 5 seconds; counts and lists are live reads, not a snapshot. Viewed on developer workstations and laptops, light or dark OS theme; occasionally from a phone by on-call staff (inferred from the audience, not confirmed as a hard requirement).

## Capabilities and Constraints

Confirmed constraints that every design decision preserves:
- No build step: Vue 3 global build vendored in dashboard/vendor, plain CSS, ES modules loaded directly; no npm, bundler or cache-busting.
- System fonts only; no font CDN or font files.
- Light and dark themes both supported, following the OS preference.

Fixed by the server: CSP allows scripts from self with 'unsafe-eval' (Vue template compiler), everything else from self. Inline SVG is the only icon mechanism available.

Capabilities (from the API): overview counts per queue and state, runner health, queues with pause and resume, task listing by state with a queue filter and cursor paging, task detail with payload, steps, failures, headers and timeline, retry, cancel with reason, delete, dead-task bulk retry, schedules with pause, resume, trigger and delete, workers with heartbeat age, issues.

Terminology is the library's: task states QUEUED, SCHEDULED, LEASED, SUCCEEDED, DEAD, CANCELLED, EXPIRED; queue; event (task type); revision; attempt; lease; schedule; worker; issue.

## Brand Commitments

Name: bth4j (lowercase), published by Bonfire Trunk. No logo, no brand palette, no voice guide exist. The owner's standing brief: "simple but polished"; "polished, fast and easy on the eye" (2026-11-03).

## Evidence on Hand

A running demo with two workers and a producer generating realistic tasks, schedules, failures and issues (local only). The Python dev server dashboard/dev/fake-api.py serves fixture data without Java. No screenshots, testimonials or usage data exist; none may be invented.

## Product Principles

- The task is the unit of truth: every page leads back to a task and every task page answers "what happened and what can I do".
- Show the library's own words and numbers; never relabel a state or hide a count.
- Act only with a revision: every mutation carries the revision the operator saw, and a conflict is shown, not swallowed.
- Nothing fake: no trends, sparklines or history the API does not provide.
- Stays a static directory anyone can serve and read.

## Accessibility & Inclusion

Keyboard operation and visible focus for every control; colour never the only carrier of a state (badges carry text); contrast that holds in both themes. No formal standard was named (inferred requirement from an operator tool used in long sessions).
