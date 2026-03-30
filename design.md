# Design System — Dark Dashboard

A reusable design reference extracted from the APAC Portfolio Finance Monitor.
Copy the CSS custom properties into any new project to maintain visual consistency.

---

## Core Philosophy

- **Dark-first**: deep navy/charcoal backgrounds, not pure black
- **Layered surfaces**: 5 surface levels create depth without shadows
- **Cool-toned**: blues, purples, teals — no warm neutrals
- **Data-dense**: small type, tight spacing, maximum information density
- **Minimal chrome**: let data breathe; borders over fills for interactive states

---

## CSS Variables — Copy-Paste Root Block

```css
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

:root {
  /* ── SURFACES (darkest → lightest) ── */
  --bg: #07090f;   /* page background */
  --s1: #0b1018;   /* header / top bar */
  --s2: #0f1520;   /* cards, tables, panels */
  --s3: #131c2a;   /* elevated: chips, dropdowns, tooltips */
  --s4: #182236;   /* deep hover / pressed state */

  /* ── BORDERS ── */
  --b1: #1b2a42;   /* default border */
  --b2: #213352;   /* moderate: hover border, table header separator */
  --b3: #2a4068;   /* strong: focus ring, emphasized divider */

  /* ── TEXT ── */
  --t1: #e8eef8;   /* primary — headings, KPI values, active text */
  --t2: #a8bdd6;   /* secondary — column headers, labels, chart ticks */
  --t3: #607a96;   /* muted — hints, subtitles, placeholder */
  --t4: #2a3a52;   /* very muted — decorative, rank numbers */

  /* ── SEMANTIC COLORS ── */
  --ac: #8177F5;   /* accent / interactive / active (purple) */
  --gr: #3CC8B8;   /* positive / gain / live / savings (teal-green) */
  --rd: #E86060;   /* negative / alert / loss (red) */
  --am: #FFCC30;   /* warning / HTB / caution (amber) */
  --pu: #FF9040;   /* warm / moderate / secondary alert (orange) */

  /* ── CATEGORY PALETTE (3-slot) ── */
  --pA: #8177F5;   /* Category A — purple */
  --pB: #3CC8B8;   /* Category B — teal */
  --pC: #FF9040;   /* Category C — orange */

  /* ── TYPOGRAPHY ── */
  --fn: 'Inter', sans-serif;   /* UI text */
  --fm: 'Inter', sans-serif;   /* data / mono-style text */
}
```

---

## Color Reference

### Surfaces

| Token | Hex | Use |
|-------|-----|-----|
| `--bg` | `#07090f` | Page / body background |
| `--s1` | `#0b1018` | Header, sticky bars |
| `--s2` | `#0f1520` | Cards, table backgrounds |
| `--s3` | `#131c2a` | Chips, dropdowns, tooltips |
| `--s4` | `#182236` | Hover fills, pressed state |

### Borders

| Token | Hex | Use |
|-------|-----|-----|
| `--b1` | `#1b2a42` | Default card/table borders |
| `--b2` | `#213352` | Hover borders, table header underline |
| `--b3` | `#2a4068` | Focus rings, strong dividers |

### Text

| Token | Hex | Use |
|-------|-----|-----|
| `--t1` | `#e8eef8` | Primary — values, headings, active labels |
| `--t2` | `#a8bdd6` | Secondary — column headers, chart labels |
| `--t3` | `#607a96` | Muted — hints, timestamps, meta |
| `--t4` | `#2a3a52` | Decorative — background numbers |

### Semantic

| Token | Hex | Meaning |
|-------|-----|---------|
| `--ac` | `#8177F5` | Accent, interactive, selected |
| `--gr` | `#3CC8B8` | Positive, gain, live status |
| `--rd` | `#E86060` | Negative, alert, loss |
| `--am` | `#FFCC30` | Warning, high-risk, caution |
| `--pu` | `#FF9040` | Moderate risk, secondary warning |

### Category Palette (3-item)

| Token | Hex | Swatch |
|-------|-----|--------|
| `--pA` | `#8177F5` | Purple |
| `--pB` | `#3CC8B8` | Teal |
| `--pC` | `#FF9040` | Orange |

For more categories, extend with: `#E86060` (red), `#FFCC30` (amber), `#60A8D6` (sky blue).

---

## Typography Scale

Base: `font-size: 13px; line-height: 1.5; font-family: 'Inter', sans-serif`

| Role | Size | Weight | Color | Notes |
|------|------|--------|-------|-------|
| Brand / App name | 15px | 700 | `#ffffff` | letter-spacing: .04em |
| Section / card title | 9px | 600 | `--t2` | UPPERCASE, ls: .12em |
| KPI label | 9px | 500 | `--t2` | UPPERCASE, ls: .14em |
| KPI value | 20px | 700 | `--t1` | or semantic color |
| KPI subtext | 10px | 400 | `--t2` | margin-top: 4px |
| Nav / tab | 12px | 500 | `--t3` → `--t1` active | |
| Table header | 9px | 600 | `--t2` | UPPERCASE, ls: .08em |
| Table cell | 12px | 400 | `--t2` | |
| Table ticker / key | 10px | 600 | `--t1` | |
| Body / action text | 13px | 400 | `--t1` | line-height: 1.6 |
| Hint / sub text | 11px | 400 | `--t3` | |
| Badge label | 9px | 600 | varies | ls: .04em |
| Export / CTA button | 10px | 600 | `--t2` | UPPERCASE, ls: .08em |

---

## Component Patterns

### Card

```css
.card {
  background: var(--s2);
  border: 1px solid var(--b1);
  border-radius: 8px;
  padding: 14px 16px;
}
```

### Header / Top Bar

```css
#header {
  height: 52px;
  padding: 0 24px;
  background: var(--s1);
  border-bottom: 1px solid var(--b1);
  position: sticky;
  top: 0;
  z-index: 200;
}
```

### Nav Tab

```css
/* default */
.tab { color: var(--t3); border: 1px solid transparent; background: none; border-radius: 4px; padding: 6px 20px; }
/* hover */
.tab:hover { color: var(--t1); border-color: var(--b2); }
/* active */
.tab.active { background: rgba(255,255,255,.08); border-color: #ffffff; color: #ffffff; }
```

### Chip Filter

```css
/* default */
.chip { background: var(--s3); border: 1px solid var(--b1); color: var(--t2); border-radius: 4px; padding: 4px 12px; font-size: 10px; }
/* hover */
.chip:hover { border-color: var(--b2); color: var(--t1); }
/* active */
.chip.active { background: rgba(129,119,245,.1); border-color: var(--ac); color: var(--ac); }
```

### Badge

```css
.badge { border-radius: 3px; padding: 1px 6px; font-size: 9px; font-weight: 600; letter-spacing: .04em; }

/* Danger / HTB   */ background: rgba(255,204,48,.12); color: var(--am); border: 1px solid rgba(255,204,48,.30);
/* Warning / Warm */ background: rgba(255,144,64,.10); color: var(--pu); border: 1px solid rgba(255,144,64,.25);
/* Neutral / GC   */ background: var(--s3);            color: var(--t3); border: 1px solid var(--b1);
/* Accent / pA    */ background: rgba(129,119,245,.10); color: var(--pA); border: 1px solid rgba(129,119,245,.25);
/* Teal / pB      */ background: rgba(60,200,184,.10);  color: var(--pB); border: 1px solid rgba(60,200,184,.25);
/* Orange / pC    */ background: rgba(255,144,64,.10);  color: var(--pC); border: 1px solid rgba(255,144,64,.25);
```

### Table

```css
table { width: 100%; border-collapse: collapse; font-size: 12px; }

thead th {
  position: sticky; top: 0; z-index: 2;
  background: var(--s2);
  font-size: 9px; font-weight: 600; letter-spacing: .08em;
  color: var(--t2); text-transform: uppercase;
  padding: 7px 10px;
  border-bottom: 1px solid var(--b2);
}

td {
  padding: 7px 12px;
  border-bottom: 1px solid rgba(27,42,66,.5);
  color: var(--t2);
}

tr:hover td { background: rgba(40,60,90,0.6); }
tbody tr:nth-child(even) td { background: rgba(255,255,255,0.018); }
```

### Dropdown / Select

```css
select {
  font-size: 12px; padding: 5px 10px; border-radius: 4px;
  background: var(--s3); border: 1px solid var(--b1); color: var(--t1); outline: none;
}
select:focus { border-color: var(--b2); }
```

### Ghost / Outline Button

```css
.btn-outline {
  font-size: 10px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase;
  padding: 5px 14px; border-radius: 4px;
  background: transparent; border: 1px solid var(--b2); color: var(--t2);
  transition: all .15s; cursor: pointer;
}
.btn-outline:hover { border-color: var(--t1); color: var(--t1); }
.btn-outline.success { border-color: var(--gr); color: var(--gr); }
.btn-outline.success:hover { background: rgba(60,200,184,.08); }
```

### Live Status Dot

```css
.live-dot {
  width: 7px; height: 7px; border-radius: 50%;
  background: var(--gr); box-shadow: 0 0 6px var(--gr);
}
```

### Tooltip

```css
.tooltip {
  position: fixed; z-index: 9999;
  background: var(--s3); border: 1px solid var(--b2); border-radius: 4px;
  padding: 8px 11px; font-size: 10px; line-height: 2;
  box-shadow: 0 4px 14px rgba(0,0,0,.5); pointer-events: none;
}
```

### Scrollbar

```css
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--s2); }
::-webkit-scrollbar-thumb { background: #a8bdd6; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #e8eef8; }
::-webkit-scrollbar-corner { background: var(--s2); }
```

---

## Grid / Layout

```css
/* Content area */
#main { padding: 16px 24px; max-width: 1680px; margin: 0 auto; }

/* Rows */
.row { display: grid; gap: 12px; margin-bottom: 14px; }

/* Column presets */
.c2 { grid-template-columns: 1fr 1fr; }
.c3 { grid-template-columns: 1fr 1fr 1fr; }
.c4 { grid-template-columns: 1fr 1fr 1fr 1fr; }
.c5 { grid-template-columns: repeat(5, 1fr); }
```

**Spacing rhythm:** `4 → 6 → 8 → 10 → 12 → 14 → 16 → 20 → 24px`

---

## Chart.js Defaults

```js
const CHART_DEFAULTS = {
  font:        { family: 'Inter' },
  tickColor:   '#a8bdd6',
  gridColor:   'rgba(27,42,66,0.6)',
  tooltipBg:   '#131c2a',
  tooltipBorder: '#213352',
  tooltipTitle: '#e8eef8',
  tooltipBody:  '#a8bdd6',
  legendColor:  '#a8bdd6',
  tickSize:     9,
  legendSize:   10,
};

// Axis tick style
ticks: { font: { family: 'Inter', size: 9 }, color: '#a8bdd6' }

// Grid lines
grid: { color: 'rgba(27,42,66,0.6)', drawBorder: false }

// Tooltip
plugins.tooltip: {
  backgroundColor: '#131c2a',
  borderColor: '#213352',
  borderWidth: 1,
  titleColor: '#e8eef8',
  bodyColor: '#a8bdd6',
}
```

**Standard chart color order (for sequential datasets):**
`#8177F5` → `#3CC8B8` → `#FF9040` → `#E86060` → `#FFCC30`

---

## Do / Don't

| Do | Don't |
|----|-------|
| Use `--s2` for cards, `--s1` for headers | Use pure black (`#000`) as background |
| Use border `1px solid var(--b1)` for all cards | Use box-shadows for depth — use borders |
| Use `--gr` (teal) for positive/savings values | Use green (`#22c55e`) — too saturated |
| Use `--rd` for negative values | Use pure red (`#ff0000`) |
| Keep font size at 9–13px for dense data UI | Use large type in data-dense contexts |
| Use uppercase + letter-spacing for section titles | Use lowercase section labels |
| Use `border-radius: 8px` for cards, `4px` for chips/buttons | Mix border-radius values arbitrarily |
