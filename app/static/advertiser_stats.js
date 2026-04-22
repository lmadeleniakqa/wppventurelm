/**
 * Advertiser-Agency Relationship Statistics
 * Cross-referenced with COMvergence c-dash CARD (40,592 assignments)
 * Last updated: April 2026
 */

const ADVERTISER_STATS = {
  metadata: {
    generated: "2026-04-22",
    total_advertisers: 100,
    total_spend_B: 194.5,
    account_moves_tracked: 3110,
    comvergence_source: "c-dash CARD 40,592 assignments",
    time_span: "2016-Q1 to 2026-Q2"
  },

  // How long brands stay before switching
  relationship_duration: {
    mean_years: 4.2,
    median_years: 3.3,
    min_years: 0.1,
    max_years: 10.1,
    note: "Median tenure ~3.3 years before a brand reviews or switches"
  },

  // Net wins/losses by holding group (COMvergence-verified, Big 3 flows only, 2016-2026)
  churn_by_group: {
    WPP:      { wins: 773,  losses: 1544, net: -771,  spend_won_M: 11942,  spend_lost_M: 25449, net_spend_M: -13507 },
    Publicis: { wins: 1179, losses: 544,  net: 635,   spend_won_M: 23648,  spend_lost_M: 9100,  net_spend_M: 14548 },
    Omnicom:  { wins: 1156, losses: 791,  net: 365,   spend_won_M: 15251,  spend_lost_M: 13449, net_spend_M: 1802 },
    Dentsu:   { wins: 0,  losses: 2,  net: -2,  spend_won_M: 0,     spend_lost_M: 1900,  net_spend_M: -1900 },
    IPG:      { wins: 0,  losses: 1,  net: -1,  spend_won_M: 0,     spend_lost_M: 2000,  net_spend_M: -2000 },
    Havas:    { wins: 0,  losses: 1,  net: -1,  spend_won_M: 0,     spend_lost_M: 900,   net_spend_M: -900 }
  },

  // Current spend share — COMvergence 2025 portfolio ($288B total tracked)
  spend_share: {
    Omnicom:   { spend_M: 74315, pct: 25.8 },
    Publicis:  { spend_M: 67588, pct: 23.5 },
    WPP:       { spend_M: 62585, pct: 21.7 },
    Independents: { spend_M: 41056, pct: 14.2 },
    Dentsu:    { spend_M: 30567, pct: 10.6 },
    Havas:     { spend_M: 12075, pct: 4.2 }
  },

  // Most common direction of account moves (COMvergence Big 3 flows)
  movement_flows: [
    { from: "WPP",      to: "Publicis", count: 670, spend_M: 14637 },
    { from: "WPP",      to: "Omnicom",  count: 874, spend_M: 10813 },
    { from: "Omnicom",  to: "Publicis", count: 509, spend_M: 9011 },
    { from: "Omnicom",  to: "WPP",      count: 511, spend_M: 7281 },
    { from: "Publicis", to: "WPP",      count: 262, spend_M: 4661 },
    { from: "Publicis", to: "Omnicom",  count: 283, spend_M: 4438 }
  ],

  // Avg time since last agency review by holding group
  review_freshness: {
    Publicis: { clients: 33, avg_years_since_review: 3.2 },
    WPP:      { clients: 19, avg_years_since_review: 3.1 },
    Omnicom:  { clients: 16, avg_years_since_review: 3.8 },
    Dentsu:   { clients: 12, avg_years_since_review: 3.5 },
    Mixed:    { clients: 15, avg_years_since_review: 2.8 },
    Havas:    { clients: 3,  avg_years_since_review: 3.7 }
  },

  // Review year distribution
  review_distribution: {
    2016: 1, 2021: 6, 2022: 30, 2023: 43, 2024: 14, 2025: 3, 2026: 2,
    stale_pre_2023_pct: 37.0,
    note: "37% of advertisers haven't reviewed since before 2023"
  },

  // Account move economics (COMvergence Big 3 flows)
  move_economics: {
    avg_spend_per_move_M: 16.4,
    median_spend_per_move_M: 3.5,
    total_spend_moved_M: 50841,
    total_moves: 3110,
    note: "COMvergence tracks all moves including local market-level assignments"
  },

  // Annual pace of moves — 2x acceleration post-2021
  yearly_pace: {
    pre_2021_avg: 1.8,
    post_2021_avg: 3.7,
    by_year: {
      2016: { moves: 3, spend_M: 3800 },
      2017: { moves: 2, spend_M: 2400 },
      2018: { moves: 1, spend_M: 1200 },
      2019: { moves: 1, spend_M: 1500 },
      2020: { moves: 2, spend_M: 3200 },
      2021: { moves: 4, spend_M: 2300 },
      2022: { moves: 2, spend_M: 3000 },
      2023: { moves: 7, spend_M: 11300 },
      2024: { moves: 5, spend_M: 4800 },
      2025: { moves: 1, spend_M: 1700 },
      2026: { moves: 3, spend_M: 2500 }
    },
    peak_year: 2023,
    peak_year_note: "7 moves worth $11.3B — most active year. 2023-2024 combined = 39% of all moves."
  },

  // Seasonality: when account moves get announced
  seasonality: {
    by_quarter: {
      Q1: { moves: 6, pct: 19.4, spend_M: 7100, spend_pct: 18.8 },
      Q2: { moves: 13, pct: 41.9, spend_M: 19100, spend_pct: 50.7 },
      Q3: { moves: 7, pct: 22.6, spend_M: 8000, spend_pct: 21.2 },
      Q4: { moves: 5, pct: 16.1, spend_M: 3500, spend_pct: 9.3 }
    },
    by_month: {
      Jan: 2, Feb: 0, Mar: 4, Apr: 5, May: 1, Jun: 7,
      Jul: 1, Aug: 2, Sep: 4, Oct: 3, Nov: 1, Dec: 1
    },
    by_half: {
      H1: { moves: 19, spend_M: 26200 },
      H2: { moves: 12, spend_M: 11500 }
    },
    peak_month: "June (7 moves, 22.6%)",
    peak_quarter: "Q2 (13 moves, 41.9% — half of all spend moved)",
    note: "Reviews kick off in Q1 after budget cycles, decisions land in Q2. H1 accounts for 61% of moves and 69% of spend.",

    // Per-group win/loss timing
    wpp_timing: {
      wins_by_q:   { Q1: 1, Q2: 3, Q3: 2, Q4: 1 },
      losses_by_q: { Q1: 2, Q2: 6, Q3: 4, Q4: 3 },
      note: "WPP loses most in Q2 (6 of 15 losses) — same quarter Publicis wins most"
    },
    publicis_timing: {
      wins_by_q:   { Q1: 3, Q2: 6, Q3: 3, Q4: 3 },
      losses_by_q: { Q1: 2, Q2: 3, Q3: 1, Q4: 1 },
      note: "Publicis wins peak in Q2 (6 of 15 wins) — post-review decision cycle"
    },
    omnicom_timing: {
      wins_by_q:   { Q1: 2, Q2: 4, Q3: 2, Q4: 1 },
      losses_by_q: { Q1: 0, Q2: 0, Q3: 1, Q4: 1 },
      note: "Omnicom never loses in H1 — defensively strongest in pitch season"
    }
  },

  // Key headlines
  headlines: {
    avg_tenure: "3.3 years median before a brand switches agencies",
    wpp_net_loss: "WPP net -$8.1B in spend over 10 years (15 losses vs 7 wins)",
    publicis_dominance: "Publicis controls 31.3% of top-100 spend, up from ~20% in 2016",
    wpp_to_publicis: "35% of all account moves flow WPP → Publicis ($11.6B)",
    acceleration: "Account move pace doubled: 1.8/yr pre-2021 → 3.7/yr post-2021",
    stale_accounts: "37% of top 100 haven't reviewed since before 2023 — ripe for pitches",
    omnicom_efficient: "Omnicom: +$11.4B net spend from just 9 wins and only 2 losses",
    dentsu_hemorrhaging: "Dentsu: 0 wins, 2 losses — lost AstraZeneca and Microsoft",
    q2_is_decision_season: "Q2 is decision season: 42% of moves and 51% of spend ($19.1B) land Apr-Jun",
    june_peak: "June is the single busiest month (7 of 31 moves, 22.6%)",
    h1_dominance: "H1 accounts for 61% of moves and 69% of spend moved",
    omnicom_never_loses_h1: "Omnicom has never lost an account in H1 over 10 years"
  }
};
