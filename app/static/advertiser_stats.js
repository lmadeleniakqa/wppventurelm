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

  // How long brands stay before switching — COMvergence verified
  relationship_duration: {
    mean_years: 5.4,
    median_years: 3.8,
    min_years: 0.1,
    max_years: 28.1,
    note: "Median tenure ~3.8 years before a brand reviews or switches (COMvergence)"
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

  // Annual pace of global move events ($50M+, 28-day clustering) — COMvergence
  yearly_pace: {
    pre_2021_avg: 22,
    post_2021_avg: 56,
    by_year: {
      2016: { moves: 745 },
      2017: { moves: 827 },
      2018: { moves: 1254 },
      2019: { moves: 1607 },
      2020: { moves: 2157 },
      2021: { moves: 2981 },
      2022: { moves: 3450 },
      2023: { moves: 3702 },
      2024: { moves: 3580 },
      2025: { moves: 3794 },
      2026: { moves: 476 }
    },
    peak_year: 2025,
    peak_year_note: "COMvergence: 76 global move events ($50M+) in 2025 — highest on record. 2.5x vs pre-2021."
  },

  // Seasonality: when account moves get announced (COMvergence, 27,025 moves with quarter data)
  seasonality: {
    by_quarter: {
      Q1: { moves: 9172, pct: 33.9 },
      Q2: { moves: 6716, pct: 24.9 },
      Q3: { moves: 5035, pct: 18.6 },
      Q4: { moves: 6102, pct: 22.6 }
    },
    by_half: {
      H1: { moves: 15888 },
      H2: { moves: 11137 }
    },
    peak_quarter: "Q1 (33.9% of all moves — contract renewals and new budget cycles)",
    note: "COMvergence: Q1 is busiest for all moves (budget cycle start). H1 accounts for 59% of competitive moves."
  },

  // Key headlines — COMvergence verified
  headlines: {
    avg_tenure: "3.8 years median before a brand switches agencies (COMvergence)",
    wpp_net_loss: "WPP net -$13.5B in spend vs Big 3 (lost $25.5B, won $12.0B)",
    publicis_dominance: "Publicis: $67.6B in managed spend, 23.5% market share",
    wpp_to_publicis: "WPP → Publicis: 670 accounts, $14.7B — largest flow between any two holdings",
    acceleration: "Global move events 2.5x: ~22/yr pre-2021 → ~56/yr post-2021 ($50M+ threshold)",
    omnicom_leads: "Omnicom: $74.3B managed spend, 8,777 assignments — largest portfolio",
    publicis_26q_streak: "Publicis: 26 consecutive net-win quarters (2020-Q1 through 2026-Q2)",
    q1_is_busiest: "Q1 is busiest quarter: 33.9% of all competitive moves (budget cycle start)",
    h1_dominance: "H1 accounts for 59% of competitive moves"
  }
};
