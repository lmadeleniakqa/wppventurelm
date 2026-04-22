/**
 * Pitch Prediction Model — Heuristic v2 (COMvergence-calibrated)
 * Scores top 100 global advertisers on likelihood of media agency review
 * over a 24-month forward window (Q2 2026 – Q1 2028).
 *
 * Seven signals: tenure staleness, recently-reviewed dampener, sector churn
 * frequency, spend magnitude, holding-group vulnerability, industry-event
 * catalysts, and seasonality.
 *
 * SECTOR_FREQ, GROUP_VULN, and FLOW matrices are now derived from
 * COMvergence c-dash CARD (40,592 assignments) rather than estimates.
 *
 * Depends on: ADVERTISERS (advertisers_data.js — generated from COMvergence)
 */

(function () {
  "use strict";

  var NOW = 2026.33; // April 2026
  var MEDIAN_TENURE = 3.8; // COMvergence-derived
  var QUARTERS = [
    "2026-Q2","2026-Q3","2026-Q4",
    "2027-Q1","2027-Q2","2027-Q3","2027-Q4",
    "2028-Q1"
  ];

  // Quarter start months (0-indexed from Jan 2026 = 0)
  var Q_START_MONTH = {
    "2026-Q2": 3, "2026-Q3": 6, "2026-Q4": 9,
    "2027-Q1": 12, "2027-Q2": 15, "2027-Q3": 18, "2027-Q4": 21,
    "2028-Q1": 24
  };

  // Seasonality: normalized so they center around 1.0 (sum = 4.0)
  // COMvergence: Q1=33.9%, Q2=24.9%, Q3=18.6%, Q4=22.6% → normalized to multiplier
  var Q_SEASON = { Q1: 1.36, Q2: 1.00, Q3: 0.74, Q4: 0.90 };

  // Sector churn frequency — COMvergence-derived from 40,592 assignments
  // Rate = proportion of assignments that were competitive moves (Agency/New-assignment)
  var SECTOR_FREQ = {
    "FMCG": 0.74, "FMCG/Beverages": 0.74, "FMCG/Beauty": 0.71,
    "Auto": 0.70,
    "Tech": 0.65, "Tech/Retail": 0.65, "Tech/Electronics": 0.65,
    "Tech/Entertainment": 0.65, "Tech/Travel": 0.65,
    "Pharma": 0.68, "Pharma/FMCG": 0.71,
    "Finance": 0.73,
    "Telecom": 0.70,
    "Media/Entertainment": 0.69, "Media/Telecom": 0.70,
    "Luxury": 0.74, "Beauty/Luxury": 0.74,
    "Energy": 0.60,
    "QSR": 0.79, "QSR/Retail": 0.79,
    "Retail": 0.71, "Retail/Apparel": 0.78,
    "Apparel/Sportswear": 0.78,
    "Travel": 0.69,
    "Other": 0.73
  };

  // Holding group vulnerability — COMvergence loss rate (accounts lost / total held)
  // WPP has highest loss rate at 36%, Publicis lowest at 24%
  var GROUP_VULN = {
    "WPP":      0.36,   // COMvergence: lost 36% of assignments to competitors
    "Dentsu":   0.34,   // COMvergence: 34% loss rate
    "IPG":      0.65,   // absorbed into Omnicom, clients may review (not in COMvergence)
    "Havas":    0.29,   // COMvergence: 29% loss rate — sticky base
    "Mixed":    0.25,   // split accounts are diversified, less likely to full-pitch
    "Publicis": 0.24,   // COMvergence: 24% loss rate — strongest defender
    "Omnicom":  0.27,   // COMvergence: 27% loss rate
    "In-house": 0.15,
    "Other":    0.30
  };

  // Flow transition matrix — COMvergence: where lost accounts actually go
  // Based on 28,798 competitive moves in the dataset
  var FLOW = {
    "WPP":      [{ to: "Omnicom", p: 0.33 }, { to: "Publicis", p: 0.25 }, { to: "Dentsu", p: 0.12 }],
    "Publicis": [{ to: "Omnicom", p: 0.25 }, { to: "WPP", p: 0.23 },      { to: "Dentsu", p: 0.16 }],
    "Dentsu":   [{ to: "Omnicom", p: 0.33 }, { to: "WPP", p: 0.24 },      { to: "Publicis", p: 0.17 }],
    "Omnicom":  [{ to: "Publicis", p: 0.21 },{ to: "WPP", p: 0.21 },      { to: "Dentsu", p: 0.11 }],
    "Havas":    [{ to: "Omnicom", p: 0.29 }, { to: "Publicis", p: 0.19 },  { to: "WPP", p: 0.15 }],
    "IPG":      [{ to: "Omnicom", p: 0.30 }, { to: "Publicis", p: 0.25 },  { to: "WPP", p: 0.20 }],
    "Mixed":    [{ to: "Omnicom", p: 0.30 }, { to: "Publicis", p: 0.25 },  { to: "WPP", p: 0.22 },{ to: "Dentsu", p: 0.12 }],
    "In-house": [{ to: "Omnicom", p: 0.30 }, { to: "Publicis", p: 0.30 },  { to: "WPP", p: 0.25 }],
    "Other":    [{ to: "Omnicom", p: 0.30 }, { to: "Publicis", p: 0.30 },  { to: "WPP", p: 0.25 }]
  };

  // Typical review durations (months) by spend tier
  function reviewDuration(spendM) {
    if (spendM >= 3000) return { rfi: 2, pitch: 3, transition: 3, total: 8 }; // mega account
    if (spendM >= 1000) return { rfi: 1, pitch: 3, transition: 2, total: 6 }; // large
    if (spendM >= 500)  return { rfi: 1, pitch: 2, transition: 2, total: 5 }; // mid
    return { rfi: 1, pitch: 2, transition: 1, total: 4 }; // smaller
  }

  // Weights — rebalanced
  var W = {
    staleness: 0.35,
    recency_dampener: 0.15,  // negative signal for recently reviewed
    sector: 0.10,
    spend: 0.05,
    vulnerability: 0.20,
    catalyst: 0.10,
    seasonality: 0.05
  };

  function sigmoid(x, center, steepness) {
    return 1 / (1 + Math.exp(-steepness * (x - center)));
  }

  function computeScores(a) {
    var yr = a.last_review != null ? a.last_review : 2020;
    var yearsSince = Math.min(NOW - yr, 8);

    // Staleness: S-curve centered at median tenure
    var staleness = sigmoid(yearsSince, MEDIAN_TENURE, 1.8);

    // Recency dampener: recently reviewed accounts are very unlikely to pitch again
    // 2026 review → strong dampener, 2025 → moderate, 2024 → mild, older → none
    var recencyDamp = 0; // 0 = no dampening, 1 = max dampening
    if (yr >= 2026) recencyDamp = 1.0;
    else if (yr >= 2025.5) recencyDamp = 0.8;
    else if (yr >= 2025) recencyDamp = 0.5;
    else if (yr >= 2024.5) recencyDamp = 0.2;

    var sector = SECTOR_FREQ[a.sector] || 0.40;
    var spend = Math.min(1.0, Math.log10(a.spend) / Math.log10(8200));
    var vulnerability = GROUP_VULN[a.group] || 0.30;

    // Catalyst: Omnicom-IPG merger (only if NOT already reviewed post-merger)
    var catalyst = 0;
    var agLower = (a.agency || "").toLowerCase();
    if (a.group === "Omnicom" && yr < 2025) {
      catalyst += 0.25;
      // Legacy IPG clients get extra catalyst
      if (agLower.indexOf("um ") >= 0 || agLower.indexOf("initiative") >= 0 ||
          agLower.indexOf("mediahub") >= 0 || agLower.indexOf("legacy-ipg") >= 0) {
        catalyst += 0.25;
      }
    }
    // WPP Elevate28: only if not already reviewed post-restructure
    if (a.group === "WPP" && yr < 2026) catalyst += 0.20;
    // IPG clients still transitioning
    if (a.group === "IPG" && yr < 2025) catalyst += 0.35;

    catalyst = Math.min(catalyst, 1.0);

    return {
      staleness: staleness,
      recencyDamp: recencyDamp,
      sector: sector,
      spend: spend,
      vulnerability: vulnerability,
      catalyst: catalyst,
      yearsSince: yearsSince,
      reviewYear: yr
    };
  }

  function compositeScore(scores, seasonMult) {
    // Positive signals
    var pos = W.staleness * scores.staleness +
              W.sector * scores.sector +
              W.spend * scores.spend +
              W.vulnerability * scores.vulnerability +
              W.catalyst * scores.catalyst +
              W.seasonality * Math.min(1.0, seasonMult / 1.68); // normalize to 0-1

    // Apply recency dampener as a multiplier (not additive)
    var dampened = pos * (1 - scores.recencyDamp * 0.85);

    return Math.round(Math.min(100, Math.max(0, dampened * 115)));
    // 115 scalar so high-risk accounts can reach 85-95
  }

  function riskBracket(score) {
    if (score >= 75) return "3mo";
    if (score >= 55) return "6mo";
    if (score >= 35) return "9mo";
    return "low";
  }

  function wppStance(a, score) {
    if (score < 30) return "neutral";
    if (a.group === "WPP") return "at_risk";
    return "opportunity";
  }

  // Estimate when a review would start and decision would land
  // based on score peak quarter and seasonality
  function estimateTimeline(pred, scores) {
    var dur = reviewDuration(pred.spend);
    var peakQ = pred.peak_quarter;
    var peakMonth = Q_START_MONTH[peakQ] || 15; // months from Jan 2026

    // Decision likely lands in peak quarter (when seasonality is highest)
    // Review starts dur.rfi + dur.pitch months before decision
    var decisionMonth = peakMonth + 1; // mid-quarter
    var reviewStartMonth = Math.max(0, decisionMonth - dur.rfi - dur.pitch);
    var transitionEndMonth = decisionMonth + dur.transition;

    // Convert to dates
    function monthToDate(m) {
      var yr = 2026 + Math.floor(m / 12);
      var mo = (m % 12) + 1;
      return yr + "-" + (mo < 10 ? "0" : "") + mo;
    }

    return {
      review_start: monthToDate(reviewStartMonth),
      decision: monthToDate(decisionMonth),
      transition_end: monthToDate(transitionEndMonth),
      duration_months: dur.total,
      review_start_offset: reviewStartMonth,  // months from Jan 2026
      decision_offset: decisionMonth,
      transition_end_offset: transitionEndMonth
    };
  }

  // --- Build predictions ---------------------------------------------------

  var predictions = [];
  var timeline = {};
  QUARTERS.forEach(function (q) { timeline[q] = []; });

  if (typeof ADVERTISERS !== "undefined") {
    ADVERTISERS.forEach(function (a) {
      var scores = computeScores(a);

      // Per-quarter risk
      var quarterly = {};
      var peakQ = QUARTERS[0], peakScore = 0;
      QUARTERS.forEach(function (q) {
        var qLabel = q.split("-")[1];
        var sMult = Q_SEASON[qLabel] || 1.0;
        var qIdx = QUARTERS.indexOf(q);
        // Staleness ages as quarters advance
        var adjScores = Object.assign({}, scores, {
          staleness: Math.min(1.0, scores.staleness + qIdx * 0.02),
          // Recency dampener fades over time
          recencyDamp: Math.max(0, scores.recencyDamp - qIdx * 0.1)
        });
        var sc = compositeScore(adjScores, sMult);
        quarterly[q] = sc;
        if (sc > peakScore) { peakScore = sc; peakQ = q; }
      });

      var nearestScore = quarterly[QUARTERS[0]];
      var bracket = riskBracket(nearestScore);

      var flowArr = FLOW[a.group] || FLOW["Mixed"];
      var destinations = flowArr.map(function (f) {
        return { group: f.to, probability: f.p };
      });

      var pred = {
        name: a.name,
        spend: a.spend,
        group: a.group,
        agency: a.agency,
        sector: a.sector,
        last_review: a.last_review,
        years_since_review: Math.round((NOW - (a.last_review || 2020)) * 10) / 10,
        scores: {
          staleness: Math.round(scores.staleness * 100) / 100,
          recencyDamp: Math.round(scores.recencyDamp * 100) / 100,
          sector: scores.sector,
          spend: Math.round(scores.spend * 100) / 100,
          vulnerability: scores.vulnerability,
          catalyst: Math.round(scores.catalyst * 100) / 100
        },
        composite_score: nearestScore,
        risk_bracket: bracket,
        quarterly_risk: quarterly,
        peak_quarter: peakQ,
        peak_score: peakScore,
        likely_destination: destinations,
        wpp_stance: wppStance(a, nearestScore)
      };

      // Add competition timeline for accounts with real risk
      if (peakScore >= 35) {
        pred.competition_timeline = estimateTimeline(pred, scores);
      }

      predictions.push(pred);

      QUARTERS.forEach(function (q) {
        if (quarterly[q] >= 35) {
          timeline[q].push({ name: a.name, score: quarterly[q], group: a.group, spend: a.spend });
        }
      });
    });

    predictions.sort(function (a, b) { return b.composite_score - a.composite_score; });

    Object.keys(timeline).forEach(function (q) {
      timeline[q].sort(function (a, b) { return b.score - a.score; });
    });
  }

  // --- Summary aggregates --------------------------------------------------

  var groupSummary = {};
  ["WPP", "Publicis", "Omnicom", "Dentsu", "Mixed", "Havas", "IPG"].forEach(function (g) {
    var atRisk = predictions.filter(function (p) { return p.group === g && p.composite_score >= 55; });
    var opportunities = predictions.filter(function (p) {
      return p.group !== g && p.composite_score >= 55;
    });
    groupSummary[g] = {
      at_risk_count: atRisk.length,
      at_risk_spend_M: atRisk.reduce(function (s, p) { return s + p.spend; }, 0),
      opportunity_count: opportunities.length,
      opportunity_spend_M: opportunities.reduce(function (s, p) { return s + p.spend; }, 0)
    };
  });

  // --- Competition calendar: sorted by estimated decision date -------------

  var competitionCalendar = predictions
    .filter(function (p) { return p.competition_timeline && p.composite_score >= 35; })
    .sort(function (a, b) {
      return a.competition_timeline.decision_offset - b.competition_timeline.decision_offset;
    })
    .map(function (p) {
      return {
        name: p.name,
        spend: p.spend,
        group: p.group,
        score: p.composite_score,
        bracket: p.risk_bracket,
        review_start: p.competition_timeline.review_start,
        decision: p.competition_timeline.decision,
        transition_end: p.competition_timeline.transition_end,
        duration_months: p.competition_timeline.duration_months,
        review_start_offset: p.competition_timeline.review_start_offset,
        decision_offset: p.competition_timeline.decision_offset,
        transition_end_offset: p.competition_timeline.transition_end_offset,
        likely_destination: p.likely_destination[0]
      };
    });

  // --- Export ---------------------------------------------------------------

  window.PITCH_PREDICTIONS = {
    metadata: {
      generated: "2026-04-21",
      model_version: "heuristic_v2",
      time_horizon_months: 24,
      total_advertisers: predictions.length,
      quarters: QUARTERS,
      weights: W
    },
    predictions: predictions,
    timeline: timeline,
    group_summary: groupSummary,
    competition_calendar: competitionCalendar,

    getAtRisk: function (group) {
      return predictions.filter(function (p) { return p.group === group && p.composite_score >= 55; });
    },
    getOpportunities: function (group) {
      return predictions.filter(function (p) { return p.group !== group && p.composite_score >= 55; });
    },
    getBracket: function (bracket) {
      return predictions.filter(function (p) { return p.risk_bracket === bracket; });
    }
  };
})();
