/**
 * Comprehensive Agency Knowledge Base for Major Advertising Holding Groups
 * Last updated: April 2026
 *
 * Covers: WPP, Publicis Groupe, Omnicom Group
 * Includes mergers, acquisitions, and restructurings from 2021-2026.
 *
 * Sources:
 * - WPP Elevate28 strategy (Feb 2026)
 * - Omnicom-IPG merger completion (Dec 2025) and restructuring (Feb 2026)
 * - Publicis Groupe official agency listings (2025)
 * - Industry trade publications (Ad Age, Adweek, Campaign, The Drum)
 */

const HOLDING_GROUPS_AGENCIES = {
  // =========================================================================
  // WPP plc (LON: WPP)
  // =========================================================================
  wpp: {
    name: "WPP plc",
    ticker: "WPP",
    headquarters: "London, United Kingdom",
    ceo: "Mark Read",
    // Elevate28 strategy announced Feb 2026: reorganizing into four operating
    // divisions — WPP Creative, WPP Media, WPP Production, WPP Enterprise Solutions
    restructureNote:
      "In Feb 2026, WPP announced 'Elevate28' — a shift from holding company to single integrated company with four operating divisions. Targets £500M in annualized cost savings by 2028.",

    divisions: {
      // ----- WPP MEDIA (formerly GroupM) -----
      wppMedia: {
        name: "WPP Media",
        formerName: "GroupM",
        rebrandedDate: "May 2025",
        description:
          "World's largest media investment company. Rebranded from GroupM to WPP Media in May 2025.",
        agencies: [
          {
            name: "Mindshare",
            type: "media",
            description: "Global media agency network — media planning and buying",
          },
          {
            name: "Wavemaker",
            type: "media",
            description: "Global media agency — purchase journey media planning",
          },
          {
            name: "EssenceMediacom",
            type: "media",
            mergerNote:
              "Formed in 2023 from merger of Essence (digital/performance) and MediaCom (full-service media). Part of GroupM consolidation from 5 agencies to 3.",
            description: "Performance and data-driven media agency",
          },
        ],
        platforms: [
          { name: "Xaxis", description: "Programmatic media platform" },
          { name: "Finecast", description: "Addressable TV platform" },
          { name: "Choreograph", description: "Data products company" },
        ],
      },

      // ----- WPP CREATIVE -----
      wppCreative: {
        name: "WPP Creative",
        announcedDate: "February 2026",
        leader: "Jon Cook (CEO, formerly VML CEO)",
        description:
          "Unifies WPP's creative, PR, and design agencies under a single operating model. Preserves distinct agency cultures while implementing shared operating systems.",
        agencies: [
          {
            name: "Ogilvy",
            type: "creative",
            description:
              "Global creative and advertising agency. One of the most iconic agency brands in advertising history.",
            note: "Grey was merged into Ogilvy in May 2025.",
          },
          {
            name: "VML",
            type: "creative",
            mergerNote:
              "Formed in Oct 2023 from merger of Wunderman Thompson and VMLY&R. The largest creative agency in the world by headcount at the time of merger.",
            description:
              "Global creative agency spanning brand experience, customer experience, and commerce.",
            predecessors: [
              "Wunderman Thompson (itself a 2018 merger of Wunderman and J. Walter Thompson)",
              "VMLY&R (itself a 2018 merger of VML and Y&R)",
            ],
          },
          {
            name: "AKQA",
            type: "creative",
            description:
              "Innovation and design agency specializing in digital experiences.",
          },
          {
            name: "Burson",
            type: "pr",
            formerName: "Burson Cohn & Wolfe (BCW)",
            rebrandedDate: "2024",
            description:
              "Global communications/PR agency. Rebranded from BCW to Burson in 2024.",
            note: "BCW was itself formed in 2018 from merger of Burson-Marsteller and Cohn & Wolfe.",
          },
          {
            name: "Hill & Knowlton",
            type: "pr",
            formerName: "Hill+Knowlton Strategies",
            description: "Global public relations and public affairs firm.",
          },
          {
            name: "Landor",
            type: "brand_consulting",
            description:
              "Brand consulting and design agency. Formerly Landor & Fitch.",
          },
          {
            name: "Superunion",
            type: "brand_consulting",
            description:
              "Brand consultancy formed from Brand Union, Lambie-Nairn, and other WPP brand firms.",
          },
        ],
        retiredBrands: [
          {
            name: "Grey",
            retiredDate: "May 2025",
            mergedInto: "Ogilvy",
            note: "Grey was folded under Ogilvy, ending its nearly century-long independent existence.",
          },
        ],
      },

      // ----- WPP PRODUCTION -----
      wppProduction: {
        name: "WPP Production",
        announcedDate: "January 2026",
        description:
          "Consolidation of WPP's production capabilities into a single unit.",
        agencies: [
          {
            name: "Hogarth",
            type: "production",
            description:
              "Global content production company — creative production, adaptation, and content management at scale.",
          },
        ],
      },

      // ----- WPP ENTERPRISE SOLUTIONS -----
      wppEnterpriseSolutions: {
        name: "WPP Enterprise Solutions",
        description:
          "Brings together customer experience, commerce, CRM, content transformation, and technology/data capabilities.",
        note: "Houses WPP's technology consulting and data-driven transformation services.",
      },

      // ----- WPP HEALTH (Legacy / being integrated) -----
      wppHealth: {
        name: "WPP Health & Wellness",
        description:
          "Specialist healthcare and pharma marketing division. Being integrated into the Elevate28 structure.",
        agencies: [
          {
            name: "Ogilvy Health",
            type: "health",
            description:
              "Healthcare marketing and communications (formerly Ogilvy CommonHealth Worldwide).",
          },
          {
            name: "Sudler & Hennessey",
            type: "health",
            description: "Healthcare advertising and communications.",
          },
          {
            name: "CMI/Compas",
            type: "health_media",
            description:
              "Healthcare/pharma media planning and buying specialist.",
          },
          {
            name: "GHG (Grey Health Group)",
            type: "health",
            description: "Healthcare communications network.",
          },
        ],
      },
    },

    // Key mergers & acquisitions timeline 2021-2026
    mergerTimeline: [
      {
        year: 2023,
        event: "Essence + MediaCom merged into EssenceMediacom under GroupM",
      },
      {
        year: 2023,
        event:
          "Wunderman Thompson + VMLY&R merged into VML — the largest creative agency in the world",
      },
      {
        year: 2024,
        event: "BCW (Burson Cohn & Wolfe) rebranded to Burson",
      },
      {
        year: 2025,
        month: "May",
        event: "GroupM rebranded as WPP Media",
      },
      {
        year: 2025,
        month: "May",
        event: "Grey merged into Ogilvy",
      },
      {
        year: 2026,
        month: "January",
        event: "WPP Production launched (consolidating Hogarth and other production assets)",
      },
      {
        year: 2026,
        month: "February",
        event:
          "Elevate28 announced — WPP Creative formed, unifying Ogilvy, VML, AKQA, Burson, H&K, and Landor",
      },
    ],

    // Note: WPP sold majority of Kantar to Bain Capital in 2019; retains minority stake
    formerAgencies: [
      {
        name: "Kantar",
        note: "Research & insights company. WPP sold 60% to Bain Capital in 2019; retains minority stake.",
      },
      {
        name: "Grey",
        note: "Merged into Ogilvy (May 2025).",
      },
      {
        name: "J. Walter Thompson (JWT)",
        note: "Merged with Wunderman to form Wunderman Thompson (2018), then merged into VML (2023).",
      },
      {
        name: "Y&R (Young & Rubicam)",
        note: "Merged with VML to form VMLY&R (2018), then merged into VML (2023).",
      },
    ],
  },

  // =========================================================================
  // PUBLICIS GROUPE (EPA: PUB)
  // =========================================================================
  publicis: {
    name: "Publicis Groupe",
    ticker: "PUB",
    headquarters: "Paris, France",
    ceo: "Arthur Sadoun",
    structureNote:
      "Organized around the 'Power of One' model with shared platforms (Marcel, Publicis People Cloud) and Epsilon data backbone. Four Solutions Hubs: Publicis Communications, Publicis Media, Publicis Sapient, Publicis Health.",

    divisions: {
      // ----- PUBLICIS COMMUNICATIONS -----
      publicisComms: {
        name: "Publicis Communications",
        description:
          "Creative and communications hub housing Publicis Groupe's creative agencies.",
        agencies: [
          {
            name: "Publicis Worldwide",
            type: "creative",
            description: "Global creative agency network and Publicis flagship brand.",
          },
          {
            name: "Leo Burnett",
            type: "creative",
            description:
              "Global creative agency known for iconic brand building (e.g., Marlboro Man, Tony the Tiger).",
          },
          {
            name: "Saatchi & Saatchi",
            type: "creative",
            description: "Global creative agency — 'Nothing is Impossible' ethos.",
          },
          {
            name: "BBH (Bartle Bogle Hegarty)",
            type: "creative",
            description: "Creative agency known for award-winning campaigns.",
          },
          {
            name: "Fallon",
            type: "creative",
            description: "Creative agency.",
          },
          {
            name: "Marcel",
            type: "creative",
            description: "Creative agency (not to be confused with the Publicis internal platform also called Marcel).",
          },
          {
            name: "Conill",
            type: "creative",
            description: "US Hispanic multicultural creative agency.",
          },
          {
            name: "MSL (MSLGROUP)",
            type: "pr",
            description:
              "Global public relations and communications agency. One of the largest PR firms in the world.",
          },
          {
            name: "Prodigious",
            type: "production",
            description: "Global production platform — content, digital, and print production.",
          },
        ],
      },

      // ----- PUBLICIS MEDIA -----
      publicisMedia: {
        name: "Publicis Media",
        description:
          "Media planning, buying, and investment arm of Publicis Groupe.",
        agencies: [
          {
            name: "Starcom",
            type: "media",
            description:
              "Global media agency focused on human experience and consumer-first approach.",
          },
          {
            name: "Zenith",
            type: "media",
            description: "Global media agency — ROI-focused media planning and buying.",
          },
          {
            name: "Spark Foundry",
            type: "media",
            description:
              "Global media agency with entrepreneurial spirit (formerly Mediavest | Spark).",
          },
          {
            name: "Performics",
            type: "performance_media",
            description:
              "Performance marketing agency — search, social, programmatic, and affiliate.",
          },
          {
            name: "Publicis Health Media",
            type: "health_media",
            description: "Specialist healthcare media agency.",
          },
        ],
      },

      // ----- PUBLICIS SAPIENT -----
      publicisSapient: {
        name: "Publicis Sapient",
        acquiredDate: "2015 (Sapient Corporation acquired for ~$3.7B)",
        description:
          "Digital business transformation company — consulting, technology, and engineering.",
        agencies: [
          {
            name: "Publicis Sapient",
            type: "tech_consulting",
            description:
              "Digital business transformation through strategy, consulting, experience design, and engineering.",
          },
          {
            name: "Digitas",
            type: "digital",
            description:
              "Connected marketing agency — strategy, media, CX, creative, social, CRM.",
          },
          {
            name: "Razorfish",
            type: "digital",
            description:
              "Digital experience and transformation agency.",
          },
        ],
      },

      // ----- DATA & TECHNOLOGY (Epsilon) -----
      dataAndTech: {
        name: "Epsilon",
        acquiredDate: "2019 (acquired for $4.4B)",
        description:
          "Data-driven marketing and technology company at the center of Publicis Groupe, powering all hubs.",
        agencies: [
          {
            name: "Epsilon",
            type: "data_tech",
            description:
              "Data-driven marketing platform. Identity resolution (CORE ID), data management, and performance marketing.",
          },
          {
            name: "CitrusAd",
            type: "retail_media_tech",
            acquiredDate: "2021",
            description:
              "Retail media technology platform — powers retailer ad platforms. Now 'CitrusAd, powered by Epsilon'.",
          },
          {
            name: "Profitero",
            type: "commerce_analytics",
            acquiredDate: "2022",
            description:
              "E-commerce analytics and intelligence platform across 700+ retailer sites.",
          },
          {
            name: "CJ (Commission Junction)",
            type: "affiliate",
            description:
              "Global affiliate marketing network — one of the world's largest performance marketing platforms.",
          },
        ],
      },

      // ----- PUBLICIS HEALTH -----
      publicisHealth: {
        name: "Publicis Health",
        description:
          "Specialist healthcare communications hub with 40+ offices and 8 brands.",
        agencies: [
          {
            name: "Digitas Health",
            type: "health",
            description: "Healthcare digital marketing and communications.",
          },
          {
            name: "Saatchi & Saatchi Wellness",
            type: "health",
            description: "Healthcare creative agency — the 'Wellness Effect'.",
          },
          {
            name: "Razorfish Health",
            type: "health",
            description: "Healthcare digital and technology marketing.",
          },
          {
            name: "Heartbeat",
            type: "health",
            description: "Healthcare creative agency for challenger brands.",
          },
          {
            name: "Langland",
            type: "health",
            description:
              "Healthcare communications — medical education, scientific communications.",
          },
          {
            name: "Payer Sciences",
            type: "health",
            description: "Payer marketing and market access specialist.",
          },
          {
            name: "BBK Worldwide",
            type: "health",
            description: "Clinical trial recruitment and retention specialist.",
          },
        ],
      },
    },

    // Key mergers & acquisitions timeline
    mergerTimeline: [
      {
        year: 2015,
        event: "Acquired Sapient Corporation (~$3.7B) to form Publicis.Sapient",
      },
      {
        year: 2019,
        event: "Acquired Epsilon ($4.4B) — data/identity backbone for entire Groupe",
      },
      {
        year: 2021,
        event: "Acquired CitrusAd — retail media technology platform",
      },
      {
        year: 2022,
        event: "Acquired Profitero — e-commerce analytics",
      },
      {
        year: 2024,
        event: "Acquired Influential — AI-powered influencer marketing platform",
      },
      {
        year: 2025,
        event: "Publicis Health announced intent to acquire p-value Group (medical communications)",
      },
    ],
  },

  // =========================================================================
  // OMNICOM GROUP (NYSE: OMC)
  // =========================================================================
  omnicom: {
    name: "Omnicom Group",
    ticker: "OMC",
    headquarters: "New York, United States",
    ceo: "John Wren",
    structureNote:
      "In Dec 2024, Omnicom announced acquisition of IPG (Interpublic Group) for ~$13.3B in all-stock deal. Deal closed late 2025. Post-merger restructuring announced Feb 2026 — now the world's largest advertising holding company by revenue (~$26B combined).",

    divisions: {
      // ----- OMNICOM ADVERTISING -----
      omnicomAdvertising: {
        name: "Omnicom Advertising Group",
        leader: "Troy Ruhanen (CEO)",
        description:
          "Three global creative networks plus ~12 boutique agencies. DDB, FCB, and MullenLowe being retired by mid-2026.",
        globalNetworks: [
          {
            name: "BBDO",
            type: "creative",
            origin: "legacy-Omnicom",
            description:
              "Global creative agency network — 'The Work. The Work. The Work.'",
            note: "FCB (from legacy-IPG) being folded into BBDO.",
          },
          {
            name: "TBWA",
            type: "creative",
            origin: "legacy-Omnicom",
            description:
              "Global creative agency network — known for 'Disruption' methodology.",
            note: "DDB and MullenLowe being folded into TBWA.",
          },
          {
            name: "McCann",
            type: "creative",
            origin: "legacy-IPG",
            fullName: "McCann Worldgroup",
            description:
              "Global creative agency network — 'Truth Well Told'. One of the oldest agencies in the world.",
            includes: [
              "McCann Erickson",
              "MRM (formerly MRM//McCannn — digital/CRM)",
              "Craft Worldwide (production)",
              "McCann Health",
            ],
          },
        ],
        boutiqueAgencies: [
          { name: "The Martin Agency", origin: "legacy-IPG" },
          { name: "Goodby Silverstein & Partners (GS&P)", origin: "legacy-Omnicom" },
          { name: "Lucky Generals", origin: "legacy-Omnicom", location: "UK" },
          { name: "Grabarz & Partners", origin: "legacy-Omnicom", location: "Germany" },
          { name: "GSD&M", origin: "legacy-Omnicom" },
          { name: "Alma", origin: "legacy-Omnicom", note: "US Hispanic agency" },
          { name: "Zimmerman", origin: "legacy-Omnicom" },
          { name: "Carmichael Lynch", origin: "legacy-Omnicom" },
          { name: "Deutsch", origin: "legacy-IPG" },
          { name: "Lola MullenLowe", origin: "legacy-IPG", location: "Spain" },
          { name: "Africa", origin: "legacy-IPG", location: "Brazil" },
          { name: "Merkley & Partners", origin: "legacy-Omnicom" },
          { name: "Antoni", origin: "legacy-Omnicom", location: "Germany" },
        ],
        retiredBrands: [
          {
            name: "DDB (Doyle Dane Bernbach)",
            retiredDate: "Mid-2026",
            mergedInto: "TBWA",
            origin: "legacy-Omnicom",
            note: "One of advertising's most storied agencies, founded 1949.",
          },
          {
            name: "FCB (Foote, Cone & Belding)",
            retiredDate: "Mid-2026",
            mergedInto: "BBDO",
            origin: "legacy-IPG",
          },
          {
            name: "MullenLowe",
            retiredDate: "Mid-2026",
            mergedInto: "TBWA",
            origin: "legacy-IPG",
          },
        ],
      },

      // ----- OMNICOM MEDIA -----
      omnicomMedia: {
        name: "Omnicom Media",
        leader: "Florian Adamski (Global CEO)",
        formerEntities: ["Omnicom Media Group", "IPG Mediabrands (retired)"],
        description:
          "World's largest media organization. Six global media agency brands from combined Omnicom + IPG portfolio. IPG Mediabrands brand retired.",
        agencies: [
          {
            name: "OMD",
            type: "media",
            origin: "legacy-Omnicom",
            description:
              "Global media agency — one of the largest media agencies in the world.",
          },
          {
            name: "PHD",
            type: "media",
            origin: "legacy-Omnicom",
            description:
              "Global media agency — known for strategic planning and innovation.",
          },
          {
            name: "Hearts & Science",
            type: "media",
            origin: "legacy-Omnicom",
            description:
              "Data-driven media agency. Built around people-based marketing.",
          },
          {
            name: "Initiative",
            type: "media",
            origin: "legacy-IPG",
            description: "Global media agency — culture-driven media.",
          },
          {
            name: "UM (Universal McCann)",
            type: "media",
            origin: "legacy-IPG",
            description:
              "Global media agency — 'Better Science, Better Art, Better Outcomes'.",
          },
          {
            name: "Mediahub",
            type: "media",
            origin: "legacy-IPG",
            description: "Global media agency — challenger media approach.",
          },
        ],
      },

      // ----- OMNICOM PUBLIC RELATIONS -----
      omnicomPR: {
        name: "Omnicom Public Relations",
        description:
          "Unified global PR division post-IPG merger. Key consolidations: Porter Novelli into FleishmanHillard; Ketchum merging with Golin.",
        agencies: [
          {
            name: "FleishmanHillard",
            type: "pr",
            origin: "legacy-Omnicom",
            description:
              "Global PR and communications agency. Porter Novelli integrated as a dedicated brand within FleishmanHillard.",
            includes: ["Porter Novelli (integrated as sub-brand, from legacy-Omnicom)"],
          },
          {
            name: "Golin",
            type: "pr",
            origin: "legacy-IPG",
            description:
              "Global PR agency — merging with Ketchum post-IPG deal.",
            mergerNote: "Golin and Ketchum merging into single entity under Golin leadership.",
          },
          {
            name: "Ketchum",
            type: "pr",
            origin: "legacy-Omnicom",
            description: "Global PR agency — merging with Golin.",
          },
          {
            name: "Weber Shandwick",
            type: "pr",
            origin: "legacy-IPG",
            description:
              "Global PR agency. Continuing to operate independently post-merger.",
          },
          {
            name: "Mercury",
            type: "public_affairs",
            origin: "legacy-Omnicom",
            description: "Public affairs and strategic communications.",
          },
          {
            name: "GMMB",
            type: "public_affairs",
            origin: "legacy-IPG",
            description: "Public affairs, cause-related, and political communications.",
          },
          {
            name: "Vox Global",
            type: "public_affairs",
            origin: "legacy-Omnicom",
            description: "Public affairs communications.",
          },
          {
            name: "FP1 Strategies",
            type: "public_affairs",
            description: "Political and public affairs agency.",
          },
          {
            name: "PLUS Communications",
            type: "public_affairs",
            description: "Public affairs and advocacy.",
          },
        ],
      },

      // ----- OMNICOM HEALTH -----
      omnicomHealth: {
        name: "Omnicom Health",
        leader: "Dana Maiman (CEO, from legacy-IPG Health)",
        description:
          "Healthcare marketing and communications division combining legacy-Omnicom and legacy-IPG health agencies.",
        agencies: [
          {
            name: "Adelphi",
            type: "health",
            origin: "legacy-Omnicom",
            description: "Health outcomes research and real-world evidence.",
          },
          {
            name: "Biolumina",
            type: "health",
            origin: "legacy-Omnicom",
            description: "Oncology specialist agency.",
          },
          {
            name: "Area23",
            type: "health",
            origin: "legacy-IPG",
            description:
              "Healthcare creative agency — part of IPG Health prior to merger.",
          },
          {
            name: "Neon",
            type: "health",
            origin: "legacy-IPG",
            description: "Healthcare creative and strategic agency.",
          },
          {
            name: "McCann Health",
            type: "health",
            origin: "legacy-IPG",
            description:
              "Healthcare communications network within McCann Worldgroup.",
          },
          {
            name: "CDM New York (now part of Omnicom Health)",
            type: "health",
            origin: "legacy-Omnicom",
            description: "Healthcare creative agency.",
          },
        ],
      },

      // ----- FLYWHEEL / COMMERCE -----
      flywheelCommerce: {
        name: "Flywheel Commerce Network",
        leader: "Duncan Painter (CEO)",
        acquiredDate: "October 2023 ($835M)",
        description:
          "Commerce and retail media platform. Combined with Omni platform to form OmniPlus.",
        agencies: [
          {
            name: "Flywheel Digital",
            type: "commerce",
            description:
              "E-commerce optimization, retail media, and marketplace management platform.",
          },
        ],
      },

      // ----- OMNICOM PRECISION MARKETING -----
      omnicomPrecision: {
        name: "Omnicom Precision Marketing Group",
        leader: "Luke Taylor (CEO)",
        description: "CRM, data-driven, and precision marketing agencies.",
        agencies: [
          {
            name: "Credera",
            type: "tech_consulting",
            description:
              "Management consulting and technology solutions firm.",
          },
          {
            name: "Critical Mass",
            type: "digital",
            description: "Digital experience design agency.",
          },
          {
            name: "RAPP",
            type: "crm",
            description: "Data-driven CRM and direct marketing agency.",
          },
        ],
      },

      // ----- DIVERSIFIED AGENCY SERVICES (DAS) -----
      das: {
        name: "Omnicom Diversified Agency Services (DAS)",
        description:
          "Houses specialty agencies across health, precision marketing, commerce, branding, and experiential.",
        notableAgencies: [
          {
            name: "Interbrand",
            type: "brand_consulting",
            description: "Global brand consultancy — publishes annual Best Global Brands ranking.",
          },
          {
            name: "Siegel+Gale",
            type: "brand_consulting",
            description: "Brand strategy, design, and experience consultancy.",
          },
          {
            name: "Jack Morton",
            type: "experiential",
            origin: "legacy-IPG",
            description: "Experiential and brand experience agency.",
          },
          {
            name: "Octagon",
            type: "sports_entertainment",
            origin: "legacy-IPG",
            description: "Sports and entertainment marketing agency.",
          },
          {
            name: "Marina Maher Communications (MMC)",
            type: "pr",
            description: "Consumer PR and marketing agency.",
          },
        ],
      },
    },

    // Key mergers & acquisitions timeline
    mergerTimeline: [
      {
        year: 2023,
        month: "October",
        event: "Acquired Flywheel Digital for $835M (e-commerce/retail media)",
      },
      {
        year: 2024,
        month: "December",
        event:
          "Announced acquisition of Interpublic Group (IPG) for ~$13.3B in all-stock deal",
      },
      {
        year: 2025,
        month: "Late",
        event:
          "Omnicom-IPG merger completed. IPG Mediabrands retired. Combined entity becomes world's largest ad holding company (~$26B revenue).",
      },
      {
        year: 2026,
        month: "February",
        event:
          "Post-merger restructuring announced: DDB, FCB, MullenLowe to be retired by mid-2026. 4,000 job cuts. Three global creative networks (BBDO, TBWA, McCann). Six media networks. PR consolidations (Golin+Ketchum, FleishmanHillard+Porter Novelli).",
      },
    ],

    // Legacy-IPG agencies now part of Omnicom (notable ones)
    legacyIPGAgencies: [
      "McCann Worldgroup",
      "FCB (being retired into BBDO)",
      "MullenLowe (being retired into TBWA)",
      "R/GA",
      "Huge",
      "Initiative",
      "UM",
      "Mediahub",
      "Weber Shandwick",
      "Golin",
      "The Martin Agency",
      "Deutsch",
      "Jack Morton",
      "Octagon",
      "GMMB",
      "Area23 (IPG Health)",
    ],
  },
};

// =========================================================================
// QUICK REFERENCE: Agency Type Lookup
// =========================================================================
const AGENCY_TYPE_LABELS = {
  media: "Media (Buying/Planning)",
  creative: "Creative/Advertising",
  pr: "Public Relations",
  public_affairs: "Public Affairs",
  digital: "Digital/Interactive",
  data_tech: "Data & Technology",
  tech_consulting: "Technology Consulting",
  commerce: "Commerce & Retail Media",
  commerce_analytics: "Commerce Analytics",
  retail_media_tech: "Retail Media Technology",
  health: "Health & Pharma",
  health_media: "Health Media",
  production: "Production",
  brand_consulting: "Brand Consulting & Design",
  performance_media: "Performance Marketing",
  crm: "CRM & Direct Marketing",
  affiliate: "Affiliate Marketing",
  experiential: "Experiential Marketing",
  sports_entertainment: "Sports & Entertainment",
};

// =========================================================================
// SUMMARY: Key competitive comparison
// =========================================================================
const HOLDING_GROUP_SUMMARY = {
  wpp: {
    estimatedRevenue: "~$15B (2025)",
    employees: "~100,000",
    mediaArm: "WPP Media (fka GroupM)",
    keyDifferentiator:
      "Largest media buyer globally. Elevate28 restructure into single integrated company.",
    dataAsset: "Choreograph",
    aiPlatform: "WPP Open (AI-powered marketing OS)",
  },
  publicis: {
    estimatedRevenue: "~$16B (2025)",
    employees: "~100,000",
    mediaArm: "Publicis Media",
    keyDifferentiator:
      "Epsilon data backbone (240M+ US consumer profiles). Power of One integrated model. Strongest data/identity position.",
    dataAsset: "Epsilon (CORE ID identity graph)",
    aiPlatform: "CoreAI / Marcel (internal collaboration platform)",
  },
  omnicom: {
    estimatedRevenue: "~$26B (2025/2026 combined post-IPG)",
    employees: "~130,000+",
    mediaArm: "Omnicom Media",
    keyDifferentiator:
      "World's largest ad holding company post-IPG merger. Six media networks. Flywheel commerce platform. Omni data platform.",
    dataAsset: "Omni / OmniPlus (with Flywheel)",
    aiPlatform: "Omni AI Platform",
  },
};

// Export for use in other modules
if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    HOLDING_GROUPS_AGENCIES,
    AGENCY_TYPE_LABELS,
    HOLDING_GROUP_SUMMARY,
  };
}
