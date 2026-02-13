const stateSelect = document.getElementById("state-select");

const elements = {
  headline: document.getElementById("headline-text"),
  subheadline: document.getElementById("subheadline-text"),
  headlinePoints: document.getElementById("headline-points"),
  readinessScore: document.getElementById("readiness-score"),
  readinessLabel: document.getElementById("readiness-label"),
  ehrAdoption: document.getElementById("ehr-adoption"),
  hieExchange: document.getElementById("hie-exchange"),
  apiUse: document.getElementById("api-use"),
  digitalInsight: document.getElementById("digital-insight"),
  ruralPct: document.getElementById("rural-pct"),
  urbanPct: document.getElementById("urban-pct"),
  ruralLabel: document.getElementById("rural-label"),
  ruralConstraints: document.getElementById("rural-constraints"),
  ruralImplications: document.getElementById("rural-implications"),
  mapdShare: document.getElementById("mapd-share"),
  maOnlyShare: document.getElementById("ma-only-share"),
  pdpShare: document.getElementById("pdp-share"),
  mapdLabel: document.getElementById("mapd-label"),
  mapdImplications: document.getElementById("mapd-implications"),
  planMethodNote: document.getElementById("plan-method-note"),
  rolesSummary: document.getElementById("roles-summary"),
  rolesList: document.getElementById("roles-list"),
  preseasonBefore: document.getElementById("preseason-before"),
  preseasonAfter: document.getElementById("preseason-after"),
  preseasonRisks: document.getElementById("preseason-risks"),
  maEnrollment: document.getElementById("ma-enrollment"),
  partdEnrollment: document.getElementById("partd-enrollment"),
  avgStar: document.getElementById("avg-star"),
  churnPct: document.getElementById("churn-pct"),
  volatilityLabel: document.getElementById("volatility-label"),
  starsNotes: document.getElementById("stars-notes"),
  sourceList: document.getElementById("source-list"),
  updatedAt: document.getElementById("updated-at"),
};

function fmtPct(value) {
  if (value === null || value === undefined || value === "") return "Not available";
  return `${Number(value).toFixed(1)}%`;
}

function fmtNumber(value) {
  if (value === null || value === undefined || value === "") return "Not available";
  return Number(value).toLocaleString();
}

function setText(element, value, fallback = "Not available") {
  if (!element) return;
  element.textContent = value === null || value === undefined || value === "" ? fallback : value;
}

function setList(element, items) {
  if (!element) return;
  element.innerHTML = "";
  if (!items || !items.length) {
    const li = document.createElement("li");
    li.textContent = "Not available";
    element.appendChild(li);
    return;
  }
  items.forEach((item) => {
    const li = document.createElement("li");
    li.textContent = item;
    element.appendChild(li);
  });
}

function impactClass(impact) {
  const level = (impact || "").toLowerCase();
  if (level === "high") return "impact-high";
  if (level === "medium") return "impact-medium";
  return "impact-low";
}

function setRoles(element, roles) {
  if (!element) return;
  element.innerHTML = "";
  if (!roles || !roles.length) {
    const empty = document.createElement("div");
    empty.textContent = "Not available";
    element.appendChild(empty);
    return;
  }
  roles.forEach((role) => {
    const card = document.createElement("div");
    card.className = `role-card ${impactClass(role.impact)}`;

    const left = document.createElement("div");
    const right = document.createElement("div");

    const name = document.createElement("div");
    name.className = "role-name";
    name.textContent = role.role;

    const why = document.createElement("div");
    why.className = "role-why";
    why.textContent = role.why;

    const impact = document.createElement("div");
    impact.className = `role-impact ${impactClass(role.impact)}`;
    impact.textContent = role.impact;

    left.appendChild(name);
    left.appendChild(why);
    right.appendChild(impact);

    card.appendChild(left);
    card.appendChild(right);
    element.appendChild(card);
  });
}

const SOURCE_LABELS = {
  onc: "ONC Health IT Dashboard",
  cms: "CMS Medicare Advantage / Part D",
  ruca: "USDA ERS Rural-Urban Classification",
  census: "U.S. Census Bureau",
};

function setSources(element, sources) {
  if (!element) return;
  element.innerHTML = "";
  if (!sources || !Object.keys(sources).length) {
    const li = document.createElement("li");
    li.textContent = "Not available";
    element.appendChild(li);
    return;
  }
  Object.entries(sources).forEach(([key, value]) => {
    const li = document.createElement("li");
    const label = SOURCE_LABELS[key] || key.toUpperCase();
    li.innerHTML = `<strong>${label}</strong> &mdash; ${value}`;
    element.appendChild(li);
  });
}

// Scroll-based active section highlighting
const sectionIds = [
  "headline",
  "digital-readiness",
  "rural-urban",
  "mapd-pdp",
  "roles",
  "preseason",
  "stars-context",
];
const navLinks = document.querySelectorAll(".section-nav a");

function updateActiveNav() {
  let current = "";
  for (const id of sectionIds) {
    const section = document.getElementById(id);
    if (!section) continue;
    const rect = section.getBoundingClientRect();
    if (rect.top <= 180) {
      current = id;
    }
  }
  navLinks.forEach((link) => {
    if (link.getAttribute("href") === `#${current}`) {
      link.classList.add("active");
    } else {
      link.classList.remove("active");
    }
  });
}

window.addEventListener("scroll", updateActiveNav, { passive: true });

// State transition
function fadeWall(callback) {
  const wall = document.getElementById("wall");
  wall.classList.add("fade-out");
  setTimeout(() => {
    callback();
    wall.classList.remove("fade-out");
    wall.classList.add("fade-in");
    setTimeout(() => wall.classList.remove("fade-in"), 300);
  }, 150);
}

async function loadIndex() {
  try {
    const response = await fetch("data/index.json");
    const payload = await response.json();

    stateSelect.innerHTML = "";
    payload.states
      .sort((a, b) => a.name.localeCompare(b.name))
      .forEach((state) => {
        const option = document.createElement("option");
        option.value = state.code;
        option.textContent = `${state.name} (${state.code})`;
        stateSelect.appendChild(option);
      });

    if (payload.states.length) {
      stateSelect.value = payload.states[0].code;
      await loadState(payload.states[0].code);
    }

    setText(elements.updatedAt, `Updated ${payload.updated_at}`);
  } catch (error) {
    setText(elements.headline, "State data not found. Run the build pipeline.");
  }
}

async function loadState(code) {
  try {
    const response = await fetch(`data/states/${code}.json`);
    const data = await response.json();

    setText(elements.headline, data.summary?.headline);
    setText(elements.subheadline, data.summary?.subheadline);
    setList(elements.headlinePoints, data.summary?.key_points);

    const readinessScore = data.digital_readiness?.readiness_score;
    setText(elements.readinessScore, readinessScore !== null && readinessScore !== undefined ? readinessScore.toFixed(1) : "Not available");
    setText(elements.readinessLabel, data.digital_readiness?.readiness_label);

    setText(elements.ehrAdoption, fmtPct(data.digital_readiness?.ehr_adoption_pct));
    setText(elements.hieExchange, fmtPct(data.digital_readiness?.hie_exchange_pct));
    setText(elements.apiUse, fmtPct(data.digital_readiness?.api_use_pct));
    setText(elements.digitalInsight, data.digital_readiness?.insight);

    setText(elements.ruralPct, fmtPct(data.rural_urban?.rural_pct));
    setText(elements.urbanPct, fmtPct(data.rural_urban?.urban_pct));
    setText(elements.ruralLabel, data.rural_urban?.label);
    setList(elements.ruralConstraints, data.rural_urban?.constraints);
    setList(elements.ruralImplications, data.rural_urban?.implications);

    setText(elements.mapdShare, fmtPct(data.mapd_pdp?.mapd_share_pct));
    setText(elements.maOnlyShare, fmtPct(data.mapd_pdp?.ma_only_share_pct));
    setText(elements.pdpShare, fmtPct(data.mapd_pdp?.pdp_share_pct));
    setText(elements.mapdLabel, data.mapd_pdp?.label);
    setText(elements.mapdImplications, data.mapd_pdp?.implications?.join(" "));
    setText(elements.planMethodNote, data.mapd_pdp?.method_note, "");

    setText(elements.rolesSummary, data.roles_impact?.summary);
    setRoles(elements.rolesList, data.roles_impact?.roles);

    setList(elements.preseasonBefore, data.preseason_shift?.before);
    setList(elements.preseasonAfter, data.preseason_shift?.after);
    setList(elements.preseasonRisks, data.preseason_shift?.operational_risks);

    setText(elements.maEnrollment, fmtNumber(data.stars_context?.ma_enrollment));
    setText(elements.partdEnrollment, fmtNumber(data.stars_context?.partd_enrollment));
    setText(elements.avgStar, data.stars_context?.avg_star !== null && data.stars_context?.avg_star !== undefined ? data.stars_context?.avg_star.toFixed(1) : "Not available");
    setText(elements.volatilityLabel, data.stars_context?.volatility_label);
    setText(elements.churnPct, fmtPct(data.stars_context?.churn_pct));
    setList(elements.starsNotes, data.stars_context?.notes);

    setSources(elements.sourceList, data.sources || {});
    setText(elements.updatedAt, `Updated ${data.updated_at}`);

    updateActiveNav();
  } catch (error) {
    setText(elements.headline, "State data not found. Run the build pipeline.");
  }
}

stateSelect.addEventListener("change", (event) => {
  fadeWall(() => loadState(event.target.value));
});

loadIndex();
