/**
 * app.js - PROJECT ALPHA COMMAND CENTER
 * Logic Engine: 8-Bay State Management, D3.js, Leaflet, and Real-Time Bus Sync.
 * Version: 3.0 (Enterprise Suite)
 */

const STATE = {
    activeView: 'strat-master',
    busBuffer: [],
    districts: {},   // Cumulative HPS and risk levels
    modules: {},     // 51 Module Heartbeats
    geoData: null,
    map: null,
    geoLayer: null,
};

// 1. MODULE MANIFEST (M-01 to M-51)
const ALPHA_MODULES = [
    { id: "M-01", name: "CONN-FFWC", group: "DATA" },
    { id: "M-02", name: "CONN-ERA5", group: "DATA" },
    { id: "M-03", name: "CONN-USGS", group: "DATA" },
    { id: "M-04", name: "CONN-MS", group: "DATA" },
    { id: "M-05", name: "CONN-EIA", group: "DATA" },
    { id: "M-06", name: "HYDR-PROC", group: "HYDRO" },
    { id: "M-07", name: "HYDR-FORC", group: "HYDRO" },
    { id: "M-08", name: "HYDR-EXT", group: "HYDRO" },
    { id: "M-09", name: "HYDR-MIDAS", group: "HYDRO" },
    { id: "M-10", name: "PBT-HPS", group: "PBT" },
    { id: "M-11", name: "AQUA-KRIG", group: "AQUA" },
    { id: "M-12", name: "AQUA-MOB", group: "AQUA" },
    { id: "M-13", name: "AQUA-CLAS", group: "AQUA" },
    { id: "M-14", name: "AQUA-SAFE", group: "AQUA" },
    { id: "M-15", name: "AQUA-FORM", group: "AQUA" },
    { id: "M-16", name: "CASC-MAP", group: "CASCADE" },
    { id: "M-17", name: "CASC-REG", group: "CASCADE" },
    { id: "M-18", name: "CASC-DET", group: "CASCADE" },
    { id: "M-19", name: "CASC-TREE", group: "CASCADE" },
    { id: "M-20", name: "CASC-PUB", group: "CASCADE" },
    { id: "M-21", name: "PBT-NS", group: "PBT" },
    { id: "M-22", name: "PBT-RBR", group: "PBT" },
    { id: "M-23", name: "PBT-SFF", group: "PBT" },
    { id: "M-24", name: "GEO-INSAR", group: "GEO" },
    { id: "M-25", name: "GEO-EROS", group: "GEO" },
    { id: "M-26", name: "GEO-SEIS", group: "GEO" },
    { id: "M-27", name: "GEO-LIQ", group: "GEO" },
    { id: "M-28", name: "AGRI-NDVI", group: "AGRI" },
    { id: "M-29", name: "AGRI-HARV", group: "AGRI" },
    { id: "M-30", name: "AGRI-LOSS", group: "AGRI" },
    { id: "M-31", name: "AGRI-RECO", group: "AGRI" },
    { id: "M-32", name: "ATMO-E5", group: "ATMO" },
    { id: "M-33", name: "ATMO-CAPE", group: "ATMO" },
    { id: "M-34", name: "ATMO-WWL", group: "ATMO" },
    { id: "M-35", name: "ATMO-STRM", group: "ATMO" },
    { id: "M-36", name: "ATMO-CYCL", group: "ATMO" },
    { id: "M-37", name: "ECON-DEMD", group: "ECON" },
    { id: "M-38", name: "ECON-RES", group: "ECON" },
    { id: "M-39", name: "ECON-PPS", group: "ECON" },
    { id: "M-40", name: "ECON-DOM", group: "ECON" },
    { id: "M-41", name: "ECON-GLOB", group: "ECON" },
    { id: "M-42", name: "ECON-CRIS", group: "ECON" },
    { id: "M-43", name: "NOW-MIDAS", group: "NOW" },
    { id: "M-44", name: "UPD-KF", group: "UPDATE" },
    { id: "M-45", name: "UPD-UKF", group: "UPDATE" },
    { id: "M-46", name: "UPD-PF", group: "UPDATE" },
    { id: "M-47", name: "VAL-BACK", group: "VALIDA" },
    { id: "M-48", name: "VAL-DM", group: "VALIDA" },
    { id: "M-49", name: "PRES-SEL", group: "PRESC" },
    { id: "M-50", name: "PRES-OPT", group: "PRESC" },
    { id: "M-51", name: "PRES-DISP", group: "PRESC" }
];

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', () => {
    initViewOrchestrator();
    initMap();
    initNerveGrid();
    initBusPoller();
    startClock();
});

// 2. VIEW ORCHESTRATOR
function initViewOrchestrator() {
    const navLinks = document.querySelectorAll('.nav-links li');
    const stages = document.querySelectorAll('.theater-stage');

    navLinks.forEach(link => {
        link.addEventListener('click', () => {
            const view = link.getAttribute('data-view');
            if (!view) return;

            STATE.activeView = view;

            // Nav UI
            navLinks.forEach(l => l.classList.remove('active'));
            link.classList.add('active');

            // Theatre UI
            stages.forEach(s => s.classList.remove('active'));
            const activeStage = document.getElementById(`view-${view}`);
            if (activeStage) activeStage.classList.add('active');

            // Specific View Initializers
            if (view === 'strat-master') renderProbabilityTree();
            if (view === 'econ') renderEconVials();
        });
    });

    document.getElementById('close-drawer').onclick = () => {
        document.getElementById('tactical-drawer').classList.remove('active');
    };
}

// 3. MAP CONTROLLER (Leaflet)
function initMap() {
    STATE.map = L.map('alpha-map-master', {
        zoomControl: false,
        attributionControl: false,
    }).setView([23.8103, 90.4125], 7);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png').addTo(STATE.map);

    fetch('data/districts.json')
        .then(res => res.json())
        .then(data => {
            STATE.geoData = data;
            drawDistricts();
        });
}

function drawDistricts() {
    if (!STATE.geoData) return;
    if (STATE.geoLayer) STATE.map.removeLayer(STATE.geoLayer);

    STATE.geoLayer = L.geoJSON(STATE.geoData, {
        style: (feature) => {
            const hps = STATE.districts[feature.properties.NAME_2]?.hps || 0;
            return {
                fillColor: getColorForHPS(hps),
                weight: 1, color: 'rgba(255,255,255,0.1)', fillOpacity: 0.6
            };
        },
        onEachFeature: (f, l) => {
            l.on('click', () => openDistrictDrawer(f.properties.NAME_2));
        }
    }).addTo(STATE.map);
}

function getColorForHPS(hps) {
    if (hps > 3.5) return 'var(--tone-critical)';
    if (hps > 2.5) return 'var(--tone-warning)';
    if (hps > 1.0) return 'var(--tone-elevated)';
    return 'var(--tone-safe)';
}

// 4. NERVE CENTER (51 LEDs)
function initNerveGrid() {
    const grid = document.getElementById('module-health-grid');
    ALPHA_MODULES.forEach(mod => {
        const card = document.createElement('div');
        card.className = 'module-led-card';
        card.id = `led-card-${mod.id}`;
        card.innerHTML = `
            <span class="led-state"></span>
            <span class="module-id">${mod.id}</span>
            <span class="module-label" style="font-size:0.6rem; opacity:0.5;">${mod.name}</span>
        `;
        card.onclick = () => openModuleDetail(mod);
        grid.appendChild(card);
    });
}

// 5. BUS POLLING (Unified)
function initBusPoller() {
    setInterval(async () => {
        try {
            const [busRes, heartbeatRes] = await Promise.all([
                fetch('/api/bus'),
                fetch('/api/heartbeats')
            ]);
            
            const busData = await busRes.json();
            const heartbeatData = await heartbeatRes.json();

            updateBusLog(busData);
            updateNerveLEDs(heartbeatData);
            updateDistrictsFromBus(busData);
        } catch (e) {
            console.warn("Poll failed (Service expected at localhost:8080)", e);
        }
    }, 2000);
}

function updateBusLog(messages) {
    const log = document.getElementById('bus-live-log');
    if (!messages.length) return;
    
    // Clear and append
    log.innerHTML = messages.map(m => `
        <div style="margin-bottom:4px; padding-bottom:4px; border-bottom:1px solid rgba(255,255,255,0.05);">
            <span style="opacity:0.3">[${m.timestamp}]</span> 
            <span style="color:var(--tone-safe)">${m.channel}:</span> 
            <span style="opacity:0.7">${JSON.stringify(m.message).substring(0, 100)}...</span>
        </div>
    `).join('');
}

function updateNerveLEDs(heartbeats) {
    ALPHA_MODULES.forEach(mod => {
        const led = document.querySelector(`#led-card-${mod.id} .led-state`);
        if (!led) return;
        
        const status = heartbeats[mod.id]?.status || "OFFLINE";
        led.className = "led-state";
        if (status === "ACTIVE" || status === "HEALTHY") led.classList.add('green');
        else if (status === "DEGRADED") led.classList.add('amber');
        else if (status === "CRITICAL") led.classList.add('red');
    });
}

function updateDistrictsFromBus(messages) {
    messages.forEach(m => {
        if (m.channel === 'pbt.hps_raw') {
            const payload = m.message;
            if (payload.district) {
                STATE.districts[payload.district] = { hps: payload.hps_value };
            }
        }
    });
    if (STATE.activeView === 'strat-master') drawDistricts();
}

// 6. PROBABILITY TREE (D3.js)
function renderProbabilityTree() {
    const container = document.getElementById('master-probability-tree');
    const width = container.clientWidth;
    const height = 400;

    d3.select("#cascade-svg-canvas").selectAll("*").remove();
    const svg = d3.select("#cascade-svg-canvas")
        .attr("width", width)
        .attr("height", height)
        .append("g").attr("transform", "translate(50,20)");

    // Mock Recursive Tree Data for demo
    const treeData = {
        name: "HYDRO: FLOOD",
        prob: "88%",
        children: [
            { name: "GEO: EROSION", prob: "62%", children: [{ name: "AQUA: TOXIC", prob: "45%" }] },
            { name: "ECON: PPS SPIKE", prob: "31%", children: [{ name: "AGRI: ROT", prob: "72%" }] }
        ]
    };

    const tree = d3.tree().size([350, width - 200]);
    const root = d3.hierarchy(treeData);
    tree(root);

    svg.selectAll(".link")
        .data(root.links())
        .enter().append("path")
        .attr("class", "link")
        .attr("d", d3.linkHorizontal().x(d => d.y).y(d => d.x))
        .style("fill", "none").style("stroke", "rgba(255,255,255,0.1)").style("stroke-width", 2);

    const nodes = svg.selectAll(".node")
        .data(root.descendants())
        .enter().append("g")
        .attr("transform", d => `translate(${d.y},${d.x})`);

    nodes.append("circle").attr("r", 8).style("fill", "var(--bg-active)").style("stroke", "#fff");
    nodes.append("text").attr("dy", 18).attr("x", 0).attr("text-anchor", "middle")
        .text(d => `${d.data.name} (${d.data.prob})`).style("font-size", "10px").style("fill", "#fff");
}

// 7. ECON VIALS (Mock Demo)
function renderEconVials() {
    const container = document.getElementById('econ-reserve-vials');
    container.innerHTML = `
        <div style="display:flex; justify-content:space-around; align-items:flex-end; height:300px;">
            ${['PETROLEUM', 'FOOD', 'MEDICINE'].map(label => `
                <div style="text-align:center;">
                    <div style="width:40px; height:200px; border:1px solid #555; position:relative; background:rgba(0,0,0,0.3); border-radius:4px;">
                        <div style="position:absolute; bottom:0; width:100%; height:75%; background:var(--tone-safe); opacity:0.8; box-shadow:0 0 20px var(--tone-safe);"></div>
                    </div>
                    <div style="font-size:0.6rem; margin-top:8px; opacity:0.5;">${label}</div>
                    <div style="font-size:0.8rem; font-weight:800;">75%</div>
                </div>
            `).join('')}
        </div>
    `;
}

// 8. DETAIL DRAWER
function openDistrictDrawer(name) {
    const drawer = document.getElementById('tactical-drawer');
    const content = document.getElementById('drawer-content');
    document.getElementById('drawer-title').textContent = `DISTRICT: ${name.toUpperCase()}`;
    
    content.innerHTML = `
        <div style="margin-bottom:24px;">
            <label style="font-size:0.7rem; opacity:0.5;">CURRENT HPS</label>
            <div style="font-size:3rem; font-weight:900; font-family:var(--font-display);">${(STATE.districts[name]?.hps || 0.42).toFixed(2)}</div>
        </div>
        <div class="alpha-glass" style="font-size:0.85rem; opacity:0.7;">
            <strong>ADVERSARIAL TRACE:</strong><br>
            Predictive surge driven by HYDRO M-07 (Upper Brahmaputra wave front) and 
            AQUA M-11 kriging anomalies in deep aquifer facies.
        </div>
    `;
    drawer.classList.add('active');
}

function openModuleDetail(mod) {
    const drawer = document.getElementById('tactical-drawer');
    document.getElementById('drawer-title').textContent = `MODULE: ${mod.id}`;
    document.getElementById('drawer-content').innerHTML = `
        <div style="font-size:0.9rem; margin-bottom:20px;">
            <strong>${mod.name}</strong><br>Group: ${mod.group}
        </div>
        <div class="alpha-glass">
            <label style="font-size:0.7rem; opacity:0.5;">INPUTS</label><br>
            <span style="color:var(--tone-safe)">raw.bus_ingest</span><br><br>
            <label style="font-size:0.7rem; opacity:0.5;">WORKLOAD</label><br>
            <span>Processed 42 packets last cycle</span>
        </div>
    `;
    drawer.classList.add('active');
}

function startClock() {
    setInterval(() => {
        document.getElementById('national-clock').textContent = new Date().toISOString().split('T')[1].split('.')[0] + " UTC";
    }, 1000);
}
