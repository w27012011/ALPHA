/**
 * app.js - PROJECT ALPHA
 * Read-Only Telemetry Binding Layer
 * Architected to passively consume /api/bus and /api/heartbeats.
 */

class AlphaTelemetryBus {
    constructor() {
        this.pollInterval = 2000;
        this.state = {
            bus: [],
            heartbeats: {},
            channels: {},
            // M-53: Rich Static Database (64 Districts)
            db: {
                soil: null,
                crops: null,
                moisture: null,
                dominant: null,
                capacity: null,
                census: null,
                arsenic: null
            },
            currentDistrict: 'BD-01' // Default focus (Dhaka)
        };
        this.start();
    }

    async start() {
        console.log("AlphaTelemetryBus: Initializing Strategic Command Suite...");
        this.initNavigation(); // M-55: Setup SPA View Switching (TTI Immediate)
        this.loadRichDatabase(); // Load in background
        this.tick();
        setInterval(() => this.tick(), this.pollInterval);
    }

    initNavigation() {
        // M-55 Tri-Pane Navigator Logic
        const navItems = document.querySelectorAll('.nav-item');
        const stages = document.querySelectorAll('.theater-stage');

        navItems.forEach(item => {
            item.addEventListener('click', () => {
                const view = item.getAttribute('data-view');
                if (!view) return;

                // Toggle Active Nav
                navItems.forEach(i => i.classList.remove('active'));
                item.classList.add('active');

                // Toggle Active Stage
                stages.forEach(s => {
                    s.classList.remove('active');
                    if (s.id === `view-${view}`) s.classList.add('active');
                });
                
                console.log(`AlphaBus: Switched View to [${view}]`);
            });
        });
    }

    setDistrict(code) {
        if (!code) return;
        const normalized = code.startsWith('BD-') ? code : `BD-${code}`;
        this.state.currentDistrict = normalized;
        console.log(`AlphaBus: Global Context set to [${normalized}]`);
        this.processBindings(); // Immediate UI refresh
        
        // Notify any bespoke observers
        window.dispatchEvent(new CustomEvent('AlphaDistrictChange', { detail: normalized }));
    }

    /**
     * THE TRANSFORMER (M-53/M-55/M-59)
     * Synthesizes raw mathematical telemetry with District Ground-Truth.
     */
    transform(districtKey, channel, rawValue) {
        if (!this.state.db.soil || !this.state.db.crops) return null;

        // M-78 Support for both HASC codes (BD-XX) and District Names (Dhaka)
        const dbKey = districtKey; 
        
        // 1. Hazard Impact on Population (Census/BBS)
        if (channel === 'pbt.hps_raw') {
            const census = this.state.db.census ? (this.state.db.census[dbKey] || this.state.db.census[dbKey.toUpperCase()]) : null;
            const pop = census ? census.population : 150000;
            return {
                risk_population: Math.round(rawValue * (pop / 100)),
                severity_label: rawValue > 3.5 ? 'CRITICAL' : (rawValue > 2.0 ? 'ELEVATED' : 'STABLE')
            };
        }
        
        // 2. Crop Economic Risk (Agri/BBS)
        if (channel === 'agri.ndvi_processed') {
            const yieldVal = (this.state.db.crops[dbKey] || this.state.db.crops[`BD-${dbKey}`]) || 100000;
            const pricePerTon = 28000; 
            const lossTons = Math.round((1 - rawValue) * (yieldVal.rice_boro || 100000));
            return {
                tonnage_at_risk: lossTons,
                economic_loss_bdt: lossTons * pricePerTon,
                soil_type: this.state.db.soil[dbKey] || 'ALLUVIAL_LOAM'
            };
        }
        
        // 3. Arsenic Exposure Risk (BGS)
        if (channel === 'wat.arsenic_raw' || channel === 'pbt.hps_raw') {
            const arsenic = this.state.db.arsenic ? (this.state.db.arsenic[dbKey] || this.state.db.arsenic[dbKey.toUpperCase()]) : null;
            const ppb = arsenic ? (arsenic.avg_arsenic_ugl || arsenic.arsenic_ppb || 25) : 25;
            return {
                arsenic_ppb: ppb,
                health_risk: arsenic ? (arsenic.risk_level || (ppb > 50 ? 'HIGH' : 'SAFE')) : 'UNKNOWN'
            };
        }

        return null;
    }

    async loadRichDatabase() {
        try {
            const files = [
                { key: 'soil', url: '/data/district_soil_class.json' },
                { key: 'crops', url: '/data/district_crop_production.json' },
                { key: 'moisture', url: '/data/soil_moisture_baseline.json' },
                { key: 'dominant', url: '/data/dominant_crop.json' },
                { key: 'capacity', url: '/data/district_capacity.json' },
                { key: 'census', url: '/data/district_population.json' },
                { key: 'arsenic', url: '/data/arsenic_risk.json' }
            ];

            for (const file of files) {
                const resp = await fetch(file.url);
                if (resp.ok) {
                    this.state.db[file.key] = await resp.json();
                }
            }
            console.log("AlphaTelemetryBus: 64-District Rich Database Loaded.");
        } catch (e) {
            console.error("AlphaTelemetryBus: Failed to load rich static data.", e);
        }
    }

    async tick() {
        try {
            await Promise.all([this.fetchBus(), this.fetchHeartbeats()]);
            this.processChannels();
            this.processBindings();
            
            // Dispatch a global event for bespoke map/chart updates perfectly siloed per page
            window.dispatchEvent(new CustomEvent('AlphaDataSync', { detail: this.state }));
        } catch (err) {
            console.error("Telemetry Sync Error:", err);
        }
    }

    async fetchBus() {
        try {
            const res = await fetch('/api/bus');
            if (res.ok) {
                const data = await res.json();
                this.state.bus = data;
            }
        } catch(e) {
            console.warn("API Bus unreachable.");
        }
    }

    async fetchHeartbeats() {
         try {
            const res = await fetch('/api/heartbeats');
             if (res.ok) {
                 this.state.heartbeats = await res.json();
             }
        } catch(e) {
            console.warn("API Heartbeat unreachable.");
        }
    }

    processChannels() {
        if (!Array.isArray(this.state.bus)) return;
        
        for (let i = this.state.bus.length - 1; i >= 0; i--) {
            const entry = this.state.bus[i];
            if (entry.channel && entry.message) {
                this.state.channels[entry.channel] = entry.message;
            }
        }
    }

    processBindings() {
        const elements = document.querySelectorAll('[data-alpha-bind]');
        elements.forEach(el => {
            const bindPath = el.getAttribute('data-alpha-bind');
            const parts = bindPath.split('.');
            let val = null;

            if (parts[0] === 'db') {
                val = this.state.db[parts[1]];
                for (let i = 2; i < parts.length; i++) {
                    if (val && val[parts[i]] !== undefined) val = val[parts[i]];
                    else { val = null; break; }
                }
            } else if (parts[0] === 'transformer') {
                // M-55/M-62: Transformer Binding Logic
                // Usage: transformer.hps_impact.BD-01 OR transformer.hps_impact.*
                const type = parts[1];
                let code = parts[2];
                if (code === '*') code = this.state.currentDistrict;
                
                const hps = this.state.channels['pbt.hps_raw']?.hps_value || 0;
                const trans = this.transform(code, 'pbt.hps_raw', hps);
                if (trans) val = trans[type];
            } else {
                const msg = this.state.channels[parts[0]];
                if (msg) {
                    val = msg;
                    for (let i = 1; i < parts.length; i++) {
                        if (val[parts[i]] !== undefined) val = val[parts[i]];
                        else { val = null; break; }
                    }
                }
            }

            if (val !== null && val !== undefined) {
                const newVal = typeof val === 'number' ? 
                    (Number.isInteger(val) ? val.toLocaleString() : val.toFixed(2)) : val;
                if (el.innerText !== String(newVal)) el.innerText = newVal;
            }
        });
    }
}

// Initialize on load
window.addEventListener('DOMContentLoaded', () => {
    window.AlphaBus = new AlphaTelemetryBus();
});
