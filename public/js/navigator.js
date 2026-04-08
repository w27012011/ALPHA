/**
 * navigator.js - PROJECT ALPHA
 * Shared Multi-Theme Navigation (V4 Elegant Duo)
 */

const NAV_ITEMS = [
    { id: 'strat_master', label: 'Command Center', icon: 'layers', url: 'strat_master.html' },
    { id: 'hydro', label: 'Hydrology Bay', icon: 'waves', url: 'hydro.html' },
    { id: 'geo', label: 'Geology Bay', icon: 'activity', url: 'geo.html' },
    { id: 'aqua', label: 'Water/Arsenic Bay', icon: 'droplets', url: 'aqua.html' },
    { id: 'agri', label: 'Agriculture Bay', icon: 'leaf', url: 'agri.html' },
    { id: 'atmo', label: 'Atmosphere Bay', icon: 'cloud-lightning', url: 'atmo.html' },
    { id: 'econ', label: 'Economic Warning', icon: 'trending-up', url: 'econ.html' },
    { id: 'nerve', label: 'Nerve Center', icon: 'zap', url: 'nerve.html' },
];

function initNavigator() {
  const currentPage = window.location.pathname.split('/').pop() || 'strat_master.html';

  // 0. Inject Zoom Dependencies
  injectScript('https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js');
  injectScript('https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js');

  // 1. Theme Persistence
  const savedTheme = localStorage.getItem('alpha-theme') || 'dark';
  document.body.setAttribute('data-theme', savedTheme);

  // 2. Inject Header
  const header = document.createElement('header');
  header.className = "h-14 border-b fixed top-0 right-0 left-0 z-50 flex items-center justify-between px-6 shadow-sm";
  header.innerHTML = `
    <div class="flex items-center gap-4">
      <div class="flex items-center gap-2 cursor-pointer" onclick="window.location.href='strat_master.html'">
        <div class="w-7 h-7 bg-blue-600 rounded flex items-center justify-center">
          <i data-lucide="shield-alert" class="text-white w-4 h-4"></i>
        </div>
        <div>
          <h1 class="text-base font-bold tracking-tight text-primary leading-none" style="color:var(--text-primary)">PROJECT ALPHA</h1>
          <p class="text-[9px] text-secondary font-mono uppercase tracking-widest mt-1" style="color:var(--text-secondary)">USS v2.0 • National Resilience</p>
        </div>
      </div>
    </div>
    <div class="flex items-center gap-4">
      <div class="hidden md:flex items-center gap-2 px-2.5 py-1 bg-slate-900 border border-slate-800 rounded-full" style="background:rgba(0,0,0,0.1); border-color:var(--border-primary)">
        <div class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></div>
        <span class="text-[9px] font-bold text-secondary uppercase tracking-wider" style="color:var(--text-secondary)">Live • <span id="current-time">00:00:00</span> UTC</span>
      </div>
      <div class="flex items-center gap-2">
        <!-- Theme Toggle -->
        <button id="theme-toggle" class="p-2 border border-primary rounded-lg hover:bg-slate-800 transition-colors" style="border-color:var(--border-primary)">
          <i data-lucide="${savedTheme === 'light' ? 'moon' : 'sun'}" class="w-4 h-4" style="color:var(--text-primary)"></i>
        </button>
        <div class="w-8 h-8 rounded-full bg-slate-800 border border-primary flex items-center justify-center" style="border-color:var(--border-primary)">
          <i data-lucide="user" class="text-secondary w-4 h-4" style="color:var(--text-secondary)"></i>
        </div>
      </div>
    </div>
  `;
  document.body.prepend(header);

  // 3. Inject Sidebar
  const sidebar = document.createElement('aside');
  sidebar.className = "fixed top-14 bottom-0 left-0 w-64 border-r transition-all duration-300 z-40 overflow-hidden";
  sidebar.innerHTML = `
    <div class="p-4 space-y-2">
      <p class="text-[9px] font-bold text-secondary uppercase tracking-[0.2em] px-4 mb-3" style="color:var(--text-secondary)">Tactical Bays</p>
      <nav id="main-nav" class="space-y-0.5">
        ${NAV_ITEMS.map(item => `
          <a href="${item.url}" class="w-full flex items-center gap-3 px-4 py-2.5 rounded-lg transition-all duration-200 group ${currentPage === item.url ? 'bg-blue-600/10 border border-blue-500/20 active-link' : 'text-secondary hover:bg-slate-800/10 hover:text-primary'}">
            <i data-lucide="${item.icon}" class="w-4 h-4 ${currentPage === item.url ? 'text-blue-500' : 'text-slate-500 group-hover:text-slate-200'}"></i>
            <span class="text-xs font-semibold" style="${currentPage === item.url ? 'color:var(--accent-blue)' : 'color:var(--text-secondary)'}">${item.label}</span>
            ${currentPage === item.url ? '<div class="ml-auto w-1 h-1 rounded-full bg-blue-500"></div>' : ''}
          </a>
        `).join('')}
      </nav>
    </div>
    <div class="absolute bottom-0 left-0 right-0 p-4 border-t border-primary bg-sidebar" style="border-color:var(--border-primary)">
       <div class="p-3 rounded-lg bg-body border border-primary flex items-center gap-3" style="border-color:var(--border-primary); background:rgba(0,0,0,0.05)">
        <div class="p-2 bg-blue-600/10 rounded">
          <i data-lucide="refresh-cw" class="text-blue-500 w-3.5 h-3.5 animate-spin-slow"></i>
        </div>
        <div>
          <p class="text-[9px] font-bold text-secondary uppercase tracking-wider" style="color:var(--text-secondary)">V4 Bus Sync</p>
          <p class="text-[11px] font-mono text-blue-500" id="sync-timer">00:00:10</p>
        </div>
      </div>
    </div>
  `;
  document.body.appendChild(sidebar);

  // 4. Initializers
  if (typeof lucide !== 'undefined') lucide.createIcons();
  
  document.getElementById('theme-toggle').onclick = toggleTheme;
  startTimers();
}

function toggleTheme() {
  const current = document.body.getAttribute('data-theme');
  const next = current === 'light' ? 'dark' : 'light';
  document.body.setAttribute('data-theme', next);
  localStorage.setItem('alpha-theme', next);
  
  // Reload icons to swap Sun/Moon
  const icon = document.querySelector('#theme-toggle i');
  icon.setAttribute('data-lucide', next === 'light' ? 'moon' : 'sun');
  lucide.createIcons();
}

function injectScript(url) {
  const s = document.createElement('script');
  s.src = url;
  document.head.appendChild(s);
}

function startTimers() {
  const timeEl = document.getElementById('current-time');
  const syncEl = document.getElementById('sync-timer');
  let syncCount = 10;

  setInterval(() => {
    const now = new Date();
    if (timeEl) timeEl.textContent = now.toISOString().split('T')[1].split('.')[0];
    
    syncCount--;
    if (syncCount < 0) syncCount = 10;
    if (syncEl) syncEl.textContent = `00:00:${syncCount.toString().padStart(2, '0')}`;
  }, 1000);
}

document.addEventListener('DOMContentLoaded', initNavigator);
