// FDS Dashboard - Client Script

document.addEventListener("DOMContentLoaded", () => {
    initNavigation();
    initChart();
    initImportanceChart();
    populateMockAlerts();
    populateStreamTable();
    populateFullAlertsTable();
    simulateLiveData();
});

let liveChart;
let importChart;

function initNavigation() {
    const navItems = document.querySelectorAll('.sidebar nav .nav-item[data-target]');
    const views = document.querySelectorAll('.view-section');
    const pageTitle = document.getElementById('page-title');

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            
            // Update active state in sidebar
            navItems.forEach(nav => nav.classList.remove('active'));
            item.classList.add('active');
            
            // Hide all views
            views.forEach(view => view.style.display = 'none');
            
            // Show target view
            const targetId = item.getAttribute('data-target');
            document.getElementById(targetId).style.display = 'block';
            
            // Update page title
            pageTitle.innerText = item.innerText;
        });
    });
}

function initChart() {
    const ctx = document.getElementById('liveChart').getContext('2d');
    
    // Gradient for the line area
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(0, 240, 255, 0.2)');
    gradient.addColorStop(1, 'rgba(0, 240, 255, 0)');

    // Mock initial data
    const labels = Array.from({length: 20}, (_, i) => `-${20-i}s`);
    const dataVol = Array.from({length: 20}, () => Math.floor(Math.random() * 50) + 150);

    liveChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Transactions / sec',
                data: dataVol,
                borderColor: '#00f0ff',
                backgroundColor: gradient,
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 6,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(10, 10, 15, 0.9)',
                    titleColor: '#8a8a9a',
                    bodyColor: '#e0e0e5',
                    borderColor: 'rgba(255,255,255,0.1)',
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false,
                        drawBorder: false
                    },
                    ticks: {
                        color: '#8a8a9a',
                        maxTicksLimit: 10
                    }
                },
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)',
                        drawBorder: false
                    },
                    ticks: {
                        color: '#8a8a9a'
                    },
                    min: 0,
                    max: 300
                }
            },
            animation: {
                duration: 0 // disable animation for live update feel
            }
        }
    });
}

function initImportanceChart() {
    const ctx = document.getElementById('importanceChart');
    if(!ctx) return;
    
    importChart = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: {
            labels: ['amt_sum_1h', 'tx_count_10m', 'pct_ecommerce', 'max_amt_1h', 'distinct_mcc_30d', 'avg_seconds_between_tx'],
            datasets: [{
                label: 'Feature Importance (Gain)',
                data: [0.35, 0.22, 0.15, 0.12, 0.09, 0.07],
                backgroundColor: 'rgba(0, 240, 255, 0.6)',
                borderColor: '#00f0ff',
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.05)' },
                    ticks: { color: '#8a8a9a' }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: '#e0e0e5' }
                }
            }
        }
    });
}

const mockAlerts = [
    { id: "tx_bench_16999321", amount: "$20,000.00", rule: "Amount exceeds maximum", type: "decline", time: "13:42:01" },
    { id: "tx_bench_16999305", amount: "$5,430.50", rule: "Suspiciously high score", type: "review", time: "13:41:45" },
    { id: "tx_bench_16999298", amount: "$1,200.00", rule: "High fraud probability", type: "decline", time: "13:40:12" },
    { id: "tx_bench_16999120", amount: "$15,000.00", rule: "Amount exceeds maximum", type: "decline", time: "13:38:50" },
    { id: "tx_bench_16998999", amount: "$80,000.00", rule: "International high amount", type: "review", time: "13:35:10" },
];

function populateMockAlerts() {
    const list = document.getElementById('alertsList');
    if(!list) return;
    list.innerHTML = '';

    mockAlerts.forEach(a => {
        const div = document.createElement('div');
        div.className = `alert-item ${a.type}`;
        div.innerHTML = `
            <div class="alert-header">
                <span class="alert-id">${a.id}</span>
                <span class="alert-amount">${a.amount}</span>
            </div>
            <div class="alert-rule">${a.rule}</div>
        `;
        list.appendChild(div);
    });
}

function populateFullAlertsTable() {
    const tbody = document.getElementById('fullAlertsBody');
    if(!tbody) return;
    tbody.innerHTML = '';
    
    mockAlerts.forEach(a => {
        const tr = document.createElement('tr');
        const badgeClass = a.type === 'decline' ? 'badge-decline' : 'badge-review';
        tr.innerHTML = `
            <td>${a.id}</td>
            <td><span class="${badgeClass}">${a.type.toUpperCase()}</span></td>
            <td>${a.amount}</td>
            <td style="color: #8a8a9a">${a.rule}</td>
            <td style="color: #8a8a9a">${a.time}</td>
        `;
        tbody.appendChild(tr);
    });
}

function populateStreamTable() {
    const tbody = document.getElementById('streamTableBody');
    if(!tbody) return;
    
    const mccList = ["5411", "5812", "5912", "4511"];
    let html = '';
    for(let i=0; i<10; i++) {
        html += `
            <tr>
                <td>tx_live_${Math.floor(Math.random()*1000000)}</td>
                <td>user_${Math.floor(Math.random()*5000)}</td>
                <td style="color: var(--accent-green)">$${(Math.random()*500).toFixed(2)}</td>
                <td>merch_${Math.floor(Math.random()*100)}</td>
                <td style="color: #8a8a9a">${new Date().toLocaleTimeString()}</td>
            </tr>
        `;
    }
    tbody.innerHTML = html;
}

function simulateLiveData() {
    setInterval(() => {
        // Update Chart
        if(liveChart) {
            const currentData = liveChart.data.datasets[0].data;
            currentData.shift();
            // Generate a random TPS around 197
            const newTps = 180 + Math.random() * 40;
            currentData.push(newTps);
            
            liveChart.update();
            
            // Update Metric Text
            const tpsEl = document.getElementById('tps-value');
            if(tpsEl) tpsEl.innerText = newTps.toFixed(1);
            
            // Random fluctuation for latency
            const latEl = document.getElementById('latency-value');
            const lat = 150 + Math.random() * 20;
            if(latEl) latEl.innerText = lat.toFixed(0) + " ms";
        }
        
        // Slightly update the stream table to look alive
        if(document.getElementById('view-streaming').style.display !== 'none') {
            populateStreamTable();
        }
    }, 1000);
}
